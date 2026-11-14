// This is my full-depth check against Binance. The depth5 check only covers the top 5 levels,
// so during a live capture I had a separate process save REST /depth snapshots (limit=1000)
// every 10 s, and this program compares the rest of the book against them.
//
// usage: verify_against_rest MARKET_DATA_CSV SNAPSHOT.json [SNAPSHOT.json ...]
// Build it with `cmake --build build --target verify_against_rest`. It isn't part of ctest.
// Each snapshot file is a raw /depth response: lastUpdateId plus bids/asks, best price first.
//
// It replays the market data file through the same Processor the program uses. A snapshot is
// the true book at its lastUpdateId L, so I bring each one forward with the captured diffs,
// with the same sequence rules (skip u <= L, give up if a diff's previous id is past the
// snapshot book), until it's at the same update id as the processor's book. Then both sides
// are compared level by level from the best price. Only prices from the best down to the
// snapshot's last price count, because deeper levels in the snapshot book are just the ones
// later diffs touched. It prints one line per snapshot and a summary.
//
// On the 10-minute BTCUSDT captures (spot and USD-M), all 103 snapshots were exact through the
// top 100 levels, and every first mismatch further down was at a price outside what our own
// startup snapshot covered. I also gave it a snapshot I edited on purpose, to make sure it
// catches a real difference, and it did.

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include <simdjson.h>

#include "csv.h"
#include "decimal.h"
#include "metrics.h"
#include "order_book.h"
#include "processor.h"
#include "types.h"

namespace fs = std::filesystem;

namespace {

enum class Status { Waiting, Started, Compared, NotAligned, Broken };

struct Diff {
    std::uint64_t last_id = 0;
    std::uint64_t prev_id = 0;
    std::vector<Level> bids;
    std::vector<Level> asks;
};

struct Snapshot {
    std::string name;
    std::uint64_t rest_id = 0;
    std::vector<Level> bids;
    std::vector<Level> asks;
    Status status = Status::Waiting;
    std::uint64_t book_id = 0;
    std::unique_ptr<BookSide> book_bids;
    std::unique_ptr<BookSide> book_asks;
    std::size_t bid_levels = 0;
    std::size_t ask_levels = 0;
    std::size_t bids_matched = 0;
    std::size_t asks_matched = 0;
    std::string result;
};

template <typename T>
bool parse_number(const std::string& s, T& out) {
    auto res = std::from_chars(s.data(), s.data() + s.size(), out);
    return res.ec == std::errc() && res.ptr == s.data() + s.size();
}

bool read_levels(simdjson::ondemand::array levels, std::vector<Level>& out) {
    out.clear();
    for (auto entry : levels) {
        simdjson::ondemand::array pair = entry.get_array();
        std::string_view price;
        std::string_view qty;
        std::size_t n = 0;
        for (auto v : pair) {
            std::string_view text = v.get_string();
            if (n == 0)
                price = text;
            else if (n == 1)
                qty = text;
            ++n;
        }
        Level lv;
        if (n != 2 || !parse_scaled(price, lv.price) || !parse_scaled(qty, lv.qty)) return false;
        out.push_back(lv);
    }
    return true;
}

bool sorted_best_first(const std::vector<Level>& levels, bool is_bid) {
    for (std::size_t i = 1; i < levels.size(); ++i) {
        bool ok = is_bid ? levels[i].price < levels[i - 1].price : levels[i].price > levels[i - 1].price;
        if (!ok) return false;
    }
    return true;
}

bool load_snapshot(simdjson::ondemand::parser& parser, const std::string& path, Snapshot& snap) {
    simdjson::padded_string json;
    if (simdjson::padded_string::load(path).get(json)) return false;
    bool have_id = false;
    try {
        simdjson::ondemand::document doc = parser.iterate(json);
        for (auto field : doc.get_object()) {
            std::string_view key = field.unescaped_key();
            if (key == "lastUpdateId") {
                snap.rest_id = field.value().get_uint64();
                have_id = true;
            } else if (key == "bids") {
                if (!read_levels(field.value().get_array(), snap.bids)) return false;
            } else if (key == "asks") {
                if (!read_levels(field.value().get_array(), snap.asks)) return false;
            }
        }
    } catch (const simdjson::simdjson_error&) {
        return false;
    }
    return have_id && (!snap.bids.empty() || !snap.asks.empty()) && sorted_best_first(snap.bids, true) &&
           sorted_best_first(snap.asks, false);
}

bool parse_diff(simdjson::ondemand::parser& parser, const std::string& payload, Venue venue, Diff& d) {
    simdjson::padded_string json(payload);
    std::uint64_t first_id = 0;
    bool have_first = false;
    bool have_last = false;
    bool have_prev = false;
    d.prev_id = 0;
    d.bids.clear();
    d.asks.clear();
    simdjson::ondemand::document doc = parser.iterate(json);
    for (auto field : doc.get_object()) {
        std::string_view key = field.unescaped_key();
        if (key == "U") {
            first_id = field.value().get_uint64();
            have_first = true;
        } else if (key == "u") {
            d.last_id = field.value().get_uint64();
            have_last = true;
        } else if (key == "pu") {
            std::int64_t pu = field.value().get_int64();
            d.prev_id = pu > 0 ? static_cast<std::uint64_t>(pu) : 0;
            have_prev = true;
        } else if (key == "b") {
            if (!read_levels(field.value().get_array(), d.bids)) return false;
        } else if (key == "a") {
            if (!read_levels(field.value().get_array(), d.asks)) return false;
        }
    }
    if (venue == Venue::Usdm) return have_last && have_prev;
    d.prev_id = first_id > 0 ? first_id - 1 : 0;
    return have_last && have_first;
}

void advance(Snapshot& snap, const Diff& d) {
    if (snap.status == Status::Waiting) {
        if (d.last_id <= snap.rest_id) return;
        if (d.prev_id > snap.rest_id) {
            snap.status = Status::NotAligned;
            snap.result = "skipped, not aligned: the first diff after it (u=" + std::to_string(d.last_id) +
                          ") has previous id " + std::to_string(d.prev_id) + " > L";
            return;
        }
        snap.status = Status::Started;
        snap.book_id = snap.rest_id;
        snap.book_bids = std::make_unique<BookSide>(true);
        snap.book_asks = std::make_unique<BookSide>(false);
        snap.book_bids->replace_top(snap.bids);
        snap.book_asks->replace_top(snap.asks);
    }
    if (snap.status != Status::Started || d.last_id <= snap.book_id) return;
    if (d.prev_id > snap.book_id) {
        snap.status = Status::Broken;
        snap.result = "skipped, diffs broken: diff u=" + std::to_string(d.last_id) + " has previous id " +
                      std::to_string(d.prev_id) + " > snapshot book id " + std::to_string(snap.book_id);
        snap.book_bids.reset();
        snap.book_asks.reset();
        return;
    }
    for (const Level& lv : d.bids) snap.book_bids->set(lv.price, lv.qty);
    for (const Level& lv : d.asks) snap.book_asks->set(lv.price, lv.qty);
    snap.book_id = d.last_id;
}

std::size_t levels_in_range(const BookSide& side, const std::vector<Level>& fetched, bool is_bid) {
    if (fetched.empty()) return 0;
    std::int64_t worst = fetched.back().price;
    std::size_t n = 0;
    while (n < side.size()) {
        std::int64_t p = side.level(n).price;
        if (is_bid ? p < worst : p > worst) break;
        ++n;
    }
    return n;
}

std::size_t leading_matches(const BookSide& a, const BookSide& b, std::size_t n) {
    std::size_t i = 0;
    while (i < n && a.level(i) == b.level(i)) ++i;
    return i;
}

std::string decimal(std::int64_t v) {
    char buf[48];
    std::snprintf(buf, sizeof(buf), "%lld.%08lld", static_cast<long long>(v / kScale),
                  static_cast<long long>(v % kScale));
    return buf;
}

std::string level_text(const BookSide& side, std::size_t i) {
    if (i >= side.size()) return "none";
    Level lv = side.level(i);
    return decimal(lv.price) + " x " + decimal(lv.qty);
}

std::string mismatch_text(const char* name, const BookSide& rest, const BookSide& book, std::size_t i) {
    return std::string("; first ") + name + " mismatch at index " + std::to_string(i) + ": rest " +
           level_text(rest, i) + ", book " + level_text(book, i);
}

void compare(Snapshot& snap, const SymbolState& s) {
    snap.bid_levels = levels_in_range(*snap.book_bids, snap.bids, true);
    snap.ask_levels = levels_in_range(*snap.book_asks, snap.asks, false);
    snap.bids_matched = leading_matches(*snap.book_bids, s.bids, snap.bid_levels);
    snap.asks_matched = leading_matches(*snap.book_asks, s.asks, snap.ask_levels);
    snap.result = "compared at " + std::to_string(snap.book_id) + ", bids matched " +
                  std::to_string(snap.bids_matched) + "/" + std::to_string(snap.bid_levels) + ", asks matched " +
                  std::to_string(snap.asks_matched) + "/" + std::to_string(snap.ask_levels);
    if (snap.bids_matched < snap.bid_levels)
        snap.result += mismatch_text("bid", *snap.book_bids, s.bids, snap.bids_matched);
    if (snap.asks_matched < snap.ask_levels)
        snap.result += mismatch_text("ask", *snap.book_asks, s.asks, snap.asks_matched);
    snap.status = Status::Compared;
    snap.book_bids.reset();
    snap.book_asks.reset();
}

}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "usage: %s MARKET_DATA_CSV SNAPSHOT.json [SNAPSHOT.json ...]\n", argv[0]);
        return 1;
    }

    simdjson::ondemand::parser parser;
    std::vector<Snapshot> snaps(static_cast<std::size_t>(argc - 2));
    for (std::size_t i = 0; i < snaps.size(); ++i) {
        std::string path = argv[i + 2];
        snaps[i].name = fs::path(path).filename().string();
        if (!load_snapshot(parser, path, snaps[i])) {
            std::fprintf(stderr, "cannot read snapshot %s (needs lastUpdateId and sorted bids/asks)\n", path.c_str());
            return 1;
        }
    }

    std::string md_path = argv[1];
    std::ifstream in(md_path, std::ios::binary);
    if (!in) {
        std::fprintf(stderr, "cannot open %s\n", md_path.c_str());
        return 1;
    }
    std::string line;
    if (!std::getline(in, line)) {
        std::fprintf(stderr, "%s is empty\n", md_path.c_str());
        return 1;
    }
    if (!line.empty() && line.back() == '\r') line.pop_back();
    if (line != kMarketDataHeader) {
        std::fprintf(stderr, "%s does not have the market data header\n", md_path.c_str());
        return 1;
    }

    std::string book_path =
        (fs::temp_directory_path() / ("verify_" + fs::path(md_path).stem().string() + "_orderbook.csv")).string();
    Metrics metrics;
    std::unique_ptr<Processor> processor;
    Venue venue = Venue::Spot;
    std::string symbol;
    std::string record;
    std::vector<std::string> fields;
    RawMessage msg;
    Diff diff;
    std::uint64_t bad_rows = 0;
    std::uint64_t diffs = 0;
    std::uint64_t bad_diffs = 0;
    std::uint64_t last_diff_id = 0;

    while (std::getline(in, line)) {
        record = line;
        while (!split_csv_row(record, fields)) {
            if (!std::getline(in, line)) {
                std::fprintf(stderr, "%s: unterminated quote at the end\n", md_path.c_str());
                return 1;
            }
            record += '\n';
            record += line;
        }
        if (fields.size() == 1 && fields[0].empty()) continue;

        Venue row_venue;
        StreamKind kind;
        std::int64_t tsec = 0;
        std::int64_t tnsec = 0;
        bool ok = fields.size() == 9 && parse_number(fields[0], tsec) && parse_number(fields[1], tnsec) &&
                  parse_venue(fields[2], row_venue) && parse_stream_kind(fields[3], kind) &&
                  parse_number(fields[4], msg.shard_id) && parse_number(fields[5], msg.conn_epoch) &&
                  parse_number(fields[6], msg.conn_seq);
        if (!ok) {
            ++bad_rows;
            continue;
        }
        if (!processor) {
            venue = row_venue;
            symbol = fields[7];
            processor = std::make_unique<Processor>(venue, metrics);
            if (!processor->add_symbol(SymbolOutput{symbol, "", book_path})) {
                std::fprintf(stderr, "cannot create %s\n", book_path.c_str());
                return 1;
            }
        }
        if (fields[7] != symbol) {
            ++bad_rows;
            continue;
        }

        bool is_diff = kind == StreamKind::DepthDiff;
        if (is_diff) {
            ++diffs;
            bool parsed = false;
            try {
                parsed = parse_diff(parser, fields[8], venue, diff);
            } catch (const simdjson::simdjson_error&) {
                parsed = false;
            }
            if (parsed) {
                last_diff_id = diff.last_id;
                for (Snapshot& snap : snaps) advance(snap, diff);
            } else {
                ++bad_diffs;
            }
        }

        msg.recv_ns = tsec * 1000000000LL + tnsec;
        msg.symbol_index = 0;
        msg.kind = kind;
        msg.payload.swap(fields[8]);
        processor->process(msg);

        if (is_diff) {
            const SymbolState& s = processor->state(0);
            for (Snapshot& snap : snaps) {
                if (snap.status == Status::Started && s.valid && snap.book_id == s.last_update_id) compare(snap, s);
            }
        }
    }
    if (processor) processor->close();
    std::error_code ec;
    fs::remove(book_path, ec);

    std::uint64_t book_id = processor ? processor->state(0).last_update_id : 0;
    int compared = 0;
    int all_matched = 0;
    int top100 = 0;
    int not_aligned = 0;
    int broken = 0;
    int never_reached = 0;
    int never_same_id = 0;
    for (Snapshot& snap : snaps) {
        switch (snap.status) {
        case Status::Waiting:
            ++never_reached;
            snap.result = "skipped, never reached: no diff after L (last diff u=" + std::to_string(last_diff_id) + ")";
            break;
        case Status::Started:
            ++never_same_id;
            snap.result = "skipped, the order book was never valid at the snapshot book's id (snapshot book at " +
                          std::to_string(snap.book_id) + ", order book at " + std::to_string(book_id) + ")";
            break;
        case Status::Compared:
            ++compared;
            if (snap.bids_matched == snap.bid_levels && snap.asks_matched == snap.ask_levels) ++all_matched;
            if (snap.bids_matched >= std::min<std::size_t>(100, snap.bid_levels) &&
                snap.asks_matched >= std::min<std::size_t>(100, snap.ask_levels))
                ++top100;
            break;
        case Status::NotAligned: ++not_aligned; break;
        case Status::Broken: ++broken; break;
        }
        std::printf("%s L=%llu: %s\n", snap.name.c_str(), static_cast<unsigned long long>(snap.rest_id),
                    snap.result.c_str());
    }

    auto v = [](const Counter& c) { return static_cast<unsigned long long>(c.load()); };
    std::printf("processor: %s %s, diffs=%llu (unparsed %llu), bad_rows=%llu, rest_applied=%llu, gaps=%llu, "
                "book_resets=%llu, depth5_checked=%llu, depth5_mismatch=%llu\n",
                venue_name(venue), symbol.c_str(), static_cast<unsigned long long>(diffs),
                static_cast<unsigned long long>(bad_diffs), static_cast<unsigned long long>(bad_rows),
                v(metrics.rest_applied), v(metrics.gaps), v(metrics.book_resets), v(metrics.depth5_checked),
                v(metrics.depth5_mismatch));
    std::printf("summary: %zu snapshots, %d compared, %d matched all levels on both sides, "
                "%d matched at least the top 100 on both sides; skipped: %d not aligned, %d broken, "
                "%d never reached, %d never at the same id\n",
                snaps.size(), compared, all_matched, top100, not_aligned, broken, never_reached, never_same_id);
    return 0;
}
