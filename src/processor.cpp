#include "processor.h"

#include <cstdio>
#include <utility>

#include "decimal.h"

namespace {

constexpr std::int64_t kNanos = 1000000000;

// FNV-1a of the symbol name: stable across runs, machines and replay,
// and independent of the order symbols are given on the command line.
std::int32_t instrument_id(const std::string& symbol) {
    std::uint32_t h = 2166136261u;
    for (unsigned char c : symbol) {
        h ^= c;
        h *= 16777619u;
    }
    return static_cast<std::int32_t>(h & 0x7fffffffu);
}

simdjson::padded_string_view padded(std::string& s) {
    if (s.capacity() - s.size() < simdjson::SIMDJSON_PADDING) s.reserve(s.size() + simdjson::SIMDJSON_PADDING);
    return simdjson::padded_string_view(s.data(), s.size(), s.capacity());
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

}  // namespace

Processor::Processor(Venue venue, Metrics& metrics) : venue_(venue), metrics_(metrics) {
    depth_.bids.reserve(1024);
    depth_.asks.reserve(1024);
    row_.reserve(8192);
}

bool Processor::add_symbol(const SymbolOutput& out) {
    auto s = std::make_unique<SymbolState>();
    s->symbol = out.symbol;
    s->id = instrument_id(out.symbol);
    s->check_bids.reserve(64);
    s->check_asks.reserve(64);
    if (!out.market_data_path.empty()) {
        if (!s->market_data.open(out.market_data_path)) return false;
        s->market_data.write(kMarketDataHeader);
        s->market_data.write("\n");
    }
    if (!s->orderbook.open(out.orderbook_path)) return false;
    s->orderbook.write(kOrderbookHeader);
    s->orderbook.write("\n");
    symbols_.push_back(std::move(s));
    return true;
}

void Processor::process(RawMessage& msg) {
    if (msg.symbol_index >= symbols_.size()) {
        bump(metrics_.unknown_streams);
        return;
    }
    SymbolState& s = *symbols_[msg.symbol_index];
    if (s.market_data.is_open()) write_market_data(s, msg);

    // A new connection means we may have missed updates while reconnecting,
    // so the book cannot be trusted until the next depth5 arrives.
    if (s.seen_epoch && msg.conn_epoch != s.epoch && s.valid) reset_book(s, msg);
    s.seen_epoch = true;
    s.epoch = msg.conn_epoch;

    try {
        switch (msg.kind) {
        case StreamKind::DepthDiff: on_diff(s, msg); break;
        case StreamKind::Depth5: on_depth5(s, msg); break;
        case StreamKind::Trade: on_trade(s, msg); break;
        case StreamKind::RestSnapshot: on_rest_snapshot(s, msg); break;
        }
    } catch (const simdjson::simdjson_error&) {
        count_parse_error(s, stream_kind_name(msg.kind));
    }
}

void Processor::on_diff(SymbolState& s, RawMessage& msg) {
    if (!parse_depth(msg.payload) || (venue_ == Venue::Usdm && !depth_.has_prev)) {
        count_parse_error(s, "depth_diff");
        return;
    }
    // Every diff is kept for a while and also moves a pending REST book
    // forward. Without --rest-snapshot nothing else happens with them.
    remember_diff(s);
    advance_rest_book(s, depth_);
    apply_diff(s, msg);
    try_rest_swap(s, msg);
}

void Processor::apply_diff(SymbolState& s, RawMessage& msg) {
    if (!s.valid) {
        bump(metrics_.diffs_waiting);
        return;
    }

    std::uint64_t prev = prev_update_id(depth_);
    if (depth_.last_id <= s.last_update_id) {
        bump(metrics_.diffs_stale);
        return;
    }
    if (prev > s.last_update_id) {
        bump(metrics_.gaps);
        reset_book(s, msg);
        return;
    }

    if (s.check_pending && depth_.last_id > s.check_id) {
        // The snapshot's id is inside this diff's range, so the two can never be
        // compared exactly (on USD-M depth5 and diffs are cut at different times).
        // Fold the snapshot in first; the diff then moves every level it touches
        // up to u, which leaves the book exact at u.
        s.bids.replace_top(s.check_bids);
        s.asks.replace_top(s.check_asks);
        s.check_pending = false;
        bump(metrics_.depth5_folded);
        bump(metrics_.depth5_applied);
    }

    for (const Level& lv : depth_.bids) s.bids.set(lv.price, lv.qty);
    for (const Level& lv : depth_.asks) s.asks.set(lv.price, lv.qty);
    s.last_update_id = depth_.last_id;
    bump(metrics_.diffs_applied);

    char side = 'N';
    if (!depth_.bids.empty() && depth_.asks.empty())
        side = 'B';
    else if (depth_.bids.empty() && !depth_.asks.empty())
        side = 'S';
    write_book_row(s, msg, 'D', side);

    if (s.check_pending && s.last_update_id == s.check_id) {
        s.check_pending = false;
        verify_top(s, msg, s.check_bids, s.check_asks);
    }
}

void Processor::on_depth5(SymbolState& s, RawMessage& msg) {
    if (!parse_depth(msg.payload) || (depth_.bids.empty() && depth_.asks.empty()) ||
        !sorted_best_first(depth_.bids, true) || !sorted_best_first(depth_.asks, false)) {
        count_parse_error(s, "depth5");
        return;
    }

    std::uint64_t id = depth_.last_id;
    if (!s.valid) {
        s.bids.clear();
        s.asks.clear();
        s.bids.replace_top(depth_.bids);
        s.asks.replace_top(depth_.asks);
        s.last_update_id = id;
        s.valid = true;
        s.check_pending = false;
        bump(metrics_.depth5_applied);
        write_book_row(s, msg, 'S', 'N');
        return;
    }

    if (id < s.last_update_id) {
        bump(metrics_.depth5_stale);
        return;
    }
    if (id == s.last_update_id) {
        verify_top(s, msg, depth_.bids, depth_.asks);
        return;
    }

    // The snapshot is ahead of the diffs applied so far (on spot it arrives a
    // few ms before the diff with the same id). Keep it until the diffs catch up.
    s.check_pending = true;
    s.check_id = id;
    s.check_bids = depth_.bids;
    s.check_asks = depth_.asks;
}

// In sync, the diffs are the source of truth and depth5 is the check. If the
// diff-built top 5 disagrees with the snapshot at the same update id, trust the
// snapshot: replace the top levels and write an S row.
void Processor::verify_top(SymbolState& s, const RawMessage& msg, const std::vector<Level>& bids,
                           const std::vector<Level>& asks) {
    bump(metrics_.depth5_checked);
    if (s.bids.top_matches(bids) && s.asks.top_matches(asks)) return;
    bump(metrics_.depth5_mismatch);
    s.bids.replace_top(bids);
    s.asks.replace_top(asks);
    bump(metrics_.depth5_applied);
    write_book_row(s, msg, 'S', 'N');
}

void Processor::on_trade(SymbolState& s, RawMessage& msg) {
    bool buyer_is_maker = false;
    bool found = false;
    simdjson::ondemand::document doc = parser_.iterate(padded(msg.payload));
    for (auto field : doc.get_object()) {
        std::string_view key = field.unescaped_key();
        if (key == "m") {
            buyer_is_maker = field.value().get_bool();
            found = true;
        }
    }
    if (!found) {
        count_parse_error(s, "trade");
        return;
    }
    bump(metrics_.trades);
    if (!s.valid) return;
    // Trades do not change a diff-built book; the row only marks that a trade
    // printed, with the aggressor side (buyer is maker -> the seller hit the bid).
    write_book_row(s, msg, 'T', buyer_is_maker ? 'S' : 'B');
}

// REST /depth snapshot (up to 1000 levels a side). It is a moment in time
// with its own update id, which is usually a little behind or ahead of the
// live book. Build a full-depth book from it, bring it forward with the diffs
// we kept, and swap it in once both books are at the same update id.
void Processor::on_rest_snapshot(SymbolState& s, RawMessage& msg) {
    if (!parse_depth(msg.payload) || (depth_.bids.empty() && depth_.asks.empty()) ||
        !sorted_best_first(depth_.bids, true) || !sorted_best_first(depth_.asks, false)) {
        count_parse_error(s, "rest_snapshot");
        return;
    }
    bump(metrics_.rest_snapshots);

    s.rest_bids.clear();
    s.rest_asks.clear();
    s.rest_bids.replace_top(depth_.bids);
    s.rest_asks.replace_top(depth_.asks);
    s.rest_id = depth_.last_id;
    s.rest_pending = true;

    std::size_t oldest = (s.recent_next + kRecentDiffs - s.recent_count) % kRecentDiffs;
    for (std::size_t k = 0; k < s.recent_count; ++k) advance_rest_book(s, s.recent[(oldest + k) % kRecentDiffs]);
    try_rest_swap(s, msg);
}

void Processor::remember_diff(SymbolState& s) {
    if (s.recent.size() != kRecentDiffs) s.recent.resize(kRecentDiffs);
    s.recent[s.recent_next] = depth_;
    s.recent_next = (s.recent_next + 1) % kRecentDiffs;
    if (s.recent_count < kRecentDiffs) ++s.recent_count;
}

// Same sequence rules as the live book: skip what the snapshot already has,
// and give up on the snapshot if a diff is missing.
void Processor::advance_rest_book(SymbolState& s, const DepthMsg& d) {
    if (!s.rest_pending || d.last_id <= s.rest_id) return;
    if (prev_update_id(d) > s.rest_id) {
        s.rest_pending = false;
        bump(metrics_.rest_dropped);
        return;
    }
    for (const Level& lv : d.bids) s.rest_bids.set(lv.price, lv.qty);
    for (const Level& lv : d.asks) s.rest_asks.set(lv.price, lv.qty);
    s.rest_id = d.last_id;
}

void Processor::try_rest_swap(SymbolState& s, const RawMessage& msg) {
    if (!s.rest_pending) return;
    if (s.valid && s.rest_id != s.last_update_id) return;  // one of them still has to catch up

    // Both books are at the same id now, so their top 5 should agree.
    if (s.valid) {
        for (int i = 0; i < kBookDepth; ++i) {
            if (!(s.bids.level(i) == s.rest_bids.level(i)) || !(s.asks.level(i) == s.rest_asks.level(i))) {
                bump(metrics_.rest_top_mismatch);
                break;
            }
        }
    }
    std::swap(s.bids, s.rest_bids);
    std::swap(s.asks, s.rest_asks);
    s.rest_bids.clear();
    s.rest_asks.clear();
    s.rest_pending = false;
    s.last_update_id = s.rest_id;
    s.valid = true;
    bump(metrics_.rest_applied);
    write_book_row(s, msg, 'S', 'N');
}

std::uint64_t Processor::prev_update_id(const DepthMsg& d) const {
    if (venue_ == Venue::Usdm) return d.prev_id;
    return d.first_id > 0 ? d.first_id - 1 : 0;
}

bool Processor::parse_depth(std::string& payload) {
    depth_.first_id = 0;
    depth_.last_id = 0;
    depth_.prev_id = 0;
    depth_.has_prev = false;
    depth_.bids.clear();
    depth_.asks.clear();

    bool have_last = false;
    simdjson::ondemand::document doc = parser_.iterate(padded(payload));
    for (auto field : doc.get_object()) {
        std::string_view key = field.unescaped_key();
        if (key == "U") {
            depth_.first_id = field.value().get_uint64();
        } else if (key == "u" || key == "lastUpdateId") {
            depth_.last_id = field.value().get_uint64();
            have_last = true;
        } else if (key == "pu") {
            std::int64_t pu = field.value().get_int64();
            depth_.prev_id = pu > 0 ? static_cast<std::uint64_t>(pu) : 0;
            depth_.has_prev = true;
        } else if (key == "b" || key == "bids") {
            if (!read_levels(field.value().get_array(), depth_.bids)) return false;
        } else if (key == "a" || key == "asks") {
            if (!read_levels(field.value().get_array(), depth_.asks)) return false;
        }
    }
    return have_last;
}

void Processor::reset_book(SymbolState& s, const RawMessage& msg) {
    s.bids.clear();
    s.asks.clear();
    s.valid = false;
    s.last_update_id = 0;
    s.check_pending = false;
    // The kept diffs and a pending REST book stay: update ids are global, so
    // advance_rest_book still catches any hole, and a REST book that is still
    // continuous is swapped in right after this reset.
    bump(metrics_.book_resets);
    write_book_row(s, msg, 'R', 'N');
}

void Processor::write_market_data(SymbolState& s, const RawMessage& msg) {
    std::string& r = row_;
    r.clear();
    append_int(r, msg.recv_ns / kNanos);
    r += ',';
    append_int(r, msg.recv_ns % kNanos);
    r += ',';
    r += venue_name(venue_);
    r += ',';
    r += stream_kind_name(msg.kind);
    r += ',';
    append_uint(r, msg.shard_id);
    r += ',';
    append_uint(r, msg.conn_epoch);
    r += ',';
    append_uint(r, msg.conn_seq);
    r += ',';
    r += s.symbol;
    r += ',';
    append_csv_field(r, msg.payload);
    r += '\n';
    s.market_data.write(r);
    bump(metrics_.md_rows);
}

void Processor::write_book_row(SymbolState& s, const RawMessage& msg, char type, char side) {
    std::string& r = row_;
    r.clear();
    append_int(r, msg.recv_ns / kNanos);
    r += ',';
    append_int(r, msg.recv_ns % kNanos);
    r += ',';
    append_uint(r, ++s.seq_no);
    r += ',';
    append_int(r, s.id);
    r += ',';
    r += type;
    r += ',';
    r += side;
    for (int i = 0; i < kBookDepth; ++i) {
        r += ',';
        append_int(r, s.bids.level(i).price);
    }
    for (int i = 0; i < kBookDepth; ++i) {
        r += ',';
        append_int(r, s.bids.level(i).qty);
    }
    for (int i = 0; i < kBookDepth; ++i) {
        r += ',';
        append_int(r, s.asks.level(i).price);
    }
    for (int i = 0; i < kBookDepth; ++i) {
        r += ',';
        append_int(r, s.asks.level(i).qty);
    }
    r += '\n';
    s.orderbook.write(r);
    bump(metrics_.ob_rows);
}

void Processor::count_parse_error(const SymbolState& s, const char* what) {
    std::uint64_t n = metrics_.parse_errors.fetch_add(1, std::memory_order_relaxed) + 1;
    if (n <= 10) std::fprintf(stderr, "[book] %s: could not parse %s payload\n", s.symbol.c_str(), what);
}

void Processor::flush() {
    for (auto& s : symbols_) {
        s->market_data.flush();
        s->orderbook.flush();
    }
}

void Processor::close() {
    for (auto& s : symbols_) {
        s->market_data.close();
        s->orderbook.close();
        bump(metrics_.write_errors, s->market_data.write_errors() + s->orderbook.write_errors());
    }
}
