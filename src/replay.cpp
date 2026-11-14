// This file is replay mode. It reads a market data CSV back into RawMessages and feeds them to
// the same Processor::process() that live mode uses. That's the whole trick behind getting a
// byte-for-byte identical order book file: the book code doesn't know whether a message came
// from the socket or from disk. Receive time, conn_epoch and conn_seq all come from the row,
// so decisions like resetting the book on a new conn_epoch happen exactly like they did live.
// The header has to match exactly (a trailing \r is fine). I read line by line, and if
// split_csv_row() says a quote is still open I glue the next line on, newline included, and
// try again, so a quoted field with a newline in it still works. Empty lines are skipped.
// parse_number() uses std::from_chars and also checks that it used up the whole field, so
// something like "12x" fails instead of quietly reading 12. A row without 9 fields or with a
// value that doesn't parse counts as a parse error and is skipped, and only the first 10 get
// printed so a broken file doesn't flood stderr. One market data file is one symbol, so the
// first good row picks the venue and symbol and creates the Processor, and rows for any other
// symbol are skipped. Replay passes an empty market data path, so only the order book file
// gets written. msg.payload.swap() moves the payload out of the fields vector without a copy.
// tests/unit_tests.cpp checks replay against live in test_spot_sync and test_rest_snapshot.

#include "replay.h"

#include <charconv>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <vector>

#include "csv.h"
#include "processor.h"

namespace fs = std::filesystem;

namespace {

template <typename T>
bool parse_number(const std::string& s, T& out) {
    auto res = std::from_chars(s.data(), s.data() + s.size(), out);
    return res.ec == std::errc() && res.ptr == s.data() + s.size();
}

}

bool replay_file(const std::string& path, const std::string& output_dir, Metrics& metrics) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        std::fprintf(stderr, "[replay] cannot open %s\n", path.c_str());
        return false;
    }

    std::string line;
    if (!std::getline(in, line)) {
        std::fprintf(stderr, "[replay] %s is empty\n", path.c_str());
        return false;
    }
    if (!line.empty() && line.back() == '\r') line.pop_back();
    if (line != kMarketDataHeader) {
        std::fprintf(stderr, "[replay] %s does not have the market data header\n", path.c_str());
        return false;
    }

    std::unique_ptr<Processor> processor;
    std::string symbol;
    std::string record;
    std::vector<std::string> fields;
    RawMessage msg;
    std::uint64_t line_no = 1;
    std::uint64_t bad_rows = 0;

    while (std::getline(in, line)) {
        ++line_no;
        record = line;
        while (!split_csv_row(record, fields)) {
            if (!std::getline(in, line)) {
                std::fprintf(stderr, "[replay] %s: unterminated quote at line %llu\n", path.c_str(),
                             static_cast<unsigned long long>(line_no));
                return false;
            }
            ++line_no;
            record += '\n';
            record += line;
        }
        if (fields.size() == 1 && fields[0].empty()) continue;

        Venue venue;
        StreamKind kind;
        std::int64_t tsec = 0;
        std::int64_t tnsec = 0;
        bool ok = fields.size() == 9 && parse_number(fields[0], tsec) && parse_number(fields[1], tnsec) &&
                  parse_venue(fields[2], venue) && parse_stream_kind(fields[3], kind) &&
                  parse_number(fields[4], msg.shard_id) && parse_number(fields[5], msg.conn_epoch) &&
                  parse_number(fields[6], msg.conn_seq);
        if (!ok) {
            ++bad_rows;
            bump(metrics.parse_errors);
            if (bad_rows <= 10)
                std::fprintf(stderr, "[replay] %s: skipping bad row at line %llu\n", path.c_str(),
                             static_cast<unsigned long long>(line_no));
            continue;
        }

        if (!processor) {
            symbol = fields[7];
            processor = std::make_unique<Processor>(venue, metrics);
            std::string out = (fs::path(output_dir) / (fs::path(path).stem().string() + "_orderbook.csv")).string();
            if (!processor->add_symbol(SymbolOutput{symbol, "", out})) {
                std::fprintf(stderr, "[replay] cannot create %s\n", out.c_str());
                return false;
            }
            std::fprintf(stderr, "[replay] %s -> %s\n", path.c_str(), out.c_str());
        }
        if (fields[7] != symbol) {
            ++bad_rows;
            continue;
        }

        bump(metrics.messages);
        msg.recv_ns = tsec * 1000000000LL + tnsec;
        msg.symbol_index = 0;
        msg.kind = kind;
        msg.payload.swap(fields[8]);
        processor->process(msg);
    }

    if (processor) processor->close();
    return true;
}
