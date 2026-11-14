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

}  // namespace

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
