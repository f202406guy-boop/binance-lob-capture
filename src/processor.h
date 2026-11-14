#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <simdjson.h>

#include "csv.h"
#include "metrics.h"
#include "order_book.h"
#include "types.h"

constexpr const char* kMarketDataHeader =
    "recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json";

constexpr const char* kOrderbookHeader =
    "tsec,tnsec,seqNo,id,type,side,"
    "bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,"
    "ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4";

constexpr int kBookDepth = 5;

// How many recent diffs each symbol keeps (3 s at 100 ms) so a REST snapshot
// that arrives a bit later can be brought forward to the live book's id.
constexpr std::size_t kRecentDiffs = 30;

// One parsed depth message (diff, depth5 or REST snapshot).
struct DepthMsg {
    std::uint64_t first_id = 0;  // U
    std::uint64_t last_id = 0;   // u, or lastUpdateId on spot depth5 and REST
    std::uint64_t prev_id = 0;   // pu (usdm only)
    bool has_prev = false;
    std::vector<Level> bids;
    std::vector<Level> asks;
};

struct SymbolOutput {
    std::string symbol;
    std::string market_data_path;  // empty in replay mode
    std::string orderbook_path;
};

struct SymbolState {
    std::string symbol;
    std::int32_t id = 0;
    BookSide bids{true};
    BookSide asks{false};
    bool valid = false;
    std::uint64_t last_update_id = 0;
    bool seen_epoch = false;
    std::uint32_t epoch = 0;
    std::uint64_t seq_no = 0;
    bool check_pending = false;
    std::uint64_t check_id = 0;
    std::vector<Level> check_bids;
    std::vector<Level> check_asks;

    // REST snapshot: the last few diffs (a ring), and a full-depth book built
    // from a snapshot that is still catching up with the live book.
    std::vector<DepthMsg> recent;
    std::size_t recent_next = 0;
    std::size_t recent_count = 0;
    bool rest_pending = false;
    std::uint64_t rest_id = 0;
    BookSide rest_bids{true};
    BookSide rest_asks{false};

    BufferedFile market_data;
    BufferedFile orderbook;
};

// Consumes RawMessages in order: writes the market data row, updates the book
// and writes the order book row. Live mode and replay mode both go through
// process(), which is why replay reproduces the live order book file exactly.
class Processor {
public:
    Processor(Venue venue, Metrics& metrics);

    bool add_symbol(const SymbolOutput& out);
    void process(RawMessage& msg);
    void flush();
    void close();

    std::size_t symbol_count() const { return symbols_.size(); }
    const SymbolState& state(std::size_t i) const { return *symbols_[i]; }

private:
    void on_diff(SymbolState& s, RawMessage& msg);
    void apply_diff(SymbolState& s, RawMessage& msg);
    void on_depth5(SymbolState& s, RawMessage& msg);
    void on_trade(SymbolState& s, RawMessage& msg);
    void on_rest_snapshot(SymbolState& s, RawMessage& msg);
    void remember_diff(SymbolState& s);
    void advance_rest_book(SymbolState& s, const DepthMsg& d);
    void try_rest_swap(SymbolState& s, const RawMessage& msg);
    std::uint64_t prev_update_id(const DepthMsg& d) const;
    bool parse_depth(std::string& payload);
    void verify_top(SymbolState& s, const RawMessage& msg, const std::vector<Level>& bids,
                    const std::vector<Level>& asks);
    void reset_book(SymbolState& s, const RawMessage& msg);
    void write_market_data(SymbolState& s, const RawMessage& msg);
    void write_book_row(SymbolState& s, const RawMessage& msg, char type, char side);
    void count_parse_error(const SymbolState& s, const char* what);

    Venue venue_;
    Metrics& metrics_;
    std::vector<std::unique_ptr<SymbolState>> symbols_;
    simdjson::ondemand::parser parser_;
    DepthMsg depth_;
    std::string row_;
};
