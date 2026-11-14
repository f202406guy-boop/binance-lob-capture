// This header has the book thread's main class, Processor, and the structs it works with.
// Processor takes the messages in order, writes the market data row, updates the book and
// writes the order book row. Live mode and replay both go through the same process(), and
// that's the main reason replay gives back the live order book file byte for byte.
// DepthMsg is one parsed depth message (a diff, a depth5 or a REST snapshot): U goes in
// first_id, u (or lastUpdateId on spot depth5 and REST) in last_id, and pu (USD-M only) in
// prev_id. SymbolState is everything I keep per symbol: both book sides, the id the book is
// at, a depth5 that's waiting for the diffs to catch up, and the REST snapshot stuff.
// For the REST snapshot each symbol keeps its last kRecentDiffs diffs in a small ring. 30 is
// 3 s at 100 ms, so a snapshot that shows up a bit late can still be brought forward, in its
// own rest_bids/rest_asks next to the live book. market_data_path is empty in replay, since
// the market data file already exists there and nothing gets written to it.
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

constexpr std::size_t kRecentDiffs = 30;

struct DepthMsg {
    std::uint64_t first_id = 0;
    std::uint64_t last_id = 0;
    std::uint64_t prev_id = 0;
    bool has_prev = false;
    std::vector<Level> bids;
    std::vector<Level> asks;
};

struct SymbolOutput {
    std::string symbol;
    std::string market_data_path;
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
