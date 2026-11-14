#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <string>
#include <vector>

#include "csv.h"
#include "decimal.h"
#include "latency.h"
#include "metrics.h"
#include "order_book.h"
#include "processor.h"
#include "replay.h"

namespace fs = std::filesystem;

namespace {

int failures = 0;

void check(bool ok, const char* expr, int line) {
    if (!ok) {
        ++failures;
        std::fprintf(stderr, "FAIL (line %d): %s\n", line, expr);
    }
}

#define CHECK(x) check((x), #x, __LINE__)

std::string read_file(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

void test_decimal() {
    std::int64_t v = 0;
    CHECK(parse_scaled("65000.12000000", v) && v == 6500012000000LL);
    CHECK(parse_scaled("0.00000001", v) && v == 1);
    CHECK(parse_scaled("1", v) && v == 100000000);
    CHECK(parse_scaled("0.10", v) && v == 10000000);
    CHECK(parse_scaled("2.50000000000", v) && v == 250000000);
    CHECK(parse_scaled("92233720368.54775807", v) && v == std::numeric_limits<std::int64_t>::max());
    CHECK(!parse_scaled("92233720368.54775808", v));
    CHECK(!parse_scaled("99999999999999999999", v));
    CHECK(!parse_scaled("1.000000001", v));
    CHECK(!parse_scaled("", v));
    CHECK(!parse_scaled(".", v));
    CHECK(!parse_scaled("-1", v));
    CHECK(!parse_scaled("1e5", v));
}

void test_csv() {
    std::string out;
    append_csv_field(out, "plain");
    CHECK(out == "plain");

    out.clear();
    append_csv_field(out, R"({"a":"b,c"})");
    CHECK(out == R"("{""a"":""b,c""}")");

    std::vector<std::string> fields;
    CHECK(split_csv_row("1,2," + out, fields));
    CHECK(fields.size() == 3 && fields[2] == R"({"a":"b,c"})");
    CHECK(!split_csv_row("1,\"still open", fields));
}

void test_book_side() {
    BookSide bids(true);
    bids.set(100, 1);
    bids.set(102, 2);
    bids.set(101, 3);
    CHECK(bids.level(0).price == 102 && bids.level(1).price == 101 && bids.level(2).price == 100);
    bids.set(102, 0);
    CHECK(bids.level(0).price == 101 && bids.size() == 2);
    bids.set(101, 7);
    CHECK(bids.level(0).qty == 7);
    bids.set(50, 0);
    CHECK(bids.size() == 2);
    CHECK(bids.level(5) == Level{});

    BookSide asks(false);
    asks.set(105, 1);
    asks.set(103, 1);
    asks.set(104, 1);
    CHECK(asks.level(0).price == 103 && asks.level(2).price == 105);

    BookSide b(true);
    b.set(99, 1);
    b.set(98, 1);
    b.set(97, 1);
    b.set(95, 1);
    std::vector<Level> snap = {{100, 5}, {98, 6}, {96, 7}};
    b.replace_top(snap);
    CHECK(b.size() == 4);
    CHECK(b.level(0) == (Level{100, 5}));
    CHECK(b.level(1) == (Level{98, 6}));
    CHECK(b.level(2) == (Level{96, 7}));
    CHECK(b.level(3) == (Level{95, 1}));
    CHECK(b.top_matches(snap));

    BookSide a(false);
    a.set(101, 1);
    a.set(103, 1);
    a.set(104, 1);
    a.set(106, 1);
    std::vector<Level> asnap = {{100, 5}, {103, 6}, {105, 7}};
    a.replace_top(asnap);
    CHECK(a.size() == 4);
    CHECK(a.level(0) == (Level{100, 5}));
    CHECK(a.level(2) == (Level{105, 7}));
    CHECK(a.level(3) == (Level{106, 1}));
    CHECK(a.top_matches(asnap));
}

void test_latency() {
    LatencyHistogram h;
    CHECK(h.percentile(50) == 0 && h.count() == 0);
    for (int us = 1; us <= 100; ++us) h.add(us * 1000LL);
    CHECK(h.count() == 100);
    CHECK(h.percentile(50) == 50);
    CHECK(h.percentile(99) == 99);
    CHECK(h.percentile(100) == 100);
    CHECK(h.max() == 100);
    h.add(50 * 1000 * 1000LL);  // 50 ms, past the last bucket
    CHECK(h.max() == 50000);
    CHECK(h.percentile(100) == LatencyHistogram::kBuckets);
    h.add(-5);
    CHECK(h.count() == 102);
}

struct Feeder {
    Processor& p;
    RawMessage msg;

    void operator()(StreamKind kind, const char* json, std::uint32_t epoch = 0) {
        msg.kind = kind;
        msg.payload = json;
        msg.conn_epoch = epoch;
        msg.recv_ns += 1000;
        ++msg.conn_seq;
        p.process(msg);
    }
};

void test_spot_sync(const fs::path& dir) {
    Metrics m;
    Processor p(Venue::Spot, m);
    CHECK(p.add_symbol(SymbolOutput{"BTCUSDT", (dir / "market_data_test.csv").string(), (dir / "ob.csv").string()}));
    Feeder feed{p, {}};
    feed.msg.recv_ns = 1700000000LL * 1000000000LL;
    const SymbolState& s = p.state(0);

    feed(StreamKind::DepthDiff, R"({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":95,"u":99,"b":[["10.0","1"]],"a":[]})");
    CHECK(!s.valid && m.diffs_waiting == 1);

    feed(StreamKind::Depth5,
         R"({"lastUpdateId":100,"bids":[["10.00","1"],["9.90","2"]],"asks":[["10.10","3"],["10.20","4"]]})");
    CHECK(s.valid && s.last_update_id == 100);
    CHECK(s.bids.level(0).price == 1000000000 && s.asks.level(0).price == 1010000000);

    feed(StreamKind::DepthDiff,
         R"({"e":"depthUpdate","E":2,"s":"BTCUSDT","U":101,"u":103,"b":[["10.00","0"],["9.95","5"]],"a":[]})");
    CHECK(s.last_update_id == 103 && m.diffs_applied == 1);
    CHECK(s.bids.level(0) == (Level{995000000, 500000000}));
    CHECK(s.bids.level(1) == (Level{990000000, 200000000}));

    feed(StreamKind::DepthDiff, R"({"e":"depthUpdate","E":3,"s":"BTCUSDT","U":90,"u":103,"b":[],"a":[]})");
    CHECK(m.diffs_stale == 1);

    feed(StreamKind::Trade, R"({"e":"trade","E":4,"s":"BTCUSDT","t":1,"p":"10.0","q":"1","T":4,"m":true,"M":true})");
    CHECK(m.trades == 1);

    feed(StreamKind::DepthDiff, R"({"e":"depthUpdate","E":5,"s":"BTCUSDT","U":110,"u":111,"b":[],"a":[["10.10","0"]]})");
    CHECK(!s.valid && m.gaps == 1 && m.book_resets == 1);

    feed(StreamKind::Depth5, R"({"lastUpdateId":120,"bids":[["10.00","1"]],"asks":[["10.10","3"]]})");
    CHECK(s.valid && s.last_update_id == 120);

    feed(StreamKind::Depth5, R"({"lastUpdateId":120,"bids":[["10.00","1"]],"asks":[["10.10","3"]]})");
    CHECK(m.depth5_checked == 1 && m.depth5_mismatch == 0);

    feed(StreamKind::Depth5, R"({"lastUpdateId":100,"bids":[["10.00","1"]],"asks":[["10.10","3"]]})");
    CHECK(m.depth5_stale == 1);

    // snapshot arrives before the diff with the same id: checked once the diff lands
    feed(StreamKind::Depth5, R"({"lastUpdateId":125,"bids":[["10.00","1"]],"asks":[["10.05","2"]]})");
    CHECK(s.check_pending && m.depth5_checked == 1);
    feed(StreamKind::DepthDiff,
         R"({"e":"depthUpdate","E":6,"s":"BTCUSDT","U":121,"u":125,"b":[],"a":[["10.05","2"]]})");
    CHECK(!s.check_pending && s.last_update_id == 125);
    CHECK(m.depth5_checked == 2 && m.depth5_mismatch == 0);

    // a snapshot that disagrees with the book repairs the top levels
    feed(StreamKind::Depth5, R"({"lastUpdateId":125,"bids":[["10.00","9"]],"asks":[["10.05","2"]]})");
    CHECK(m.depth5_checked == 3 && m.depth5_mismatch == 1);
    CHECK(s.bids.level(0) == (Level{1000000000, 900000000}));

    feed(StreamKind::DepthDiff, R"({"e":"depthUpdate","E":7,"s":"BTCUSDT","U":126,"u":126,"b":[],"a":[]})", 1);
    CHECK(!s.valid && m.book_resets == 2);

    feed(StreamKind::Depth5, R"({"lastUpdateId":"oops","bids":[],"asks":[]})", 1);
    CHECK(m.parse_errors == 1);

    p.close();

    std::ifstream ob(dir / "ob.csv");
    std::string line;
    std::getline(ob, line);
    CHECK(line == kOrderbookHeader);
    int rows = 0;
    bool all_26 = true;
    while (std::getline(ob, line)) {
        ++rows;
        if (std::count(line.begin(), line.end(), ',') != 25) all_26 = false;
    }
    CHECK(all_26);
    CHECK(rows == 8 && rows == static_cast<int>(m.ob_rows.load()));

    // replay of the market data file must give back the same order book file
    Metrics rm;
    fs::path replay_dir = dir / "replay";
    fs::create_directories(replay_dir);
    CHECK(replay_file((dir / "market_data_test.csv").string(), replay_dir.string(), rm));
    CHECK(read_file(dir / "ob.csv") == read_file(replay_dir / "market_data_test_orderbook.csv"));
}

void test_usdm_sync(const fs::path& dir) {
    Metrics m;
    Processor p(Venue::Usdm, m);
    CHECK(p.add_symbol(SymbolOutput{"BTCUSDT", "", (dir / "ob_usdm.csv").string()}));
    Feeder feed{p, {}};
    const SymbolState& s = p.state(0);

    feed(StreamKind::Depth5,
         R"({"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":40,"u":50,"pu":39,"b":[["100.0","1"]],"a":[["100.1","1"]]})");
    CHECK(s.valid && s.last_update_id == 50);

    feed(StreamKind::DepthDiff,
         R"({"e":"depthUpdate","E":2,"T":2,"s":"BTCUSDT","U":51,"u":60,"pu":50,"b":[["100.0","2"]],"a":[]})");
    CHECK(s.last_update_id == 60 && s.bids.level(0).qty == 200000000);

    // usdm depth5 ids fall inside the next diff's range: the snapshot is folded in, then the diff applied
    feed(StreamKind::Depth5,
         R"({"e":"depthUpdate","E":3,"T":3,"s":"BTCUSDT","U":61,"u":65,"pu":60,"b":[["100.0","7"]],"a":[["100.1","1"]]})");
    CHECK(s.check_pending && s.bids.level(0).qty == 200000000);
    feed(StreamKind::DepthDiff,
         R"({"e":"depthUpdate","E":4,"T":4,"s":"BTCUSDT","U":61,"u":70,"pu":60,"b":[],"a":[["100.2","4"]]})");
    CHECK(!s.check_pending && m.depth5_folded == 1 && s.last_update_id == 70);
    CHECK(s.bids.level(0) == (Level{10000000000, 700000000}));
    CHECK(s.asks.level(1) == (Level{10020000000, 400000000}));

    feed(StreamKind::DepthDiff,
         R"({"e":"depthUpdate","E":5,"T":5,"s":"BTCUSDT","U":76,"u":80,"pu":75,"b":[],"a":[]})");
    CHECK(!s.valid && m.gaps == 1);
    p.close();
}

void test_rest_snapshot(const fs::path& dir) {
    Metrics m;
    Processor p(Venue::Spot, m);
    CHECK(p.add_symbol(SymbolOutput{"BTCUSDT", (dir / "market_data_rest.csv").string(), (dir / "ob_rest.csv").string()}));
    Feeder feed{p, {}};
    feed.msg.recv_ns = 1700000000LL * 1000000000LL;
    const SymbolState& s = p.state(0);

    feed(StreamKind::Depth5, R"({"lastUpdateId":100,"bids":[["10.00","1"]],"asks":[["10.10","3"]]})");
    feed(StreamKind::DepthDiff, R"({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":101,"u":103,"b":[["9.95","5"]],"a":[]})");
    feed(StreamKind::DepthDiff, R"({"e":"depthUpdate","E":2,"s":"BTCUSDT","U":104,"u":106,"b":[],"a":[["10.20","4"]]})");
    CHECK(s.valid && s.last_update_id == 106);

    // snapshot behind the live book: caught up with the kept diffs, then swapped in
    feed(StreamKind::RestSnapshot,
         R"({"lastUpdateId":102,"bids":[["10.00","1"],["9.90","2"],["9.80","7"]],"asks":[["10.10","3"],["10.30","6"]]})");
    CHECK(m.rest_snapshots == 1 && m.rest_applied == 1 && !s.rest_pending);
    CHECK(s.last_update_id == 106);
    CHECK(s.bids.level(1) == (Level{995000000, 500000000}));
    CHECK(s.bids.level(2) == (Level{990000000, 200000000}));
    CHECK(s.bids.level(3) == (Level{980000000, 700000000}));
    CHECK(s.asks.level(1) == (Level{1020000000, 400000000}));
    CHECK(s.asks.level(2) == (Level{1030000000, 600000000}));
    CHECK(m.rest_top_mismatch == 1);  // the live book only knew 2 bid levels

    // snapshot ahead of the live book: waits until a diff brings both to the same id
    feed(StreamKind::RestSnapshot,
         R"({"lastUpdateId":110,"bids":[["10.00","1"],["9.95","5"],["9.70","9"]],"asks":[["10.10","3"],["10.20","4"]]})");
    CHECK(s.rest_pending && m.rest_applied == 1);
    feed(StreamKind::DepthDiff, R"({"e":"depthUpdate","E":3,"s":"BTCUSDT","U":107,"u":108,"b":[["9.60","1"]],"a":[]})");
    CHECK(s.rest_pending && s.last_update_id == 108);
    feed(StreamKind::DepthDiff, R"({"e":"depthUpdate","E":4,"s":"BTCUSDT","U":109,"u":112,"b":[],"a":[["10.40","8"]]})");
    CHECK(!s.rest_pending && m.rest_applied == 2 && s.last_update_id == 112);
    CHECK(s.bids.level(2) == (Level{970000000, 900000000}));
    CHECK(s.bids.size() == 3);  // 9.60 was gone again by id 110
    CHECK(s.asks.level(2) == (Level{1040000000, 800000000}));

    // snapshot so old that the kept diffs don't connect to it: dropped, book untouched
    feed(StreamKind::RestSnapshot, R"({"lastUpdateId":50,"bids":[["1.00","1"]],"asks":[["2.00","1"]]})");
    CHECK(m.rest_dropped == 1 && !s.rest_pending && m.rest_applied == 2);
    CHECK(s.bids.level(0) == (Level{1000000000, 100000000}));

    // after a reconnect the book is reset; a snapshot seeds it straight away
    feed(StreamKind::DepthDiff, R"({"e":"depthUpdate","E":5,"s":"BTCUSDT","U":113,"u":115,"b":[],"a":[]})", 1);
    CHECK(!s.valid);
    feed(StreamKind::RestSnapshot, R"({"lastUpdateId":200,"bids":[["10.00","2"]],"asks":[["10.10","2"]]})", 1);
    CHECK(s.valid && s.last_update_id == 200 && m.rest_applied == 3);
    feed(StreamKind::DepthDiff, R"({"e":"depthUpdate","E":6,"s":"BTCUSDT","U":201,"u":205,"b":[["10.00","4"]],"a":[]})", 1);
    CHECK(s.last_update_id == 205 && s.bids.level(0).qty == 400000000);

    // a gap in the live book while a continuous REST book is pending: reset, then the REST book takes over
    feed(StreamKind::RestSnapshot, R"({"lastUpdateId":210,"bids":[["10.00","4"],["9.00","1"]],"asks":[["10.10","2"]]})", 1);
    CHECK(s.rest_pending);
    feed(StreamKind::DepthDiff, R"({"e":"depthUpdate","E":7,"s":"BTCUSDT","U":208,"u":212,"b":[],"a":[]})", 1);
    CHECK(m.gaps == 1 && s.valid && s.last_update_id == 212 && m.rest_applied == 4);
    CHECK(s.bids.level(1) == (Level{900000000, 100000000}));

    p.close();
    Metrics rm;
    fs::path replay_dir = dir / "replay_rest";
    fs::create_directories(replay_dir);
    CHECK(replay_file((dir / "market_data_rest.csv").string(), replay_dir.string(), rm));
    CHECK(read_file(dir / "ob_rest.csv") == read_file(replay_dir / "market_data_rest_orderbook.csv"));
    CHECK(rm.rest_applied == 4);

    // usdm: the snapshot id falls inside a diff's range, pu decides continuity
    Metrics um;
    Processor up(Venue::Usdm, um);
    CHECK(up.add_symbol(SymbolOutput{"BTCUSDT", "", (dir / "ob_rest_usdm.csv").string()}));
    Feeder ufeed{up, {}};
    const SymbolState& us = up.state(0);
    ufeed(StreamKind::Depth5,
          R"({"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":40,"u":50,"pu":39,"b":[["100.0","1"]],"a":[["100.1","1"]]})");
    ufeed(StreamKind::DepthDiff,
          R"({"e":"depthUpdate","E":2,"T":2,"s":"BTCUSDT","U":51,"u":60,"pu":50,"b":[["99.9","2"]],"a":[]})");
    ufeed(StreamKind::RestSnapshot,
          R"({"lastUpdateId":55,"E":2,"T":2,"bids":[["100.0","1"],["99.5","3"]],"asks":[["100.1","1"]]})");
    CHECK(um.rest_applied == 1 && us.last_update_id == 60);
    CHECK(us.bids.level(1) == (Level{9990000000, 200000000}));
    CHECK(us.bids.level(2) == (Level{9950000000, 300000000}));
    up.close();
}

}  // namespace

int main() {
    fs::path dir = fs::temp_directory_path() / "binance_lob_unit_tests";
    fs::remove_all(dir);
    fs::create_directories(dir);

    test_decimal();
    test_csv();
    test_book_side();
    test_latency();
    test_spot_sync(dir);
    test_usdm_sync(dir);
    test_rest_snapshot(dir);

    if (failures == 0) {
        std::printf("all tests passed\n");
        return 0;
    }
    std::printf("%d check(s) failed\n", failures);
    return 1;
}
