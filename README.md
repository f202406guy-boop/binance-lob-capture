# binance-lob-capture

C++17 program that connects to Binance public WebSocket streams, writes every inbound
`depth`, `depth5` and `trade` message to a market data CSV, keeps a local order book from
that stream and writes the top 5 levels after every applied event to an order book CSV.

It also has a replay mode that rebuilds the order book CSV from a saved market data CSV
with no network access. Live and replay go through the same code, so the output is
byte-for-byte identical (see [Sample run](#sample-run)).

## Build

| | |
|---|---|
| Compiler | GCC 11.4 (Ubuntu 22.04) |
| Standard | C++17 |
| Build system | CMake >= 3.16 |
| Dependencies | Boost >= 1.74 (Beast + Asio, header-only), OpenSSL 3, simdjson 3.10.1, pthreads |

simdjson is used from the system if CMake can find it, otherwise CMake downloads v3.10.1
from GitHub at configure time.

```bash
# Ubuntu 22.04 / Debian
sudo apt-get update
sudo apt-get install -y build-essential cmake ninja-build git libboost-dev libssl-dev

git clone https://github.com/<your-username>/binance-lob-capture.git
cd binance-lob-capture

cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

Warnings are on for our code:
`-Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wno-sign-conversion`. I turned
`-Wsign-conversion` off because mixing `size_t` indices with signed counts made it noisy.
`-Wconversion` still catches narrowing. The build is warning-free. The default build type
is `Release` (`-O3 -DNDEBUG`).

Sanitizer builds:

```bash
cmake -B build-asan -G Ninja -DCMAKE_BUILD_TYPE=Debug -DENABLE_ASAN=ON   # ASan + UBSan
cmake -B build-tsan -G Ninja -DCMAKE_BUILD_TYPE=Debug -DENABLE_TSAN=ON   # ThreadSanitizer
```

## Run

```bash
# one symbol, spot, stop after 120 s (or press Ctrl+C at any time)
./build/binance_capture --venue spot --symbols BTCUSDT --output-dir ./output --duration 120

# several symbols on one connection
./build/binance_capture --venue spot --symbols BTCUSDT,ETHUSDT --output-dir ./output

# USD-M futures
./build/binance_capture --venue usdm --symbols BTCUSDT --output-dir ./output --duration 120

# lowest latency: the book thread spins instead of sleeping (uses one full core)
./build/binance_capture --venue spot --symbols BTCUSDT --busy-poll

# full-depth book: also fetch the REST /depth snapshot after every connect
./build/binance_capture --venue spot --symbols BTCUSDT --rest-snapshot

# several symbols, at most 2 REST snapshot requests in flight at once (default 4, max 8)
./build/binance_capture --venue spot --symbols BTCUSDT,ETHUSDT,SOLUSDT --rest-snapshot --rest-parallel 2

# replay: rebuild the order book CSV from a market data CSV, no network
./build/binance_capture --replay ./output/market_data_spot_BTCUSDT_2026-11-14.csv --output-dir ./replay_output

# unit tests
cmake --build build --target tests      # builds only the unit tests (the full build above already did)
./build/tests/unit_tests                # or: (cd build && ctest --output-on-failure)
```

| Option | Meaning |
|---|---|
| `--venue spot\|usdm` | `spot` = `wss://stream.binance.com:9443/stream`, `usdm` = `wss://fstream.binance.com/public/stream` |
| `--symbols` | comma separated, e.g. `BTCUSDT,ETHUSDT` (1 to 50 symbols). The input is uppercased first, then each symbol must be letters and digits only |
| `--output-dir` | where CSVs go (default `./output`, created if missing) |
| `--duration` | seconds to run, `0` or omitted = until SIGINT/SIGTERM. A value that is not a whole number `>= 0` is rejected |
| `--busy-poll` | the book thread never sleeps while waiting for messages: about 1 µs wait instead of ~150 µs, at the cost of one core at 100% (see [Design choices](#design-choices-and-measurements)) |
| `--rest-snapshot` | after every connect, fetch each symbol's REST `/depth` snapshot (1000 levels a side) and merge it into the book, see [REST snapshot](#rest-snapshot---rest-snapshot). Off by default |
| `--rest-parallel N` | with `--rest-snapshot`: up to N REST snapshot requests in flight at once, one symbol each. 1 to 8, default 4 (see [REST snapshot](#rest-snapshot---rest-snapshot)) |
| `--replay` | one or more market data CSVs (comma separated) |

For each symbol the program subscribes to `<symbol>@depth@100ms`, `<symbol>@depth5@100ms`
and `<symbol>@trade` on one combined-stream connection.

## Code layout

| File | What it does |
|---|---|
| `src/types.h` | shared names: `Venue`, `StreamKind`, `RawMessage` (one message in the queue), the wall clock |
| `src/decimal.h` | decimal string → `int64` scaled by 10^8, integer math only |
| `src/csv.h/.cpp` | CSV quoting and splitting, number → text, `BufferedFile` (256 KB write buffer) |
| `src/order_book.h/.cpp` | `BookSide`: one side of the book |
| `src/spsc_queue.h` | lock-free single-producer single-consumer ring between the two threads |
| `src/metrics.h`, `src/latency.h` | counters and the latency histogram |
| `src/ws_client.h/.cpp` | network thread: TLS + WebSocket, reconnect with backoff, receive timestamp, push to the ring |
| `src/rest_client.h/.cpp` | async HTTPS GET on the same thread, for the optional REST depth snapshot (one client per request in flight, `--rest-parallel` of them) |
| `src/processor.h/.cpp` | book thread: JSON → sequence checks → book → CSV rows (live and replay both use it) |
| `src/replay.h/.cpp` | reads a market data CSV back into `RawMessage`s for the processor |
| `src/main.cpp` | options, threads, signals, periodic stats |
| `tests/unit_tests.cpp` | unit tests (`ctest`, binary `build/tests/unit_tests`) |
| `tests/verify_against_rest.cpp` | full-depth accuracy check against REST `/depth` snapshots saved by another process (target `verify_against_rest`, not part of `ctest`), see [Full-depth accuracy](#full-depth-accuracy-against-binance) |
| `bench/wait_bench.cpp` | the benchmark behind the `--busy-poll` choice, see [Design choices](#design-choices-and-measurements) |
| `bench/rest_parallel_bench.sh` | the live benchmark behind the `--rest-parallel` default, see [Running the benchmarks](#running-the-benchmarks) |

Headers hold declarations (and the small template/inline code), `.cpp` files hold the
code. That way `binance_capture` and `unit_tests` link the same `lob_core` library
without compiling anything twice.

## Output files

One pair of files per symbol and run:

```
output/market_data_<venue>_<SYMBOL>_<UTC-date>.csv
output/market_data_<venue>_<SYMBOL>_<UTC-date>_orderbook.csv
```

The UTC date is taken when the run starts (a run that crosses midnight keeps writing to
the same file). An existing capture is never overwritten: if the name is taken the program
adds `_2`, `_3`, ...

Both files are UTF-8, comma separated, RFC 4180 quoting (a field with a comma, quote or
newline is wrapped in quotes and inner quotes are doubled), header in the first row.

### Market data CSV

`recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json`

| Column | Value |
|---|---|
| `recv_tsec`, `recv_tnsec` | receive time, see [Timestamps](#timestamps) |
| `venue` | `spot` or `usdm` |
| `stream_kind` | `depth_diff`, `depth5` or `trade`, plus `rest_snapshot` (the REST `/depth` response) with `--rest-snapshot` |
| `shard_id` | always `0` (one connection) |
| `conn_epoch` | starts at 0, +1 on every reconnect attempt |
| `conn_seq` | 0, 1, 2, ... per connection, across all symbols on it, so it is monotonic within `(shard_id, conn_epoch)` |
| `symbol` | uppercase |
| `payload_json` | the inner `data` object, minified, CSV-escaped |

There is one row for every message read from the socket, in the order it was read, except
frames with an unknown stream name or an envelope that does not parse. Those are counted in
`unknown_streams` / `parse_errors` and not written. This is also the order the order book
thread processes them in.

### Order book CSV

Header (exactly):

```
tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4
```

26 columns on every row.

| Column | Value |
|---|---|
| `tsec`, `tnsec` | same receive time as the market data row that produced this row |
| `seqNo` | 1, 2, 3, ... one per row written, per file |
| `id` | FNV-1a 32-bit hash of the uppercase symbol, masked to 31 bits (`BTCUSDT` is always the same id, in every run and in replay, whatever order symbols are passed in) |
| `type` | see table below |
| `side` | `B` bid side, `S` ask side, `N` both / not applicable |
| `bid0..4`, `ask0..4` | top 5 prices, best first, scaled by 10^8 |
| `bid_size0..4`, `ask_size0..4` | sizes at those prices, scaled by 10^8 |

Missing levels (fewer than 5 on a side) are written as `0` price and `0` size.

| `type` | Event | `side` |
|---|---|---|
| `D` | a `depthUpdate` was applied | `B` if it only touched bids, `S` if only asks, `N` if both or empty |
| `S` | a `depth5` was applied: it seeded the book (at start or after a reset), repaired it after a mismatch, or was folded in on USD-M (see [below](#order-book-sequencing-and-reconnects)). A folded snapshot's `S` row comes right before the `D` row of the diff it was folded into, with that diff's receive time. A REST snapshot swap also writes an `S` row | `N` |
| `T` | a trade printed while the book is valid (no `T` rows before the first seed or between a reset and the next seed). The book is **not** changed, the row only marks the trade | aggressor: `m=true` (buyer is maker) gives `S`, `m=false` gives `B` |
| `R` | the book was reset (sequence gap or new `conn_epoch`), all levels are 0 | `N` |

Every applied `depth5` writes its own `S` row. A `depth5` that only confirms the book (same
id, same top 5) changes nothing and writes no row; it is counted in `depth5_checked`. So the
number of `S` rows is `depth5_applied + rest_applied`.

## Scaling

Binance sends prices and quantities as decimal strings. Both are converted to `int64` with a
fixed scale of **10^8** (8 decimal places), straight from the string with integer math:
`"65000.12000000"` becomes `6500012000000`. No `float`/`double` is used anywhere on the
price/size path. Only reporting code uses `double` (the latency percentiles, the replay
timer and the benchmarks).

The parser rejects anything that is not a plain non-negative decimal, more than 8 non-zero
decimal places (it would lose precision), and values that do not fit in `int64`. The largest
value that fits is 92,233,720,368.54775807, which is plenty for any price and for quantities
of very low-priced coins. A rejected message is counted as a parse error and is not applied
to the book. It is still in the market data file.

## Timestamps

Wall clock (`std::chrono::system_clock`, i.e. `CLOCK_REALTIME`) taken in the read
completion handler, right after the whole WebSocket message has been read and before it is
parsed. It is split into whole seconds and the nanosecond remainder.

The order book row uses the receive time of the message that produced it. I picked receive
time over Binance's `E` field because spot `depth5` messages have no event time at all, so
`E` could not be used for every row. Also, receive time is what replay reads back from the
market data file, which keeps replay exact.

Receive time is only as good as the local clock. In the sample run (WSL2), `recv - E`
drifted from about -1.7 s to -2.3 s, which means the VM clock was behind Binance and
falling further behind. Then WSL's time sync stepped the clock about +2.4 s, at
recv 1794656935.886 (spot file line 3227, USD-M file line 3009). So `tsec`/`tnsec` in the
sample contain one jump of ~2.4 s. That is also why I don't report latency as `recv - E`,
and why durations are measured with `steady_clock`. On a real box the clock would be
disciplined with chrony/PTP.

## Order book, sequencing and reconnects

Every message goes through one function, `Processor::process()`, in file order.

**Diff (`depthUpdate`).** Each level carries the new absolute quantity at that price, and
`0` removes the level. Continuity uses Binance's update ids:

- spot: the previous id is `U - 1`
- usdm: the previous id is `pu`

With `last` = the update id the book is at:

| Condition | Action |
|---|---|
| book not valid yet | not applied (`diffs_waiting`), no row. It is still kept for a REST snapshot, so it gets parsed, and a malformed one counts as a parse error |
| `u <= last` | already in the book, drop (`diffs_stale`), no row |
| previous id `> last` | updates are missing: **reset** the book, write an `R` row, wait for the next `depth5` (`gaps`) |
| otherwise | apply all levels, `last = u`, write a `D` row |

**depth5.** These are snapshots of the top 5 levels, so they *replace* rather than add. On
each side, every level from the best price down to the snapshot's 5th price is replaced by
the snapshot. Deeper levels built from diffs are kept. The snapshot id is `lastUpdateId` on
spot and `u` on usdm. While the book is in sync, the diffs are the source of truth and
`depth5` is the check:

| Situation | Action |
|---|---|
| book not valid (start, or after a reset) | the snapshot **seeds** the book, `last` = snapshot id, `S` row |
| snapshot id `< last` | older than the book, ignored (`depth5_stale`) |
| snapshot id `== last` | compared with our diff-built top 5 (`depth5_checked`). Equal: nothing changes, no row. Different (`depth5_mismatch`): the snapshot **repairs** the top levels, `S` row |
| snapshot id `> last` | kept until the diffs catch up, see below |

A kept snapshot is resolved by the next diff that gets applied:

- **The diff ends exactly at the snapshot id.** This is normal on spot, where the snapshot
  arrives a few ms before the diff with the same id. The diff is applied first, then the
  two are compared as above.
- **The diff's range contains the snapshot id.** This is normal on USD-M, where `depth5` and
  diffs are cut about 25 ms apart, so their ids usually do not line up. In the sample, 318
  of the 1,214 `depth5` had an id equal to a diff's `u` and were compared exactly, and the
  other 895 were folded (the remaining one seeded the book). When the ids don't line up, an
  exact comparison is not possible, so the snapshot is **folded in** just before the diff
  is applied (`depth5_folded`) and gets its own `S` row, right before the diff's `D` row and
  with the diff's receive time. The top 5 in that `S` row are exactly what Binance
  published in that `depth5`. This is safe: the diff carries every change from `last` up
  to its `u`, so any level the snapshot made stale is overwritten, and the book ends exact
  at `u`. Folding also fills in any top levels we were missing.

Every applied `depth5` (seed, repair or fold) writes its own `S` row. One that only confirms
the book writes none.

I found the spot/USD-M difference by looking at the captured ids. On spot, the `depth5`
`lastUpdateId` equals a diff's `u`. On USD-M, the `depth5` `u` usually falls inside the
next diff's `U..u`.

**Trades** never change the book. A `T` row is written only to annotate when a trade printed
and who the aggressor was.

**Reconnects.** Any socket error (read error, idle timeout, failed handshake) closes the
connection, increments `conn_epoch` and reconnects with exponential backoff (0.5 s doubling
up to 30 s, reset after a successful connect). Binance's 24h disconnect is handled the
same way. When the book thread sees a message with a new `conn_epoch` and the book is
valid, it resets the book (`R` row) because updates may have been missed while
disconnected. It then waits for the next `depth5`, normally within 100 ms. Because the
reset is driven by the `conn_epoch` column, replay makes exactly the same decision.

**Without a REST snapshot (the default),** right after a seed the book only knows the top 5
levels plus whatever diffs touch afterwards. If the price moves through the seeded levels
before the deeper ones have been touched, a level can be missing. The `depth5` check
catches exactly this and repairs it. In the sample spot run, the only 2 mismatches
happened 2 s and 6 s after start, followed by 0 in the remaining ~115 s. Over a 10-minute
spot run there were 48 repairs in 6,296 checks (0.8%). Most were bunched at the start (14
in the first minute) and during one fast move (22 in minutes 5 and 6). The other minutes
had 0 to 4.

### REST snapshot (`--rest-snapshot`)

The optional stretch goal: a full-depth book from the first second. The catch is replay.
The REST response is not one of the three streams, so if it only lived in memory, replay
could no longer rebuild the same book from the market data file alone. So the response
goes into the market data file as its own row type:

1. After every WebSocket connect, the network thread fetches
   `GET /api/v3/depth?symbol=X&limit=1000` (spot) or `/fapi/v1/depth?...&limit=1000`
   (USD-M) for each symbol, up to `--rest-parallel` at a time (default 4). The requests
   are async on the same `io_context`, so the socket read is never blocked, and TLS peer
   and hostname verification are on like for the WebSocket.
2. The response is pushed into the same queue as `stream_kind = rest_snapshot`, so it lands
   in the market data CSV in the order it arrived. Replay sees exactly what live saw and
   needs no flag.
3. The book thread always keeps the last 30 diffs per symbol (3 s). A snapshot has its own
   update id `L`, usually a little behind or ahead of the live book. The book thread
   builds a separate full-depth book from it, then brings that book forward with the kept
   diffs (and then the incoming ones), using the same sequence rules as the live book:
   skip `u <= L`, and if a diff's previous id is past the REST book's id, a diff is
   missing and the snapshot is dropped (`rest_dropped`).
4. As soon as the REST book and the live book are at the same update id, the REST book is
   swapped in and an `S` row is written (`rest_applied`). If the book was not valid (start,
   or after a reset), it is swapped in right away. Before swapping, the two top 5 are
   compared (`rest_top_mismatch`): a difference means the live book was missing a level.
5. `depth5` keeps doing the checking and repairing exactly as before.

Without `--rest-snapshot` there are no `rest_snapshot` rows, so the REST code never changes
the book or the order book file.

**Request weight.** `limit=1000` costs 50 on spot (budget 6,000 a minute) and 20 on USD-M
(budget 2,400 a minute), so one round for 50 symbols is 2,500 or 1,000. The client runs at
most one round a minute, even if the connection keeps dropping and reconnecting. On an
HTTP 429 (too many requests) or 418 (IP banned for not backing off) it stops asking for the
rest of that connection, and those books use `depth5` seeding. I use `limit=1000` and not
5000 because on spot 5000 costs 250 weight instead of 50, for levels the program never
writes (it outputs the top 5).

**How many requests at once (`--rest-parallel`).** `bench/rest_parallel_bench.sh` runs the
program live on spot with 20 symbols and `--rest-snapshot`, with N = 1, 2, 4 and 8 at the
same time, so all four see the same network. It measures the time from the first market
data row to the last `rest_snapshot` row. I ran it twice, 83 s apart:

| N | run 1 | run 2 |
|---|---|---|
| 1 | 7.226 s | 6.703 s |
| 2 | 3.336 s | 3.924 s |
| 4 (default) | 1.849 s | 1.750 s |
| 8 | 1.105 s | 1.019 s |

Every run got 20/20 snapshots and 0 REST errors. I picked 4 as the default. It is about
3.9× faster than one at a time. Going from 4 to 8 only saves another ~0.75 s, but it
doubles the TLS handshakes running on the io thread (the same thread that reads the
WebSocket) and the requests already in flight when a 429 arrives. The request weight of a
round is the same for every N.

**What it does not do yet:**

- After a sequence gap, a pending REST book that is still continuous takes over. Otherwise
  the book reseeds from `depth5`. Fetching a fresh snapshot after a gap would need the
  book thread to signal the network thread.
- A snapshot that fails (network error, timeout, non-200) or is dropped is not retried
  until the next connect.
- If a reconnect happens within 60 s of the last round, the round is skipped. Symbols whose
  request was in flight when the old connection dropped are then not fetched again on the
  new connection, and their books seed from `depth5`.
- If DNS stops answering in the middle of a round, a REST lookup can delay shutdown by up
  to ~30 s. A running `getaddrinfo` can't be cancelled.

**Measured.** Spot and USD-M BTCUSDT for the same 5 minutes, with and without the flag:

| | `depth5` checks | mismatches (missing levels, repaired) | REST |
|---|---|---|---|
| spot, `depth5` seeding only | 3,097 | **15** | |
| spot, `--rest-snapshot` | 3,096 | **0** | 1 fetched (64 KB), 1 applied, top 5 agreed at the swap |
| USD-M, `depth5` seeding only | 53 | 0 | |
| USD-M, `--rest-snapshot` | 53 | 0 | 1 fetched (42 KB), 1 applied |

Both REST captures replayed byte for byte. In a 30-second spot run with BTCUSDT and
ETHUSDT (fetched one after another, before `--rest-parallel` existed), the snapshots
arrived 0.48 s and 0.77 s after start. Both repairs in that run were on ETHUSDT before its
snapshot arrived, and there were 0 after.

### Full-depth accuracy against Binance

The `depth5` check only covers the top 5 levels. To check the rest of the book, I compared
it with Binance's own REST `/depth`.

**Method.** During a 10-minute live capture of BTCUSDT (spot and USD-M, both with
`--rest-snapshot`), a separate process fetched Binance's REST `/depth` (`limit=1000`) every
10 s and saved each response. `tests/verify_against_rest.cpp` (CMake target
`verify_against_rest`) replays the market data file through the same `Processor`. It brings
each saved snapshot forward with the captured diffs (same sequence rules) until it is at the
same update id as the book, then compares both sides level by level from the best price,
over the price range the snapshot covered.

```bash
cmake --build build --target verify_against_rest
./build/verify_against_rest MARKET_DATA_CSV SNAPSHOT.json [SNAPSHOT.json ...]
```

It prints one line per snapshot and a summary.

| | spot | USD-M |
|---|---|---|
| snapshots compared | 58 | 45 |
| exact through the top 100 levels on both sides | 58 | 45 |
| exact through all ~1000 levels on both sides | 16 | 40 |
| fewest exact levels in any snapshot | bids 589, asks 959 | bids 589, asks 951 |

Every first mismatch (one per side that was not exact: 43 on spot, 5 on USD-M) was at a
price outside the range our own startup REST snapshot covered (spot: bids below 65,163.50
or asks above 65,828.15; USD-M: bids below 65,330.90 or asks above 65,637.20). 0 were
inside it.

The reason: the book knows exactly the levels in its startup snapshot plus every level a
diff has touched since. When the price drifts, Binance's 1000-level window includes deeper
prices that nobody has touched since we started, so we never heard of them. Within every
price range the book was told about, it matched Binance exactly, in all 103 snapshots.

In the same capture there were 0 gaps and 0 reconnects. The only spot `depth5` repair
happened 0.197 s after start, before our REST snapshot arrived at 0.298 s.

To check the checker itself, I also gave it a deliberately edited snapshot. It was caught
(bids matched 1/5).

## Threading and I/O

```
 network thread (io_context)                      book thread
 ---------------------------                      -----------
 async read frame                                 pop slot
 stamp receive time                               write market data row  -> buffer -> file
 simdjson: get "stream", raw "data"   SPSC ring   parse payload (simdjson)
 minify payload into slot           ===========>  update book
 publish slot                        4096 slots   write order book row   -> buffer -> file
```

**Why two threads.** The one thing I didn't want was the socket waiting on the disk. If a
single thread read a frame, updated the book and wrote two CSV rows, then any slow write
(the OS flushing its page cache, a busy disk, an antivirus scan on Windows) would stop the
socket from being read, and Binance's messages would pile up in the kernel. So the program
has exactly two threads with one job each. The **network thread** runs the Boost.Asio
`io_context`: it reads WebSocket frames, stamps the receive time, strips the
`{"stream":...,"data":...}` envelope and hands the payload over. It never touches a file.
The **book thread** takes messages in order, writes the market data row, parses the JSON,
updates the book and writes the order book row. It never touches the socket. Everything
network-related (reconnects, timers, signals, the REST snapshot requests) lives on the
network thread, so none of it needs locking either.

**How they talk: a lock-free single-producer/single-consumer ring.** Between them sits a
ring of 4096 `RawMessage` slots (`src/spsc_queue.h`). With exactly one writer and one
reader, it only needs two atomic counters: `head` (moved by the network thread) and
`tail` (moved by the book thread). The writer fills a slot and then publishes it with a
release store on `head`. The reader loads `head` with acquire, so it is guaranteed to see
the fully written slot. That pair is the only synchronisation on the per-message path:
no mutex, no condition variable. `head` and `tail` sit on separate 64-byte cache lines so
the two cores don't keep stealing the same line from each other, and each side keeps a
cached copy of the other side's counter, so it only reads the shared atomic when it looks
full or empty. ThreadSanitizer runs (live, both waiting modes) report no data races.

**No allocation per message.** The slots are created once and reused, and the
`std::string` inside each slot keeps its capacity, so after the first few seconds copying
a payload in is just a `memcpy`. The book thread reuses one row buffer, its level vectors
and one simdjson parser, and the network thread reuses its frame buffer and parser. The
one exception is the ~64 KB REST snapshot, whose slot memory is freed right after use so a
few big messages don't keep slots large forever.

**Back-pressure.** If the book thread ever fell behind and the ring filled up, I had two
options: drop messages, or wait. Dropping would leave holes in the audit file, which is the
one thing that file must not have, so the network thread waits (counted as
`queue_full_waits`). While it waits it isn't reading the socket, so TCP flow control slows
Binance down instead of anything being lost. At 50 to 200 messages a second per symbol,
4096 slots hold 20 to 80 seconds of data for one symbol, and about a second with 50 busy
symbols. `queue_full_waits` stayed 0 in every run.

**Writing files: buffered writes, not a third writer thread.** Each CSV file collects rows
in its own 256 KB `std::string` and calls `fwrite` once per full buffer. I turned stdio's
own buffering off (`setvbuf(..., _IONBF)`) so each byte isn't copied twice. Formatting a
row is a few `std::to_chars` calls into a reused buffer, well under a microsecond. I
thought about a dedicated writer thread, but it would mean a second queue and a second
hand-off for every row, while the only cost it would remove is the occasional 256 KB
`fwrite`, which shows up in the latency `max` and not in the median. Since the network
thread is already protected from the disk by the ring, I kept it to two threads. If I had
to cut the tail further, that writer thread (or `io_uring`) is the next step. The book
thread also flushes every second when it's idle, so the files on disk stay current during
a live run.

**When there's nothing to do.** An idle book thread yields 64 times and then sleeps
200 µs, so it uses almost no CPU in a quiet market. The cost is that a message can wait up
to a couple of hundred microseconds before it's picked up (measured p50 about 140 µs).
`--busy-poll` makes it spin instead, which brings that to about 1 µs at the price of one
full core. I measured a condition variable too, and it was no better than sleeping inside
WSL (see `bench/wait_bench.cpp`).

**Shutdown.** Ctrl+C, SIGTERM or `--duration` all go through `asio::signal_set` or a timer,
so the handler runs as a normal callback on the network thread, not inside a signal
handler. It stops the timers, sends a polite WebSocket close (and force-closes the socket
after 2 s at most), and `io_context::run()` returns. Then the book thread drains whatever
is still in the ring, both files are flushed and closed, and the final counters are
printed, so every message that was received ends up in the file. One bug I found here:
a timer that has already expired can't be cancelled any more, its handler still runs. A
stats timer firing in the same instant as the `--duration` timer re-armed itself and kept
`run()` alive forever (an ASan run hung on it). Now every handler checks a `stopping` flag,
and after the first signal SIGINT/SIGTERM get their default action back, so a second
Ctrl+C always kills the process.

**Lifetimes.** Everything that owns something is RAII: `BufferedFile` owns its `FILE*`,
the WebSocket lives in a `std::optional` that is recreated for each connection, and the
client object outlives `run()`. Async handlers capture `this` plus the connection's epoch
number, so a late callback from an old connection sees a different epoch and returns
without touching anything. ASan + UBSan runs, including live captures with the REST
snapshot on, came out clean.

### Why these choices

- **Beast + Asio + OpenSSL.** These are the de-facto standard for WebSocket over TLS in
  C++. TLS peer verification and hostname verification are on, and SNI is set (Binance
  needs it).
- **simdjson On-Demand.** It is the fastest mainstream C++ JSON parser. It finds the
  structure with SIMD in one pass, then we only touch the fields we use (`U`, `u`, `pu`,
  `b`, `a`, `m`, ...) and unused ones are skipped. Each payload is parsed once on the book
  thread, from the exact bytes that went into the market data file. That is what makes
  replay identical to live.
- **Minifying on the network thread.** `payload_json` has to be minified, and Binance
  payloads are only minified by convention, so the network thread runs `simdjson::minify`
  while it copies each payload into its slot. It costs about a microsecond, so I kept it on
  purpose.
- **Order book side = sorted `std::vector<Level>`, best price at the back.** The top 5
  are the last 5 elements and lookup is a binary search. An insert or erase deep in the
  book shifts the levels above it (O(n) memmove), but it is one contiguous block with no
  allocation per level, which suits the CPU cache. A `std::map` is O(log n) per update,
  but every level is a separately allocated node, so each lookup chases pointers through
  memory. A flat array indexed by price tick would be O(1), but it needs a fixed price
  band and tick size per symbol.
- **One connection for all symbols.** It is simple, and each symbol uses 3 streams, so
  50 symbols is 150 streams, well under Binance's per-connection limit. `shard_id` is in
  the format so multiple connections can be added later.

## Design choices and measurements

All numbers were measured on the dev machine (WSL2 on an Intel Core Ultra 9 285H, GCC
11.4, Release build). The benchmarks that decided a choice are kept in
[`bench/`](bench/) so they can be rerun. One-off checks are described with the command
that repeats them.

### Where the time goes

For one depth update, from Binance to a row in our CSV:

| Stage | Time | How it was measured |
|---|---|---|
| Binance batches updates (`@100ms` streams) | 0 to 100 ms | how the stream is defined |
| Network to Binance (spot, from India) | 150 to 200 ms round trip | TCP connect time, `curl -w '%{time_connect}' https://stream.binance.com:9443/` |
| Hand-off to the book thread (`wait_us`), default | p50 137 to 159 µs, p99 495 to 676 µs | `[latency]` lines, 10 and 5 minute live runs, spot and USD-M |
| Hand-off to the book thread, `--busy-poll` | p50 0 to 3 µs, p99 157 to 394 µs | same runs, at the same time |
| Book thread work per message (`work_us`) | p50 under 1 µs; p99 30 to 34 µs spot, 98 to 130 µs USD-M | same runs |

Notes on these numbers:

- Most messages are trades, which take well under a microsecond, so `work_us` p50 is 0
  or 1. The p99 is the depth diffs. On USD-M they carry about 300 levels each in a busy
  market.
- With `--busy-poll`, the p50 drops about 100×, but the p99 only drops a little. The
  spinning thread is fine, but inside WSL Windows still deschedules the VM's virtual CPUs
  now and then. On a Linux box with an isolated core that tail would mostly go away.
- The first runs measured these durations with the wall clock and showed a fake max of
  1.6 to 2.0 s, which was WSL stepping its clock. They are measured with `steady_clock`
  now.

Our own code takes microseconds, while the feed's batching and the network take tens to
hundreds of milliseconds. So making the code faster does not change the end-to-end number
much. What it does change is the tail (p99, max) and the headroom for 50 symbols.

### Choices

| Choice | Picked | Rejected | Why, with the numbers |
|---|---|---|---|
| Waiting for messages | sleep when idle (default), `--busy-poll` option | condition variable | `wait_bench`: sleep p50 225 µs at 5.7% of a core; spin p50 0.9 µs at 100% of a core; condition variable p50 246 µs at 1.4% (waking a thread is slow inside the WSL VM). Spinning is the only way to get to ~1 µs, so it is there as an option. It is not the default because it burns a core. |
| Clock for latency | `steady_clock` (monotonic) | `system_clock` | The first 10-minute run measured with the wall clock and showed a fake max of 1.6 to 2.0 s. WSL steps its clock when it re-syncs with Windows. Durations need a clock that never jumps. |
| Prices and sizes | `int64` × 10^8 | `double` | The `depth5` check compares levels for exact equality, and decimals like 0.1 are not exact in binary floating point. |
| JSON parser | simdjson On-Demand | | In the profile, simdjson's own structure scan is about 3% of replay instructions, so parsing is not where the time goes. |
| Compiler flags | `-O3` (CMake Release) | `-march=native` | Wall time of the whole replay process, including start-up, went from 15.8 to 14.9 ms (spot) and 27.1 to 27.3 ms (USD-M): noise. (The ~11 ms in [Performance](#performance) is the time replay prints for itself, without start-up.) It would also tie the binary to the CPU it was built on. |
| Includes | the specific standard headers | `<bits/stdc++.h>` | Same speed when running, since headers only declare things. A one-line file takes 0.54 s to compile instead of 0.04 s (130k lines pulled in instead of 14k), and `processor.cpp` goes from 2.4 to 3.3 s. It is also GCC-only. |
| Seeding the book | first `depth5` by default, REST `/depth` with `--rest-snapshot` | | Both keep replay exact (see [REST snapshot](#rest-snapshot---rest-snapshot)). Same 5 minutes of spot: 15 repairs with `depth5` seeding, 0 with the REST snapshot. REST stays optional because it adds an HTTPS dependency and request weight, and `depth5` seeding already self-repairs. Without REST: The cost, measured over 10 minutes of spot: 48 repairs in 6,296 checks (0.8%), 14 in the first minute and 22 during one fast move in minutes 5 and 6, while the deep levels were still unknown. |
| REST requests at once | 4 (`--rest-parallel`, 1 to 8) | 1 (one after another), 8 | `bench/rest_parallel_bench.sh`, live spot, 20 symbols, time until the last snapshot: 6.703 to 7.226 s one at a time, 1.750 to 1.849 s with 4 (about 3.9× faster), 1.019 to 1.105 s with 8. Going from 4 to 8 only saves ~0.75 s, but doubles the TLS handshakes running on the io thread that also reads the WebSocket, and the requests in flight when a 429 arrives. Request weight per round is the same for every N. See [REST snapshot](#rest-snapshot---rest-snapshot). |

### Profile

`valgrind --tool=callgrind` on a replay of the USD-M sample, as a share of all
instructions:

| Where | Share |
|---|---|
| `BookSide::set`, almost all of it `memmove` from vector insert/erase | 43% |
| splitting CSV rows (replay only, live mode never does this) | 24% |
| turning JSON price levels into integers | 16% |
| formatting order book rows | 7% |
| simdjson structure scan | 3% |

The biggest item that live mode also pays for is the book's memmove, and live it is
still only tens of microseconds at p99 per depth diff (see the table above), well under
the feed's 100 ms batching.

### Running the benchmarks

```bash
cmake --build build --target wait_bench
./build/wait_bench
```

`wait_bench`, 500 messages 10 ms apart:

```
sleep    p50    225.1 us   p99    659.8 us   max    743.6 us   cpu   5.7% of one core
spin     p50      0.9 us   p99    158.2 us   max    423.7 us   cpu 106.6% of one core
condvar  p50    245.8 us   p99    497.8 us   max    832.5 us   cpu   1.4% of one core
```

```bash
bench/rest_parallel_bench.sh [path/to/binance_capture]   # default ./build/binance_capture
```

`rest_parallel_bench.sh` runs live against Binance for 40 s: four copies of the program at
the same time (spot, 20 symbols, `--rest-snapshot`, `--rest-parallel` 1, 2, 4 and 8). It
prints one line per N with the snapshots that arrived, the time from the first market data
row to the last snapshot, and the REST errors in the log. One run of the script costs
~4,000 request weight of the 6,000 a minute per IP, so wait at least 70 s before running it
again. The results are in [REST snapshot](#rest-snapshot---rest-snapshot).

## Metrics

Printed to stderr every 10 s and at exit:

`messages, md_rows, ob_rows, parse_errors, unknown_streams, reconnects, diffs_applied,
diffs_stale, diffs_waiting, gaps, book_resets, depth5_applied, depth5_stale, depth5_checked,
depth5_mismatch, depth5_folded, trades, rest_snapshots, rest_applied, rest_dropped,
rest_top_mismatch, queue_full_waits, write_errors`

In live mode the book thread also prints latency every 10 s and at exit:

```
[final latency] n=29194 wait_us p50=2 p99=249 p99.9=744 max=14810 | work_us p50=0 p99=98 p99.9=243 max=6824
```

(from a 5-minute USD-M run with `--busy-poll`)

| Field | Meaning |
|---|---|
| `wait_us` | from reading the frame off the socket to the book thread starting on it: the hand-off through the queue, including any time the book thread was asleep |
| `work_us` | time inside `Processor::process()`: parse, sync rules, book update, CSV row into the buffer (and the occasional 256 KB write to disk) |

Both are counted in 1 µs buckets (fixed memory, so it can run for days) and reported as
percentiles. Binance → us latency is not reported, because the local clock is not
trustworthy enough for that (see [Timestamps](#timestamps)).

## What I would do in production

These matter far more than anything left inside the code, but they are outside the scope
of a take-home:

- **Run next to Binance.** Binance's servers are widely reported to be in AWS Tokyo
  (ap-northeast-1). Running the capture there would take the network from tens or
  hundreds of milliseconds down to about a millisecond. Nothing in the code comes close.
- **Use Binance's binary market data streams.** Binance offers SBE (binary) market data
  streams for spot. They have a fixed layout, so there is no JSON to parse. They need an
  API key, which this project avoids on purpose.
- **Pin the threads.** Put the network thread and the book thread on their own isolated
  cores (`isolcpus`, `taskset`) and run with `--busy-poll`, so the spinning thread never
  gets descheduled.
- **Price-indexed book.** An array indexed by price tick, around the current price, gives
  O(1) updates and an O(1) top of book. It needs each symbol's tick size (from
  `exchangeInfo`) and has to be re-centred when the price moves far.
- **Writing files off the book thread.** Every 256 KB the book thread does one `fwrite`,
  which shows up in the `work_us` max. A separate writer thread, or `io_uring`, would take
  that off the book's path.
- **A disciplined clock.** With chrony/PTP (or NIC hardware timestamps), `recv - E` would
  become a real Binance → us latency number.
- **Several connections.** For 50 busy symbols, spread them over a few connections
  (`shard_id` is already in the format) so one slow socket does not delay everything.

## Limits and safety

- 1 to 50 symbols, no duplicates. The input is uppercased first, then each symbol must be
  letters and digits only. The stream URL is capped at 4096 characters.
- Bad options are rejected with the usage text, for example a non-numeric `--duration` or a
  `--rest-parallel` outside 1 to 8.
- A short `fwrite` or a failed `fclose` counts as a write error (`write_errors`).
- WebSocket messages are capped at 1 MB (`read_message_max`). A larger message is an error
  and triggers a reconnect, not unbounded memory use.
- No API keys or secrets are used or stored. Only public market data endpoints.
- TLS verification is never disabled.

## Sample run

Captured on 2026-11-14 with a Release build on Ubuntu 22.04 (WSL2, Intel Core Ultra 9 285H).
Spot and USD-M ran at the same time:

```bash
./build/binance_capture --venue spot --symbols BTCUSDT --output-dir ./output --duration 120
./build/binance_capture --venue usdm --symbols BTCUSDT --output-dir ./output --duration 120
```

Both ran with `--duration 120`, but the files span ~128 s of Binance time: the WSL VM clock
ran slow, so the 120 s timer took longer in real time.

The full files are in [`sample/`](sample/):

| File | Rows |
|---|---|
| `market_data_spot_BTCUSDT_2026-11-14.csv` | 5,967 (1,260 depth_diff, 1,260 depth5, 3,447 trade) |
| `market_data_spot_BTCUSDT_2026-11-14_orderbook.csv` | 4,709 (1,259 D, 3 S, 3,447 T) |
| `market_data_usdm_BTCUSDT_2026-11-14.csv` | 5,788 (1,248 depth_diff, 1,214 depth5, 3,326 trade) |
| `market_data_usdm_BTCUSDT_2026-11-14_orderbook.csv` | 5,470 (1,248 D, 896 S, 3,326 T) |

The two order book files were regenerated from the two market data files by replay, after
the change that gives every applied `depth5` its own `S` row. The spot file came out
byte-identical. On USD-M the only difference is the added `S` rows (one per folded
snapshot) and the `seqNo` shift that comes with them.

Final counters:

```
spot [final] messages=5967 md_rows=5967 ob_rows=4709 parse_errors=0 unknown_streams=0 reconnects=0
     diffs_applied=1259 diffs_stale=1 diffs_waiting=0 gaps=0 book_resets=0 depth5_applied=3
     depth5_stale=0 depth5_checked=1259 depth5_mismatch=2 depth5_folded=0 trades=3447
     queue_full_waits=0 write_errors=0
usdm [final] messages=5788 md_rows=5788 ob_rows=4575 parse_errors=0 unknown_streams=0 reconnects=0
     diffs_applied=1248 diffs_stale=0 diffs_waiting=0 gaps=0 book_resets=0 depth5_applied=896
     depth5_stale=0 depth5_checked=318 depth5_mismatch=0 depth5_folded=895 trades=3326
     queue_full_waits=0 write_errors=0
```

These counters come from the build that captured the sample. Newer builds also print the
`rest_*` counters (all 0 without `--rest-snapshot`). The `S`-row change does not change any
counter apart from `ob_rows`, which is 5470 for USD-M now, matching the file above.

- **Spot:** each of the 1,259 applied diffs was matched by a `depth5` with the same update
  id, and the top 5 levels were compared exactly. 2 differed, right after start (see
  [above](#order-book-sequencing-and-reconnects)), and were repaired. 0 differed after that.
- **USD-M:** 318 exact comparisons with 0 mismatches, and 895 snapshots folded in (each
  with its own `S` row).
- **Both:** no gaps, resets, reconnects or parse errors. The one stale diff on spot is the
  diff that arrived right after the seeding snapshot with the same id, as expected.

First rows of the spot files (payload truncated here):

```
recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json
1794656854,780302145,spot,depth5,0,0,0,BTCUSDT,"{""lastUpdateId"":102819774248,""bids"":[[""67223.08000000"",""0.89361000""],...
1794656854,780359884,spot,depth_diff,0,0,1,BTCUSDT,"{""e"":""depthUpdate"",""E"":1794656856514,""s"":""BTCUSDT"",""U"":102819774245,""u"":102819774248,...
1794656854,781666127,spot,trade,0,0,2,BTCUSDT,"{""e"":""trade"",""E"":1794656856534,""s"":""BTCUSDT"",""t"":6853806266,""p"":""67223.08000000"",""q"":""0.00010000"",""T"":1794656856533,""m"":true,""M"":true}"
```

```
tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4
1794656854,780302145,1,1747767916,S,N,6722308000000,6722307000000,6722264000000,6722263000000,6722256000000,89361000,56000,16000,1185000,8000,6722309000000,6722310000000,6722399000000,6722400000000,6722401000000,192282000,40000,16000,43736000,15786000
1794656854,781666127,2,1747767916,T,S,6722308000000,6722307000000,6722264000000,6722263000000,6722256000000,89361000,56000,16000,1185000,8000,6722309000000,6722310000000,6722399000000,6722400000000,6722401000000,192282000,40000,16000,43736000,15786000
1794656854,863581984,3,1747767916,D,N,6722308000000,6722307000000,6722264000000,6722263000000,6722256000000,89351000,56000,16000,1185000,8000,6722309000000,6722310000000,6722399000000,6722400000000,6722401000000,203562000,40000,16000,43736000,15786000
```

`6722308000000` is 67,223.08 USDT at a scale of 10^8, and `89361000` is 0.89361 BTC.

### Replay (no network)

The replay reproduces both order book files byte for byte:

```bash
./build/binance_capture \
  --replay sample/market_data_spot_BTCUSDT_2026-11-14.csv,sample/market_data_usdm_BTCUSDT_2026-11-14.csv \
  --output-dir replay_output
cmp sample/market_data_spot_BTCUSDT_2026-11-14_orderbook.csv replay_output/market_data_spot_BTCUSDT_2026-11-14_orderbook.csv
cmp sample/market_data_usdm_BTCUSDT_2026-11-14_orderbook.csv replay_output/market_data_usdm_BTCUSDT_2026-11-14_orderbook.csv
# no output from cmp = identical
```

The spot order book file in `sample/` is byte-identical to the live one. The USD-M one was
itself written by replay (see [above](#sample-run)), so for USD-M this shows that replay
gives the same file every time. The live-vs-replay checks are in
[Checks I ran](#checks-i-ran).

### Performance

- Replaying the spot sample (5,967 messages) takes about 11 ms on one thread: CSV read,
  JSON parse, book update and CSV write. That is roughly 540k messages/s, or about 1.8 µs
  per message, with a peak RSS of 6.4 MB. The USD-M sample takes about 22 ms, because its
  diffs carry more levels. These are the times replay prints for itself (the
  `[replay] ... in X s` line), without process start-up. The `-march=native` row in
  [Choices](#choices) measured the wall time of the whole process instead, which is why its
  numbers are higher.
- Live, the rate was 50 to 200 messages/s per symbol (more in a busy market, mostly
  trades), so the book thread is idle more than 99% of the time. The limit is the feed,
  not the CPU. `queue_full_waits` stayed 0 in every run.
- Live latency inside the program is in [Where the time goes](#where-the-time-goes).
- The steady state does no heap allocation per message (reused slots, buffers, vectors and
  parsers, see [Threading and I/O](#threading-and-io)).

### Checks I ran

| Check | Result |
|---|---|
| Release build, `-Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wno-sign-conversion` | no warnings |
| Unit tests (decimal parser, CSV escaping, book side incl. `replace_top` on both sides, latency percentiles, spot and USD-M sync, gaps, resets, depth5 check/repair/fold, REST snapshot behind/ahead/too old/after reconnect/after a gap/USD-M, replay equals live) | pass |
| `--rest-snapshot`: ASan + UBSan and TSan builds, unit tests and 20 s live spot and USD-M captures of BTCUSDT and ETHUSDT | clean, no races |
| `--rest-snapshot` live A/B, 5 minutes, spot and USD-M (table in [REST snapshot](#rest-snapshot---rest-snapshot)) | spot repairs 15 → 0; replays byte-identical |
| Without `--rest-snapshot`: replay of both samples vs the files from the build without REST support (done before the `S`-row change) | byte-identical |
| Full-depth accuracy: `verify_against_rest` on a 10-minute live capture of BTCUSDT, spot and USD-M, against 103 REST `/depth` snapshots saved every 10 s by another process (see [Full-depth accuracy](#full-depth-accuracy-against-binance)) | top 100 levels exact in all 103; exact within every price range the book was told about; a deliberately edited snapshot was caught |
| Independent review of the REST code (sync rules, and threads/lifetime/shutdown) | 7 findings, all fixed or documented above |
| ASan + UBSan build: unit tests, replay of both samples, 20 s live USD-M capture of BTCUSDT and ETHUSDT with `--busy-poll` | clean, no leaks |
| TSan build: unit tests, 20 s live spot capture of BTCUSDT and ETHUSDT, once default and once with `--busy-poll` (network thread and book thread both busy) | no data races |
| SIGINT during a live capture | clean stop; every received message written; files end on a complete line |
| `--duration` 10, 20 and 30 s, which line up with the 10 s stats timer (this hung before the `stopping` fix, found by the ASan run) | each run stops on time (10, 23 and 32 s, including the WebSocket close) and prints its final counters |
| Replay vs live capture (spot and USD-M), also from a fresh clone | byte-identical |
| 10-minute live runs, spot and USD-M at the same time, each with and without `--busy-poll` (123k and 98k messages) | 0 gaps, 0 reconnects, 0 parse errors; USD-M 0 `depth5` mismatches, spot 48 repaired |
| Replay of the four 10-minute live captures (up to 123k messages each) | byte-identical order book files |

On recent kernels, GCC 11's TSan needs ASLR off to start ("unexpected memory mapping"),
so run it as `setarch $(uname -m) -R ./build-tsan/binance_capture ...`.

The book reset on a new `conn_epoch` is unit tested. `WsClient`'s backoff and the
`conn_epoch` increment are not, and no disconnect happened in the live runs.

## How to Submit on GitHub

### 1. Initialize and push your repository

```bash
git init
git add .
git commit -m "Initial submission: Binance WebSocket capture + LOB"
git remote add origin https://github.com/<your-username>/binance-lob-capture.git
git branch -M main
git push -u origin main
```

### 2. `.gitignore` excludes build artifacts and secrets

`build/`, `output/`, `replay_output/` and `*.csv` are ignored, except the sample files in
`sample/` (about 9 MB in total). No credentials are needed.

### 3. Tag your submission

```bash
git tag -a v1.0.0 -m "Submission v1.0.0"
git push origin v1.0.0
```

## Reviewer checklist

| Step | Command |
|---|---|
| Build from clean | `cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build` |
| Unit tests | `(cd build && ctest --output-on-failure)` or `./build/tests/unit_tests` |
| Live 300 s capture | `./build/binance_capture --venue spot --symbols BTCUSDT --duration 300` (writes to `./output`) |
| Market data file starts right | `head -2 output/market_data_spot_BTCUSDT_<date>.csv` shows the header and the first row |
| Order book file starts right | `head -2 output/market_data_spot_BTCUSDT_<date>_orderbook.csv` shows the exact header and the first row |
| Column count check | `awk -F',' 'NF!=26{n++} END{print n+0}' output/market_data_spot_BTCUSDT_<date>_orderbook.csv` gives `0` (every row has 26 columns) |
| Replay matches live | `./build/binance_capture --replay output/market_data_spot_BTCUSDT_<date>.csv --output-dir replay_output && cmp output/market_data_spot_BTCUSDT_<date>_orderbook.csv replay_output/market_data_spot_BTCUSDT_<date>_orderbook.csv` (no output = identical) |
| No secrets in repo | `git grep -i -e api_key -e secret -e password` only matches the `config/secrets.json` ignore rule and this README |

`<date>` is the UTC date the run started, e.g. `2026-11-14`. If `output/` already had a spot
BTCUSDT capture from that day, the new files are `..._<date>_2.csv` and
`..._<date>_2_orderbook.csv` (see [Output files](#output-files)).
