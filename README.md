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

Warnings are on for our code: `-Wall -Wextra -Wpedantic -Wshadow -Wconversion`. The build
is warning-free. The default build type is `Release` (`-O3 -DNDEBUG`).

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

# replay: rebuild the order book CSV from a market data CSV, no network
./build/binance_capture --replay ./output/market_data_spot_BTCUSDT_2026-11-14.csv --output-dir ./replay_output

# unit tests
ctest --test-dir build --output-on-failure      # or ./build/unit_tests
```

| Option | Meaning |
|---|---|
| `--venue spot\|usdm` | `spot` = `wss://stream.binance.com:9443/stream`, `usdm` = `wss://fstream.binance.com/public/stream` |
| `--symbols` | comma separated, case-insensitive, e.g. `BTCUSDT,ETHUSDT` (1 to 50 symbols) |
| `--output-dir` | where CSVs go (default `./output`, created if missing) |
| `--duration` | seconds to run, `0` or omitted = until SIGINT/SIGTERM |
| `--busy-poll` | the book thread never sleeps while waiting for messages: about 1 µs wait instead of ~150 µs, at the cost of one core at 100% (see [Design choices](#design-choices-and-measurements)) |
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
| `src/processor.h/.cpp` | book thread: JSON → sequence checks → book → CSV rows (live and replay both use it) |
| `src/replay.h/.cpp` | reads a market data CSV back into `RawMessage`s for the processor |
| `src/main.cpp` | options, threads, signals, periodic stats |
| `tests/unit_tests.cpp` | unit tests (`ctest`) |
| `bench/wait_bench.cpp` | the benchmark behind the `--busy-poll` choice, see [Design choices](#design-choices-and-measurements) |

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
| `stream_kind` | `depth_diff`, `depth5` or `trade` |
| `shard_id` | always `0` (one connection) |
| `conn_epoch` | starts at 0, +1 on every reconnect attempt |
| `conn_seq` | 0, 1, 2, ... per connection, across all symbols on it, so it is monotonic within `(shard_id, conn_epoch)` |
| `symbol` | uppercase |
| `payload_json` | the inner `data` object, minified, CSV-escaped |

There is one row for every message read from the socket, in the order it was read. This is
also the order the order book thread processes them in.

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
| `S` | a `depth5` snapshot seeded the book (at start or after a reset) or repaired it after a mismatch | `N` |
| `T` | a trade printed. The book is **not** changed, the row only marks the trade | aggressor: `m=true` (buyer is maker) gives `S`, `m=false` gives `B` |
| `R` | the book was reset (sequence gap or new `conn_epoch`), all levels are 0 | `N` |

## Scaling

Binance sends prices and quantities as decimal strings. Both are converted to `int64` with a
fixed scale of **10^8** (8 decimal places), straight from the string with integer math:
`"65000.12000000"` becomes `6500012000000`. No `float`/`double` is used anywhere.

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

Receive time is only as good as the local clock. In the sample run (WSL2), `recv` was about
1.7 s *earlier* than Binance's `E`, which means the VM clock was behind. That is also why I
don't report latency as `recv - E`. On a real box the clock would be disciplined with
chrony/PTP.

## Order book, sequencing and reconnects

Every message goes through one function, `Processor::process()`, in file order.

**Diff (`depthUpdate`).** Each level carries the new absolute quantity at that price, and
`0` removes the level. Continuity uses Binance's update ids:

- spot: the previous id is `U - 1`
- usdm: the previous id is `pu`

With `last` = the update id the book is at:

| Condition | Action |
|---|---|
| book not valid yet | drop (counted as `diffs_waiting`), no row |
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
  diffs are cut about 25 ms apart, so their ids never line up. An exact comparison is
  impossible, so the snapshot is **folded in** just before the diff is applied
  (`depth5_folded`). This is safe: the diff carries every change from `last` up to its
  `u`, so any level the snapshot made stale is overwritten, and the book ends exact at `u`.
  Folding also fills in any top levels we were missing.

I found the spot/USD-M difference by looking at the captured ids. On spot, the `depth5`
`lastUpdateId` equals a diff's `u`. On USD-M, the `depth5` `u` always falls inside the
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

**Why no REST snapshot.** The optional stretch goal (REST `/depth` snapshot plus buffered
diffs) would give a full-depth book from the first second. I left it out on purpose. The
REST response is not one of the three streams, so it would not be in the market data CSV,
and replay could no longer rebuild the same book from that file alone. With `depth5`
seeding, the audit file stays the single source of truth.

The cost is that right after a seed, the book only knows the top 5 levels plus whatever
diffs touch afterwards. If the price moves through the seeded levels before the deeper
ones have been touched, a level can be missing. The `depth5` check catches exactly this
and repairs it. In the sample spot run, the only 2 mismatches happened 2 s and 6 s after
start, followed by 0 in the remaining ~115 s. Over a 10-minute spot run there were 48
repairs in 6,296 checks (0.8%). Most were bunched at the start (14 in the first minute)
and during one fast move (22 in minutes 5 and 6). The other minutes had 0 to 4.

**How I would add the REST snapshot without breaking replay:**

1. After each connect, the network thread fetches `GET /api/v3/depth?symbol=X&limit=1000`
   (spot) or `/fapi/v1/depth` (USD-M) over HTTPS, without blocking the socket read.
2. It pushes the response into the same queue as a new `stream_kind`, `rest_snapshot`, so
   it lands in the market data CSV in the order it arrived. Replay then sees exactly what
   live saw.
3. Until the snapshot arrives, the book thread buffers diffs instead of dropping them.
   When it arrives: drop buffered diffs with `u <= lastUpdateId`, check that the first one
   left spans `lastUpdateId + 1` (spot) or `lastUpdateId` (USD-M), then apply the rest.
4. After a gap the book thread needs a new snapshot, so it has to signal the network
   thread (an atomic flag per symbol that a timer checks).
5. Request weight: `limit=1000` costs 50 on spot against a budget of 6,000 per minute,
   so 50 symbols fit, but a reconnect storm would have to be spread out.

`depth5` would still do the checking and repairing.

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

- **Two threads, one lock-free single-producer/single-consumer ring.** The network thread
  never touches the disk, so a slow write can never stall the socket read. The ring uses one
  atomic head and one atomic tail with acquire/release ordering, on separate cache lines.
  Each side keeps a cached copy of the other side's index, so it rarely touches the shared
  line. There are no mutexes or condition variables on the per-message path.
- **No per-message allocation in steady state.** Slots are reused in place, and the
  `std::string` in each slot keeps its capacity. The book thread reuses its row buffer, the
  level vectors and one simdjson parser. The network thread reuses its frame buffer and
  parser.
- **Back-pressure.** If the ring is full, the network thread waits (counted as
  `queue_full_waits`) instead of dropping messages, because a dropped message would make
  the audit file incomplete. Waiting stops reading the socket, so TCP flow control pushes
  back on Binance. At 4096 slots and ~30 messages/s per symbol the ring holds minutes of
  data, so in practice this never triggers.
- **Buffered writes.** Each file collects rows in a 256 KB in-memory buffer and calls
  `fwrite` once per buffer (stdio buffering is turned off, so data is not copied twice).
  The book thread also flushes every second when idle, so files on disk stay current
  during a live run. An idle book thread yields, then sleeps 200 µs, so it does not burn a
  core when the market is quiet. `--busy-poll` makes it spin instead.
- **Shutdown.** SIGINT/SIGTERM (or `--duration`) is handled with `asio::signal_set`, so the
  handler runs on the io thread, not in signal context. It stops the timers, sends a
  WebSocket close (forcing the socket closed after 2 s at most), and `io_context::run()`
  returns. The book thread drains whatever is left in the ring, then files are flushed and
  closed and final counters are printed. Every timer handler also checks a `stopping`
  flag, because a timer that has already expired can't be cancelled (its handler still
  runs, with no error). Without the flag, a stats timer firing in the same instant as the
  `--duration` timer re-armed itself and kept `run()` alive forever. After the first
  signal, SIGINT/SIGTERM get their default action back, so a second Ctrl+C always kills
  the process. All resources are RAII (`BufferedFile` owns its
  `FILE*`, the socket lives in a `std::optional` recreated per connection). Async handlers
  capture `this` plus the connection epoch, and the client outlives `run()`. A late handler
  from an old connection sees a different epoch and returns without touching anything.

### Why these choices

- **Beast + Asio + OpenSSL.** These are the de-facto standard for WebSocket over TLS in
  C++. TLS peer verification and hostname verification are on, and SNI is set (Binance
  needs it).
- **simdjson On-Demand.** It is the fastest mainstream C++ JSON parser. It finds the
  structure with SIMD in one pass, then we only touch the fields we use (`U`, `u`, `pu`,
  `b`, `a`, `m`, ...) and unused ones are skipped. Each payload is parsed once on the book
  thread, from the exact bytes that went into the market data file. That is what makes
  replay identical to live.
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
11.4, Release build). The benchmark that decided a choice is kept in
[`bench/`](bench/) so it can be rerun. One-off checks are described with the command
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
| Compiler flags | `-O3` (CMake Release) | `-march=native` | Replay went from 15.8 to 14.9 ms (spot) and 27.1 to 27.3 ms (USD-M): noise. It would also tie the binary to the CPU it was built on. |
| Includes | the specific standard headers | `<bits/stdc++.h>` | Same speed when running, since headers only declare things. A one-line file takes 0.54 s to compile instead of 0.04 s (130k lines pulled in instead of 14k), and `processor.cpp` goes from 2.4 to 3.3 s. It is also GCC-only. |
| Seeding the book | first `depth5` | REST `/depth` snapshot | Keeps replay exact (see [Why no REST snapshot](#order-book-sequencing-and-reconnects)). The cost, measured over 10 minutes of spot: 48 repairs in 6,296 checks (0.8%), 14 in the first minute and 22 during one fast move in minutes 5 and 6, while the deep levels were still unknown. |

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

### Running the benchmark

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

## Metrics

Printed to stderr every 10 s and at exit:

`messages, md_rows, ob_rows, parse_errors, unknown_streams, reconnects, diffs_applied,
diffs_stale, diffs_waiting, gaps, book_resets, depth5_applied, depth5_stale, depth5_checked,
depth5_mismatch, depth5_folded, trades, queue_full_waits, write_errors`

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

- 1 to 50 symbols, uppercase letters and digits only, no duplicates. The stream URL is
  capped at 4096 characters.
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

The full files are in [`sample/`](sample/):

| File | Rows |
|---|---|
| `market_data_spot_BTCUSDT_2026-11-14.csv` | 5,967 (1,260 depth_diff, 1,260 depth5, 3,447 trade) |
| `market_data_spot_BTCUSDT_2026-11-14_orderbook.csv` | 4,709 (1,259 D, 3 S, 3,447 T) |
| `market_data_usdm_BTCUSDT_2026-11-14.csv` | 5,788 (1,248 depth_diff, 1,214 depth5, 3,326 trade) |
| `market_data_usdm_BTCUSDT_2026-11-14_orderbook.csv` | 4,575 (1,248 D, 1 S, 3,326 T) |

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

- **Spot:** each of the 1,259 applied diffs was matched by a `depth5` with the same update
  id, and the top 5 levels were compared exactly. 2 differed, right after start (see
  [above](#order-book-sequencing-and-reconnects)), and were repaired. 0 differed after that.
- **USD-M:** 318 exact comparisons with 0 mismatches, and 895 snapshots folded in.
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

### Performance

- Replaying the spot sample (5,967 messages) takes about 11 ms end to end on one thread:
  CSV read, JSON parse, book update and CSV write. That is roughly 540k messages/s, or
  about 1.8 µs per message, with a peak RSS of 6.4 MB. The USD-M sample takes about 22 ms,
  because its diffs carry more levels.
- Live, the rate was 50 to 200 messages/s per symbol (more in a busy market, mostly
  trades), so the book thread is idle more than 99% of the time. The limit is the feed,
  not the CPU. `queue_full_waits` stayed 0 in every run.
- Live latency inside the program is in [Where the time goes](#where-the-time-goes).
- The steady state does no heap allocation per message (reused slots, buffers, vectors and
  parsers, see [Threading and I/O](#threading-and-io)).

### Checks I ran

| Check | Result |
|---|---|
| Release build, `-Wall -Wextra -Wpedantic -Wshadow -Wconversion` | no warnings |
| Unit tests (decimal parser, CSV escaping, book side incl. `replace_top` on both sides, latency percentiles, spot and USD-M sync, gaps, resets, depth5 check/repair/fold, replay equals live) | pass |
| ASan + UBSan build: unit tests, replay of both samples, 20 s live USD-M capture of BTCUSDT and ETHUSDT with `--busy-poll` | clean, no leaks |
| TSan build: unit tests, 20 s live spot capture of BTCUSDT and ETHUSDT, once default and once with `--busy-poll` (network thread and book thread both busy) | no data races |
| SIGINT during a live capture | clean stop; every received message written; files end on a complete line |
| `--duration` 10, 20 and 30 s, which line up with the 10 s stats timer (this hung before the `stopping` fix, found by the ASan run) | each run stops on time (10, 23 and 32 s, including the WebSocket close) and prints its final counters |
| Replay vs live capture (spot and USD-M), also from a fresh clone | byte-identical |
| 10-minute live runs, spot and USD-M at the same time, each with and without `--busy-poll` (123k and 98k messages) | 0 gaps, 0 reconnects, 0 parse errors; USD-M 0 `depth5` mismatches, spot 48 repaired |
| Replay of the four 10-minute live captures (up to 123k messages each) | byte-identical order book files |

On recent kernels, GCC 11's TSan needs ASLR off to start ("unexpected memory mapping"),
so run it as `setarch $(uname -m) -R ./build-tsan/binance_capture ...`.

The reconnect path (backoff, `conn_epoch` increment, book reset on a new epoch) is covered
by unit tests of the epoch logic. No disconnect happened during the sample runs.

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

`build/`, `output/`, `replay_output/` and `*.csv` are ignored, except the small sample files
in `sample/`. No credentials are needed.

### 3. Tag your submission

```bash
git tag -a v1.0.0 -m "Submission v1.0.0"
git push origin v1.0.0
```

## Reviewer checklist

| Step | Command |
|---|---|
| Build from clean | `cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build` |
| Unit tests | `ctest --test-dir build --output-on-failure` |
| Live 300 s capture | `./build/binance_capture --venue spot --symbols BTCUSDT --duration 300` |
| Column count check | `awk -F',' 'NR==2{print NF}' output/*_orderbook.csv` gives `26` |
| Replay matches live | `./build/binance_capture --replay output/market_data_spot_BTCUSDT_<date>.csv --output-dir replay_output && cmp output/*_orderbook.csv replay_output/*_orderbook.csv` |
| No secrets in repo | `git grep -i "api_key\|secret\|password"` only matches the `config/secrets.json` ignore rule and this README |
