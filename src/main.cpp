#include <atomic>
#include <cctype>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <filesystem>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/signal_set.hpp>

#include "latency.h"
#include "metrics.h"
#include "processor.h"
#include "replay.h"
#include "ws_client.h"

namespace fs = std::filesystem;
namespace net = boost::asio;
namespace ssl = net::ssl;

namespace {

constexpr std::size_t kMaxSymbols = 50;
constexpr std::size_t kMaxTargetLength = 4096;

struct Options {
    Venue venue = Venue::Spot;
    std::vector<std::string> symbols;
    std::string output_dir = "./output";
    long duration_sec = 0;
    bool busy_poll = false;
    std::vector<std::string> replay_files;
};

void usage() {
    std::fprintf(stderr,
                 "usage:\n"
                 "  binance_capture --venue spot|usdm --symbols BTCUSDT[,ETHUSDT...] [--output-dir DIR] "
                 "[--duration SECONDS] [--busy-poll]\n"
                 "  binance_capture --replay FILE[,FILE...] [--output-dir DIR]\n");
}

std::vector<std::string> split(const std::string& s, char sep) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : s) {
        if (c == sep) {
            if (!cur.empty()) out.push_back(cur);
            cur.clear();
        } else if (c != ' ') {
            cur += c;
        }
    }
    if (!cur.empty()) out.push_back(cur);
    return out;
}

bool valid_symbol(const std::string& s) {
    if (s.size() < 2 || s.size() > 20) return false;
    for (char c : s) {
        if (!((c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'))) return false;
    }
    return true;
}

bool parse_args(int argc, char** argv, Options& opt) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() -> const char* { return i + 1 < argc ? argv[++i] : nullptr; };
        if (arg == "--venue") {
            const char* v = next();
            if (!v || !parse_venue(v, opt.venue)) return false;
        } else if (arg == "--symbols") {
            const char* v = next();
            if (!v) return false;
            for (auto& sym : split(v, ',')) {
                for (char& c : sym) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
                opt.symbols.push_back(sym);
            }
        } else if (arg == "--output-dir") {
            const char* v = next();
            if (!v) return false;
            opt.output_dir = v;
        } else if (arg == "--duration") {
            const char* v = next();
            if (!v) return false;
            opt.duration_sec = std::strtol(v, nullptr, 10);
            if (opt.duration_sec < 0) return false;
        } else if (arg == "--busy-poll") {
            opt.busy_poll = true;
        } else if (arg == "--replay") {
            const char* v = next();
            if (!v) return false;
            for (auto& f : split(v, ',')) opt.replay_files.push_back(f);
        } else {
            return false;
        }
    }

    if (!opt.replay_files.empty()) return true;
    if (opt.symbols.empty() || opt.symbols.size() > kMaxSymbols) {
        std::fprintf(stderr, "need between 1 and %zu symbols\n", kMaxSymbols);
        return false;
    }
    std::set<std::string> seen;
    for (const auto& s : opt.symbols) {
        if (!valid_symbol(s) || !seen.insert(s).second) {
            std::fprintf(stderr, "bad or duplicate symbol: %s\n", s.c_str());
            return false;
        }
    }
    if (make_endpoint(opt.venue, opt.symbols).target.size() > kMaxTargetLength) {
        std::fprintf(stderr, "stream URL too long, use fewer symbols\n");
        return false;
    }
    return true;
}

std::string utc_date() {
    std::time_t t = std::time(nullptr);
    std::tm tm{};
    gmtime_r(&t, &tm);
    char buf[16];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d", &tm);
    return buf;
}

// Never overwrite an earlier capture: add _2, _3, ... if the name is taken.
std::string free_base_name(const fs::path& dir, const std::string& base) {
    std::string name = base;
    for (int n = 2; fs::exists(dir / (name + ".csv")) || fs::exists(dir / (name + "_orderbook.csv")); ++n)
        name = base + "_" + std::to_string(n);
    return (dir / name).string();
}

// The book thread. By default it yields a few times and then sleeps 200 us
// when the queue is empty, which keeps CPU near zero but means a message can
// wait up to a few hundred us before it is picked up. --busy-poll keeps
// checking instead: wait drops to about 1 us, but one core runs at 100%.
void run_consumer(MessageQueue& queue, Processor& processor, const std::atomic<bool>& producer_done,
                  bool busy_poll, LatencyStats& latency) {
    auto last_flush = std::chrono::steady_clock::now();
    auto last_report = last_flush;
    int idle = 0;
    while (true) {
        RawMessage* msg = queue.front();
        if (msg) {
            std::int64_t start = mono_clock_ns();
            processor.process(*msg);
            std::int64_t end = mono_clock_ns();
            latency.wait.add(start - msg->recv_mono_ns);
            latency.work.add(end - start);
            queue.pop();
            idle = 0;
            continue;
        }
        if (producer_done.load(std::memory_order_acquire)) {
            if (!queue.front()) break;
            continue;
        }
        auto now = std::chrono::steady_clock::now();
        if (now - last_flush > std::chrono::seconds(1)) {
            processor.flush();
            last_flush = now;
        }
        if (now - last_report > std::chrono::seconds(10)) {
            latency.print(stderr, "[latency]");
            last_report = now;
        }
        if (busy_poll) continue;
        if (++idle < 64)
            std::this_thread::yield();
        else
            std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
}

int run_live(const Options& opt) {
    std::error_code ec;
    fs::create_directories(opt.output_dir, ec);
    if (ec) {
        std::fprintf(stderr, "cannot create %s: %s\n", opt.output_dir.c_str(), ec.message().c_str());
        return 1;
    }

    Metrics metrics;
    Processor processor(opt.venue, metrics);
    std::string date = utc_date();
    for (const auto& sym : opt.symbols) {
        std::string base =
            free_base_name(opt.output_dir, std::string("market_data_") + venue_name(opt.venue) + "_" + sym + "_" + date);
        if (!processor.add_symbol(SymbolOutput{sym, base + ".csv", base + "_orderbook.csv"})) {
            std::fprintf(stderr, "cannot open output files for %s\n", sym.c_str());
            return 1;
        }
        std::fprintf(stderr, "[main] %s -> %s.csv, %s_orderbook.csv\n", sym.c_str(), base.c_str(), base.c_str());
    }

    auto queue = std::make_unique<MessageQueue>();
    std::atomic<bool> producer_done{false};
    LatencyStats latency;
    std::thread consumer([&] { run_consumer(*queue, processor, producer_done, opt.busy_poll, latency); });

    int rc = 0;
    try {
        net::io_context ioc;
        ssl::context ssl_ctx(ssl::context::tls_client);
        ssl_ctx.set_default_verify_paths();
        ssl_ctx.set_verify_mode(ssl::verify_peer);

        WsClient client(ioc, ssl_ctx, opt.venue, opt.symbols, *queue, metrics);
        net::signal_set signals(ioc, SIGINT, SIGTERM);
        net::steady_timer duration_timer(ioc);
        net::steady_timer stats_timer(ioc);

        // A timer that has already expired can't be cancelled any more: its
        // handler still runs, with no error. So every handler also checks
        // `stopping`, otherwise the stats timer could re-arm itself after
        // shutdown and run() would never return.
        bool stopping = false;
        auto shutdown = [&](const char* why) {
            if (stopping) return;
            stopping = true;
            std::fprintf(stderr, "[main] stopping: %s\n", why);
            client.stop();
            // cancel() alone keeps SIGINT/SIGTERM registered, so a second Ctrl+C
            // would be swallowed. clear() gives them back their default action,
            // which kills the process if the clean stop ever gets stuck.
            signals.cancel();
            signals.clear();
            duration_timer.cancel();
            stats_timer.cancel();
        };

        signals.async_wait([&](const boost::system::error_code& e, int) {
            if (!e) shutdown("signal");
        });
        if (opt.duration_sec > 0) {
            duration_timer.expires_after(std::chrono::seconds(opt.duration_sec));
            duration_timer.async_wait([&](const boost::system::error_code& e) {
                if (!e) shutdown("duration reached");
            });
        }
        std::function<void(const boost::system::error_code&)> on_stats = [&](const boost::system::error_code& e) {
            if (e || stopping) return;
            metrics.print(stderr, "[stats]");
            stats_timer.expires_after(std::chrono::seconds(10));
            stats_timer.async_wait(on_stats);
        };
        stats_timer.expires_after(std::chrono::seconds(10));
        stats_timer.async_wait(on_stats);

        client.start();
        ioc.run();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[main] fatal: %s\n", e.what());
        rc = 1;
    }

    producer_done.store(true, std::memory_order_release);
    consumer.join();
    processor.close();
    metrics.print(stderr, "[final]");
    latency.print(stderr, "[final latency]");
    return rc;
}

int run_replay(const Options& opt) {
    std::error_code ec;
    fs::create_directories(opt.output_dir, ec);
    if (ec) {
        std::fprintf(stderr, "cannot create %s: %s\n", opt.output_dir.c_str(), ec.message().c_str());
        return 1;
    }
    Metrics metrics;
    auto start = std::chrono::steady_clock::now();
    int rc = 0;
    for (const auto& file : opt.replay_files) {
        if (!replay_file(file, opt.output_dir, metrics)) rc = 1;
    }
    double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    metrics.print(stderr, "[final]");
    std::uint64_t rows = metrics.ob_rows.load();
    std::fprintf(stderr, "[replay] %llu order book rows in %.3f s\n", static_cast<unsigned long long>(rows), secs);
    return rc;
}

}  // namespace

int main(int argc, char** argv) {
    Options opt;
    if (argc < 2 || !parse_args(argc, argv, opt)) {
        usage();
        return 2;
    }
    if (!opt.replay_files.empty()) return run_replay(opt);
    return run_live(opt);
}
