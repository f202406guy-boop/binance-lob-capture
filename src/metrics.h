#pragma once

#include <atomic>
#include <cstdint>
#include <cstdio>

using Counter = std::atomic<std::uint64_t>;

inline void bump(Counter& c, std::uint64_t n = 1) {
    c.fetch_add(n, std::memory_order_relaxed);
}

struct Metrics {
    Counter messages{0};
    Counter parse_errors{0};
    Counter unknown_streams{0};
    Counter reconnects{0};
    Counter queue_full_waits{0};
    Counter md_rows{0};
    Counter ob_rows{0};
    Counter diffs_applied{0};
    Counter diffs_stale{0};
    Counter diffs_waiting{0};
    Counter gaps{0};
    Counter book_resets{0};
    Counter depth5_applied{0};
    Counter depth5_stale{0};
    Counter depth5_checked{0};
    Counter depth5_mismatch{0};
    Counter depth5_folded{0};
    Counter trades{0};
    Counter rest_snapshots{0};
    Counter rest_applied{0};
    Counter rest_dropped{0};
    Counter rest_top_mismatch{0};
    Counter write_errors{0};

    void print(std::FILE* out, const char* tag) const {
        auto v = [](const Counter& c) { return static_cast<unsigned long long>(c.load(std::memory_order_relaxed)); };
        std::fprintf(out,
                     "%s messages=%llu md_rows=%llu ob_rows=%llu parse_errors=%llu unknown_streams=%llu "
                     "reconnects=%llu diffs_applied=%llu diffs_stale=%llu diffs_waiting=%llu gaps=%llu "
                     "book_resets=%llu depth5_applied=%llu depth5_stale=%llu depth5_checked=%llu "
                     "depth5_mismatch=%llu depth5_folded=%llu trades=%llu rest_snapshots=%llu "
                     "rest_applied=%llu rest_dropped=%llu rest_top_mismatch=%llu queue_full_waits=%llu "
                     "write_errors=%llu\n",
                     tag, v(messages), v(md_rows), v(ob_rows), v(parse_errors), v(unknown_streams),
                     v(reconnects), v(diffs_applied), v(diffs_stale), v(diffs_waiting), v(gaps),
                     v(book_resets), v(depth5_applied), v(depth5_stale), v(depth5_checked),
                     v(depth5_mismatch), v(depth5_folded), v(trades), v(rest_snapshots), v(rest_applied),
                     v(rest_dropped), v(rest_top_mismatch), v(queue_full_waits), v(write_errors));
    }
};
