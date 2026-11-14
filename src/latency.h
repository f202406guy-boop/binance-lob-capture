#pragma once

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <vector>

// Counts latencies in 1 microsecond buckets up to 10 ms. Anything slower lands
// in the last bucket, but max() still reports the real value. Memory stays
// fixed no matter how long the capture runs.
class LatencyHistogram {
public:
    static constexpr int kBuckets = 10000;

    LatencyHistogram() : buckets_(kBuckets + 1, 0) {}

    void add(std::int64_t ns) {
        std::int64_t us = ns / 1000;
        if (us < 0) us = 0;
        if (us > max_us_) max_us_ = us;
        if (us > kBuckets) us = kBuckets;
        buckets_[us]++;
        count_++;
    }

    std::uint64_t count() const { return count_; }
    std::int64_t max() const { return max_us_; }

    // Latency in microseconds that p percent of the samples are at or below.
    std::int64_t percentile(double p) const {
        if (count_ == 0) return 0;
        std::uint64_t need = static_cast<std::uint64_t>(std::ceil(p / 100.0 * static_cast<double>(count_)));
        if (need == 0) need = 1;
        std::uint64_t seen = 0;
        for (int i = 0; i <= kBuckets; ++i) {
            seen += buckets_[i];
            if (seen >= need) return i;
        }
        return kBuckets;
    }

private:
    std::vector<std::uint64_t> buckets_;
    std::uint64_t count_ = 0;
    std::int64_t max_us_ = 0;
};

// wait: from reading the frame off the socket to the book thread starting on it.
// work: time spent inside Processor::process (parse, book update, CSV row).
struct LatencyStats {
    LatencyHistogram wait;
    LatencyHistogram work;

    void print(std::FILE* out, const char* tag) const {
        auto ll = [](std::int64_t v) { return static_cast<long long>(v); };
        std::fprintf(out,
                     "%s n=%llu wait_us p50=%lld p99=%lld p99.9=%lld max=%lld | work_us p50=%lld p99=%lld "
                     "p99.9=%lld max=%lld\n",
                     tag, static_cast<unsigned long long>(wait.count()), ll(wait.percentile(50)),
                     ll(wait.percentile(99)), ll(wait.percentile(99.9)), ll(wait.max()), ll(work.percentile(50)),
                     ll(work.percentile(99)), ll(work.percentile(99.9)), ll(work.max()));
    }
};
