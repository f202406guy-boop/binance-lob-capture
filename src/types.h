#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>

enum class Venue { Spot, Usdm };

// Wall clock in nanoseconds since the epoch, written to the CSVs as receive time.
inline std::int64_t wall_clock_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// Monotonic clock for measuring durations. The wall clock can jump when the OS
// adjusts it (WSL does this often), which would show up as fake latency.
inline std::int64_t mono_clock_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
enum class StreamKind : std::uint8_t { DepthDiff, Depth5, Trade };

inline const char* venue_name(Venue v) {
    return v == Venue::Spot ? "spot" : "usdm";
}

inline const char* stream_kind_name(StreamKind k) {
    switch (k) {
    case StreamKind::DepthDiff: return "depth_diff";
    case StreamKind::Depth5: return "depth5";
    case StreamKind::Trade: return "trade";
    }
    return "unknown";
}

inline bool parse_venue(std::string_view s, Venue& out) {
    if (s == "spot") { out = Venue::Spot; return true; }
    if (s == "usdm") { out = Venue::Usdm; return true; }
    return false;
}

inline bool parse_stream_kind(std::string_view s, StreamKind& out) {
    if (s == "depth_diff") { out = StreamKind::DepthDiff; return true; }
    if (s == "depth5") { out = StreamKind::Depth5; return true; }
    if (s == "trade") { out = StreamKind::Trade; return true; }
    return false;
}

// One inbound message with the combined-stream envelope removed.
// Replay mode rebuilds exactly this from a market data CSV row.
struct RawMessage {
    std::int64_t recv_ns = 0;
    std::int64_t recv_mono_ns = 0;  // only for latency stats, not written anywhere
    std::uint32_t shard_id = 0;
    std::uint32_t conn_epoch = 0;
    std::uint64_t conn_seq = 0;
    std::uint32_t symbol_index = 0;
    StreamKind kind = StreamKind::DepthDiff;
    std::string payload;
};
