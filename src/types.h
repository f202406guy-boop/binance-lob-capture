// This file has the small shared pieces: the Venue and StreamKind enums with their CSV names,
// the two clocks, and RawMessage, which is one message (combined-stream envelope already
// removed) sitting in the queue between the network thread and the book thread. Replay
// rebuilds the exact same RawMessage from a market data CSV row, so the book code can't tell
// live and replay apart. I kept two clocks on purpose. wall_clock_ns() is the receive time
// that goes into the CSVs. mono_clock_ns() uses steady_clock and is only for measuring
// durations (that's what recv_mono_ns is for, it's never written anywhere), because WSL steps
// the wall clock when it re-syncs and my first runs showed a fake max latency of up to 2 s.
// RestSnapshot isn't a WebSocket stream, it's the REST /depth reply (only with
// --rest-snapshot), but it still goes into the market data file like the others, so replay
// sees exactly what live saw.

#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>

enum class Venue { Spot, Usdm };

inline std::int64_t wall_clock_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

inline std::int64_t mono_clock_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
enum class StreamKind : std::uint8_t { DepthDiff, Depth5, Trade, RestSnapshot };

inline const char* venue_name(Venue v) {
    return v == Venue::Spot ? "spot" : "usdm";
}

inline const char* stream_kind_name(StreamKind k) {
    switch (k) {
    case StreamKind::DepthDiff: return "depth_diff";
    case StreamKind::Depth5: return "depth5";
    case StreamKind::Trade: return "trade";
    case StreamKind::RestSnapshot: return "rest_snapshot";
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
    if (s == "rest_snapshot") { out = StreamKind::RestSnapshot; return true; }
    return false;
}

struct RawMessage {
    std::int64_t recv_ns = 0;
    std::int64_t recv_mono_ns = 0;
    std::uint32_t shard_id = 0;
    std::uint32_t conn_epoch = 0;
    std::uint64_t conn_seq = 0;
    std::uint32_t symbol_index = 0;
    StreamKind kind = StreamKind::DepthDiff;
    std::string payload;
};
