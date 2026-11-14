// This file is one side of the order book (BookSide), the code is in order_book.cpp. I keep
// the levels in a sorted std::vector from worst to best, so the best price sits at the back
// and the top 5 are just the last 5 elements. Lookup is a binary search. An insert or erase
// deep in the book has to shift the levels above it (a memmove), but the whole side is one
// contiguous block with no allocation per level, which the CPU cache and prefetcher like.
// I thought about std::map, but every level would be its own heap node and each lookup would
// chase pointers. A flat array indexed by price tick would be O(1), but it needs a fixed
// price band and the tick size for each symbol, which felt like too much for this project.
// set() follows Binance's diff rules: qty is the new absolute size and 0 removes the level.
// replace_top() is for depth5: the snapshot (best first) is the truth for every price from
// the best down to its last level, and deeper levels are kept. Prices and sizes are int64
// scaled by 10^8 (see decimal.h).

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

struct Level {
    std::int64_t price = 0;
    std::int64_t qty = 0;
};

inline bool operator==(const Level& a, const Level& b) {
    return a.price == b.price && a.qty == b.qty;
}

class BookSide {
public:
    explicit BookSide(bool is_bid);

    void set(std::int64_t price, std::int64_t qty);

    void replace_top(const std::vector<Level>& snapshot);
    bool top_matches(const std::vector<Level>& snapshot) const;

    void clear() { levels_.clear(); }
    Level level(std::size_t i) const;
    std::size_t size() const { return levels_.size(); }

private:
    bool better(std::int64_t a, std::int64_t b) const { return is_bid_ ? a > b : a < b; }

    bool is_bid_;
    std::vector<Level> levels_;
};
