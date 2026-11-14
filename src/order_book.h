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

// One side of the book, kept sorted from worst to best so the best price sits
// at the back of the vector and the top 5 are the last 5 elements. Lookup is
// a binary search. An insert or erase deep in the book shifts the levels above
// it (a memmove), but the whole side is one contiguous block, which the CPU
// cache and prefetcher handle well.
class BookSide {
public:
    explicit BookSide(bool is_bid);

    // Binance diff semantics: qty is the new absolute size, 0 removes the level.
    void set(std::int64_t price, std::int64_t qty);

    // depth5 semantics: the snapshot (best first) is the truth for every price
    // from the best down to its last level. Deeper levels are kept.
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
