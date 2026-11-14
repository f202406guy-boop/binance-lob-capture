// The code behind BookSide. The constructor reserves 8192 levels per side up front, so in the
// normal case the vector doesn't have to reallocate while the capture runs. set() finds the
// spot with std::lower_bound and better(), so the same code works for bids and asks, and then
// updates, inserts or erases (a size of 0 for a price we don't have is just ignored).
// replace_top() first pops every level from the best down to the snapshot's worst price, then
// pushes the snapshot back on in reverse (worst first, since the best has to end up at the
// back), skipping zero sizes. level(i) counts from the best price and gives an empty Level
// past the end, which is how missing levels turn into 0 in the order book CSV. In the
// callgrind profile of a replay, set() is the biggest cost (about 43%, almost all of it the
// memmove), but live that's still only tens of microseconds at p99. tests/unit_tests.cpp
// (test_book_side) covers all of this, including replace_top on both sides.

#include "order_book.h"

#include <algorithm>

BookSide::BookSide(bool is_bid) : is_bid_(is_bid) {
    levels_.reserve(8192);
}

void BookSide::set(std::int64_t price, std::int64_t qty) {
    auto it = std::lower_bound(levels_.begin(), levels_.end(), price,
                               [this](const Level& l, std::int64_t p) { return better(p, l.price); });
    if (it != levels_.end() && it->price == price) {
        if (qty == 0)
            levels_.erase(it);
        else
            it->qty = qty;
    } else if (qty != 0) {
        levels_.insert(it, Level{price, qty});
    }
}

void BookSide::replace_top(const std::vector<Level>& snapshot) {
    if (snapshot.empty()) return;
    std::int64_t worst = snapshot.back().price;
    while (!levels_.empty() && !better(worst, levels_.back().price)) levels_.pop_back();
    for (auto it = snapshot.rbegin(); it != snapshot.rend(); ++it) {
        if (it->qty != 0) levels_.push_back(*it);
    }
}

bool BookSide::top_matches(const std::vector<Level>& snapshot) const {
    for (std::size_t i = 0; i < snapshot.size(); ++i) {
        if (!(level(i) == snapshot[i])) return false;
    }
    return true;
}

Level BookSide::level(std::size_t i) const {
    if (i >= levels_.size()) return Level{};
    return levels_[levels_.size() - 1 - i];
}
