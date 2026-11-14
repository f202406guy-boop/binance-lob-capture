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
