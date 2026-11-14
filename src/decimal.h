#pragma once

#include <cstdint>
#include <limits>
#include <string_view>

constexpr int kScaleDigits = 8;
constexpr std::int64_t kScale = 100000000;

// Turns a Binance decimal string into a fixed-point integer with 8 decimals,
// e.g. "65000.12000000" -> 6500012000000. No floating point is involved.
// Fails on anything that is not a plain non-negative decimal, on more than
// 8 significant decimals, and on int64 overflow.
inline bool parse_scaled(std::string_view s, std::int64_t& out) {
    constexpr std::int64_t kMax = std::numeric_limits<std::int64_t>::max();
    std::int64_t whole = 0;
    std::int64_t frac = 0;
    int frac_digits = 0;
    bool any_digit = false;
    std::size_t i = 0;

    for (; i < s.size() && s[i] != '.'; ++i) {
        char c = s[i];
        if (c < '0' || c > '9') return false;
        int d = c - '0';
        if (whole > (kMax - d) / 10) return false;
        whole = whole * 10 + d;
        any_digit = true;
    }

    if (i < s.size()) {
        ++i;
        for (; i < s.size(); ++i) {
            char c = s[i];
            if (c < '0' || c > '9') return false;
            if (frac_digits < kScaleDigits) {
                frac = frac * 10 + (c - '0');
                ++frac_digits;
            } else if (c != '0') {
                return false;
            }
            any_digit = true;
        }
    }

    if (!any_digit) return false;
    for (; frac_digits < kScaleDigits; ++frac_digits) frac *= 10;
    if (whole > (kMax - frac) / kScale) return false;
    out = whole * kScale + frac;
    return true;
}
