// This file turns Binance's decimal strings into int64 fixed-point with 8 decimals, so
// "65000.12000000" becomes 6500012000000. I didn't want any float or double on the price/size
// path: the depth5 check compares levels for exact equality, and numbers like 0.1 aren't
// exact in binary floating point. So it's just integer math straight from the string.
// parse_scaled() rejects anything that isn't a plain non-negative decimal, more than 8
// non-zero decimals (that would lose precision, extra trailing zeros are fine), and anything
// that doesn't fit in int64. The overflow checks run before each multiply, so it never
// actually overflows. The max is 92233720368.54775807, plenty for any price or size. The edge
// cases, including that max and one past it, are in tests/unit_tests.cpp (test_decimal).

#pragma once

#include <cstdint>
#include <limits>
#include <string_view>

constexpr int kScaleDigits = 8;
constexpr std::int64_t kScale = 100000000;

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
