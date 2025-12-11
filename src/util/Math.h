#pragma once

#include <algorithm>

namespace util {
template <typename T> T round_up(T d, T align) {
  return ((d - 1) / align + 1) * align;
}

template <typename T>
T max_of(T a, T b, T c) {
  return std::max(a, std::max(b, c));
}
} // namespace util