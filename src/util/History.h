#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <vector>

#include "util/Macros.h"

// A class recoding and analysis a batch of data, copied from rocksdb codebase
// This class is thread-local.
struct History {
  friend class Statistics;
  // This would support 100 * 1000000 = 1e8 samples
  static const uint64_t kRecordInterval = 10;
  static const size_t kDefaultHistSize = 300000;

  History(size_t hist_size = kDefaultHistSize) : hist_size(hist_size), hist() {
    hist.clear();
    hist.resize(hist_size, 0);
    inc = 0;
    min_ = std::numeric_limits<uint64_t>::max();
    max_ = std::numeric_limits<uint64_t>::min();
    sum_ = 0;
    num_ = 0;
    sum_squares_ = 0;
  }
  ~History() = default;

  ALWAYS_INLINE
  void Add(uint64_t value) {
    auto idx = inc++;
    hist[(idx / kRecordInterval) % hist_size] = value;
    num_ += 1;
    min_ = (std::min(min(), value));
    max_ = (std::max(max(), value));
    sum_ += value;
    sum_squares_ += (value * value);
    sorted_ = false;
  }

  void Clear() {
    inc = 0;
    min_ = std::numeric_limits<uint64_t>::max();
    max_ = std::numeric_limits<uint64_t>::min();
    sum_ = 0;
    num_ = 0;
    sum_squares_ = 0;
  }

  inline uint64_t min() const { return min_; }
  inline uint64_t max() const { return max_; }
  inline uint64_t num() const { return num_; }
  inline uint64_t sum() const { return sum_; }
  inline uint64_t sum_squares() const { return sum_squares_; }

  double Median() const { return Percentile(0.50); }

  // Do not use this function when there are concurrent updates
  double Percentile(double p) const {
    if (!sorted_) {
      std::sort(hist.begin(), hist.begin() + LastRecordIndex());
      sorted_ = true;
    }
    return hist[(int)(LastRecordIndex() * p)];
  }

  double Average() const {
    auto n = num(), s = sum();
    if (n == 0) {
      return 0.0;
    }
    return s / n;
  }

  double StandardDeviation() const {
    uint64_t cur_num = num();
    uint64_t cur_sum = sum();
    uint64_t cur_sum_squares = sum_squares();
    if (cur_num == 0) return 0;
    double variance = static_cast<double>(cur_sum_squares * cur_num - cur_sum * cur_sum) /
                      static_cast<double>(cur_num * cur_num);
    return std::sqrt(variance);
  }

  // Merge with another history object, do not use this function
  // with concurrent update functions
  void Merge(const History &h) {
    // Erase all data invalid
    hist.erase(hist.begin() + LastRecordIndex() + 1, hist.end());
    hist.insert(hist.end(), h.hist.begin(), h.hist.begin() + h.LastRecordIndex() + 1);
    inc = hist.end() - hist.begin();
    min_ = std::min(min(), h.min());
    max_ = std::max(max(), h.max());
    num_ += h.num();
    sum_ += h.sum();
    sum_squares_ += h.sum_squares();
    sorted_ = false;
    // add the history data
  }

 private:
  uint64_t LastRecordIndex() const { return std::min(hist_size - 1, num() / kRecordInterval); }

  size_t hist_size;
  int inc;

  uint64_t min_;
  uint64_t max_;
  uint64_t num_;
  uint64_t sum_;
  uint64_t sum_squares_;
  mutable bool sorted_;
  mutable std::vector<uint64_t> hist;
  // uint64_t hist[kDefaultHistSize];
};