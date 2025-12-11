#pragma once

#include <cstdint>

#include "common/Type.h"
#include "util/Macros.h"

// Bitset uses a single 64-bit integer to store a set of ColumnId
class ColumnSet {
  static constexpr uint64_t BIT_ONE = (1ULL << 63);

 public:
  static ColumnSet Union(const ColumnSet& lhs, const ColumnSet& rhs) {
    return ColumnSet(lhs.bits | rhs.bits);
  }

  static ColumnSet Intersect(const ColumnSet& lhs, const ColumnSet& rhs) {
    return ColumnSet(lhs.bits & rhs.bits);
  }

  static ColumnSet EmptySet() { return ColumnSet(0); }

  static ColumnSet DiffSet(const ColumnSet& s1, const ColumnSet& s2) {
    return ColumnSet(s1.bits & ~s2.bits);
  }

 public:
  struct SingleIterator {
    SingleIterator(uint64_t b) : bits(b) {}

    bool Valid() const { return bits != 0; }

    ColumnId Value() {
      ColumnId cid = __builtin_clzl(bits);
      return cid;
    }

    void Next() {
      ColumnId cid = __builtin_clzl(bits);
      bits &= ~(BIT_ONE >> cid);
    }

   private:
    uint64_t bits;
  };

  // ConsecutiveIter returns the consecutive 1s of the bit set
  struct RangeIterator {
    RangeIterator(uint64_t b) : bits(b) {}

    bool Valid() const { return bits != 0; }

    void Value(ColumnId* start, ColumnId* end) {
      ColumnId cid = __builtin_clzl(bits);
      *start = cid;
      *end = cid;
      bits &= ~(BIT_ONE >> cid);
      while (bits != 0) {
        cid = __builtin_clzl(bits);
        if (cid == *end + 1) {
          *end = cid;
          bits &= ~(BIT_ONE >> cid);
        } else {
          break;
        }
      }
    }

    void Next() {
      // Nothing to do
    }

   private:
    uint64_t bits;
  };

 public:
  ColumnSet() : bits(0) {}
  ColumnSet(uint64_t b) : bits(b) {}
  ColumnSet(const std::vector<ColumnId>& vec) {
    for (const auto& cid : vec) {
      Add(cid);
    }
  }

  ColumnSet(const ColumnSet&) = default;
  ColumnSet& operator=(const ColumnSet&) = default;

  ~ColumnSet() = default;

  void Add(ColumnId cid) {
    INVARIANT(cid < 64);
    bits |= BIT_ONE >> cid;
  }

  void Add(uint64_t bms) { bits |= bms; }

  void Add(const ColumnSet& bs) { bits |= bs.bits; }

  bool Contain(ColumnId cid) const { return (bits & (BIT_ONE >> cid)) != 0ULL; }

  void Intersect(const ColumnSet& bs) { bits &= bs.bits; }

  void Intersect(uint64_t bms) { bits &= bms; }

  bool Empty() const { return bits == 0; }

  size_t Size() const { return __builtin_popcountl(bits); }

  uint64_t Uint64() const { return bits; }

  void Reset() { bits = 0ULL; }

 public:
  // Generate iterator
  SingleIterator Iter() const { return SingleIterator(bits); }

  RangeIterator RangeIter() const { return RangeIterator(bits); }

 private:
  uint64_t bits;
};
