#pragma once

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <vector>

enum ValueType {
  kInt32,
  kUInt32,
  kInt64,
  kUInt64,
  kFloat,
  kDouble,
  kVarChar,

  kDataTypeMax
};

inline size_t GetTypeSize(const ValueType vt) {
  assert(vt != kVarChar);
  switch (vt) {
    case kInt32:
    case kUInt32:
      return sizeof(int32_t);
    case kInt64:
    case kUInt64:
      return sizeof(int64_t);
    case kFloat:
      return sizeof(float);
    case kDouble:
      return sizeof(double);
    default:
      abort();
  }
}