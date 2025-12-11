#pragma once

#include <cstdint>
#include <vector>

#include "common/Type.h"
#include "util/Macros.h"

// Define some configuration parameters and constant values in this file

constexpr uint32_t MAX_ATTRIBUTE_NUM = 64;

constexpr uint32_t WORD_SIZE = 8; // 8 Bytes

constexpr uint32_t LOCK_SLOT_PER_WORD = 64;

constexpr uint32_t MAX_LOCK_SLOT_NUM = MAX_ATTRIBUTE_NUM;

constexpr uint32_t WORD_NUM_FOR_LOCK = MAX_LOCK_SLOT_NUM / LOCK_SLOT_PER_WORD;

static_assert(MAX_LOCK_SLOT_NUM % LOCK_SLOT_PER_WORD == 0);

static_assert(MAX_LOCK_SLOT_NUM % sizeof(uint64_t) == 0,
              "MAX_ATTRIBUTE_NUM must be aligned with 8bytes");

constexpr uint64_t CACHE_LINE_SIZE = 64;

constexpr uint32_t MAX_COLUMN_VALUE_SIZE = 16;

constexpr uint32_t MAX_COLUMN_HEADER_INLINE_SIZE = 16;

constexpr uint32_t VCELL_SIZE = 16;

constexpr uint32_t VCELL_NUM = 4;

constexpr uint32_t MAX_MEMORY_NODE_NUM = 16;

// Max number of coro for each thread
constexpr uint32_t MAX_CORO_NUM = 16;

constexpr uint32_t MAX_COLUMN_NUM = 20;

constexpr lock_t UNLOCKED = 0x00;

constexpr lock_t LOCKED = 0xFF;

constexpr uint64_t UNUSED = 0x00;

constexpr uint64_t USED = 0x01;

constexpr uint64_t LOCK_CHECK_BIT = 0x01ULL << 63;

inline std::vector<ColumnId> GetUpdatedColumn(bitmap_t bm) {
  std::vector<ColumnId> ret;
  uint64_t attr_num = MAX_ATTRIBUTE_NUM;
  while (bm != 0) {
    int d = TRAILING_ZEROS(bm);
    ret.emplace_back(attr_num - 1 - d);
    bm >>= (1 + d);
    attr_num -= (1 + d);
  }
  return ret;
}
