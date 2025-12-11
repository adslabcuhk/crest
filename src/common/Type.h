#pragma once

#include <cstdint>
#include <limits>
#include <sstream>
#include <string>

#include "mempool/Coroutine.h"

using RecordKey = uint64_t;    // The key type used to index a row
using ColumnId = uint32_t;     // Uniquely distinguish each column
using TableId = uint64_t;      // Unique identifier of a table
using lock_t = uint8_t;        // Use 1byte as a lock for each column
using timestamp_t = uint64_t;  // A timestamp is at most uint64_t
using node_id_t = uint16_t;    // Unique identifier of memory and compute nodes
using PoolPtr = uint64_t;      // Global memory pointer (both CN & MN)
using ThreadId = uint64_t;     // Unique identifier of a thread
using TxnId = uint64_t;        // Unique identifier of transaction
using version_t = uint64_t;    // Identifying a version
// using LockStatus = uint64_t;   // Lock Status
using bitmap_t = uint16_t;       // Indicate which columns have been modified
using sub_version_t = uint16_t;  // Indicate the sub-version of a version
using txn_type_id = uint64_t;    // Transaction type id
using SegmentId = uint32_t;      // Identifiying each Segment
using BucketId = uint64_t;       // The Id of a PoolHashIndex
using SlotId = uint32_t;         // The SlotId within a HashBucket

enum class TableCCLevel {
  RECORD_LEVEL = 1,  // Transactions accessing this table must acquire the record lock
  CELL_LEVEL,        // Transactions accessing this table at fine-grained cell level
};

inline node_id_t NodeId(PoolPtr g_ptr) { return static_cast<node_id_t>(g_ptr >> 48); }

inline uint64_t NodeAddress(PoolPtr g_ptr) {
  return static_cast<uint64_t>((g_ptr << 16) >> 16);
}

inline PoolPtr MakePoolPtr(node_id_t nid, uint64_t ptr) { return (uint64_t)nid << 48 | ptr; }

static constexpr uint64_t lower_48_bits_mask = ~(0xFFFFULL << 48);

inline uint64_t Bitmap(version_t v) { return v & ~lower_48_bits_mask; }

// inline version_t Version(timestamp_t ts, bitmap_t bm) { return ((uint64_t)bm) << 16 | ts; }

// inline version_t Version(timestamp_t ts, uint64_t bm) { return bm | ts; }

inline version_t Version(sub_version_t sub_v, timestamp_t ts) {
  return ((uint64_t)sub_v) << 48 | ts;
}

inline sub_version_t SubVersion(version_t v) { return static_cast<sub_version_t>(v >> 48); }

inline timestamp_t Timestamp(version_t v) {
  return static_cast<timestamp_t>(v & lower_48_bits_mask);
}

inline TxnId TransactionId(ThreadId tid, coro::coro_id_t coro_id, uint64_t sequence) {
  uint64_t t = static_cast<uint8_t>(tid);
  uint64_t c = static_cast<uint8_t>(coro_id);
  return (t << 56) | (c << 48) | sequence;
}

inline ThreadId GetThreadId(TxnId txn_id) { return txn_id >> 56; }

inline ThreadId GetCoroId(TxnId txn_id) { return (txn_id << 8) >> 56; }

inline ThreadId GetSequence(TxnId txn_id) { return (txn_id << 16) >> 16; }

inline std::string TxnIdToString(TxnId txn_id) {
  std::stringstream ss;
  ss << std::to_string(GetThreadId(txn_id)) << ":" << std::to_string(GetCoroId(txn_id)) << ":"
     << std::to_string(GetSequence(txn_id));
  return ss.str();
}

// some constant values
constexpr timestamp_t TIMESTAMP_INF = std::numeric_limits<timestamp_t>::max();
constexpr timestamp_t BASE_TIMESTAMP = 0;

constexpr PoolPtr POOL_PTR_NULL = 0;

constexpr RecordKey INVALID_RECORD_KEY = (RecordKey)-1;

constexpr timestamp_t INVALID_TIMESTAMP = (timestamp_t)-1;

constexpr TableId INVALID_TABLE_ID = (TableId)-1;

constexpr ColumnId NULL_COLUMN_KEY = (ColumnId)-1;

constexpr SegmentId INVALID_SEGMENT_ID = (SegmentId)-1;

constexpr SlotId INVALID_SLOT_ID = (SlotId)-1;

constexpr ColumnId INVALID_COLUMN_ID = (ColumnId)-1;

constexpr char INVISIBLE_DATA = (char)0x00;

constexpr char VISIBLE_DATA = (char)0x01;

constexpr version_t BASE_VERSION = 00ULL;

constexpr version_t INIT_INSERT_VERSION = BASE_VERSION + 1;

constexpr version_t INVALID_VERSION = TIMESTAMP_INF;


// Check the if the right-leading bit is set
inline bool is_set(uint64_t value, ColumnId cid) { return value & (1ULL << (63 - cid)); }