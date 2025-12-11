#pragma once

#include <cstdlib>

#include "common/Config.h"
#include "common/Type.h"

// This file defines how the database records are stored on MNs.
// Each record has a row header and its comprising columns:
//
// |-- RowHeader --|-- ColumnHeader, ColumnValue --|
//
// The RowHeader contains:
//  * Record Identifier: the table id and indexing key
//  * The used flag: If this record space has been occupied
//  * The lock array: each bit of which is used as a column lock
//
// The ColumnHeader contains:
//  * Timestamp of the current value of this column
//
// Currently our system only supports at most 64 attributes.

// struct RowHeader {
//   TableId table_id;
//   RecordKey key;
//   uint64_t used;  // If this Row is in use
//   // NOTE: make sure the address of lock_array 8B-aligned
//   lock_t lock_array[WORD_NUM_FOR_LOCK * WORD_SIZE];

//   bool Used() const { return used; }

// } ALIGNED(WORD_SIZE);

// static constexpr size_t RowHeaderSize = sizeof(RowHeader);

struct RecordHeader {
  RecordKey record_key;                        // The key of the indexing record
  uint64_t used;                               // If this Row is in use, for insertion
  uint64_t lock;                               // Seperate lock for each column
  sub_version_t sub_versions[MAX_COLUMN_NUM];  // An aggregate array for all sub-versions

  RecordKey GetRecordKey() const { return record_key; }

  bool InUse() const { return used; }

  void SetUnuse() { this->used = UNUSED; }

  void SetInuse() { this->used = USED; }

  sub_version_t GetSubVersion(ColumnId cid) const { return sub_versions[cid]; }

  void UpdateSubVersion(sub_version_t sv, ColumnId cid) { sub_versions[cid] = sv; }

  bool RecordKeyMatch(RecordKey key) const { return InUse() && record_key == key; }
} ALIGNED(CACHE_LINE_SIZE);

static constexpr size_t RecordHeaderSize = sizeof(RecordHeader);

static_assert(RecordHeaderSize <= CACHE_LINE_SIZE, "RecordHeaderSize exceeds the size limit");

struct ColumnHeader {
  version_t version;       // The write timestamp and attributes modified
  uint8_t inline_data[0];  // data stored in this ColumnHeader

  char* ColumnData() const { return (char*)&inline_data[0]; }
} ALIGNED(WORD_SIZE);

static constexpr size_t ColumnHeaderSize = sizeof(ColumnHeader);

// ALWAYS_INLINE
// char* lock_array_pos(char* record_addr) { return record_addr + offsetof(RowHeader, lock_array); }

// ALWAYS_INLINE
// PoolPtr lock_array_pos(PoolPtr record_addr) {
//   return record_addr + offsetof(RowHeader, lock_array);
// }