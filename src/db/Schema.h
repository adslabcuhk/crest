#pragma once

#include <algorithm>
#include <cstddef>
#include <string>

#include "common/Config.h"
#include "common/Type.h"
#include "db/ColumnInfo.h"
#include "db/Format.h"
#include "db/ValueType.h"
#include "util/Math.h"

// A Schema class indicates how the database manages the records on memory
// nodes.
class TxnSchema {
 public:
  // Constructor must accept an input argument to create the schema
  TxnSchema(SchemaArg args) {
    // Sort the input arg based on their column id
    std::sort(args.begin(), args.end(),
              [](const auto &a, const auto &b) { return a.ckey < b.ckey; });

    size_t curr_offset = RecordHeaderSize;
    for (const auto &arg : args) {
      size_t value_sz = arg.GetValueSize();

      size_t column_mem_sz = ColumnHeaderSize + value_sz;
      if (cross_cacheline(curr_offset, column_mem_sz)) {
        curr_offset = util::round_up(curr_offset, CACHE_LINE_SIZE);
      }

      column_infos_.emplace_back(arg, column_mem_sz, curr_offset, false);
      curr_offset += column_mem_sz;
    }
    record_mem_size_ = util::round_up(curr_offset, CACHE_LINE_SIZE);
  }

  // Create an empty schema, such a stupid way
  TxnSchema() = default;

  TxnSchema(const TxnSchema &) = default;
  TxnSchema &operator=(const TxnSchema &) = default;

 public:
  // Schema only provides constant interface as it can not be changed once
  // created
  size_t GetColumnCount() const { return column_infos_.size(); }

  const ColumnInfo &GetColumnInfo(ColumnId cid) const { return column_infos_[cid]; }

  const std::vector<ColumnInfo> &GetColumnInfos() const { return column_infos_; }

  size_t ColumnOffset(ColumnId cid) const { return GetColumnInfo(cid).offset; }

  size_t ColumnMemSize(ColumnId cid) const { return GetColumnInfo(cid).mem_sz; }

  size_t SubVersionOffset(ColumnId cid) const {
    return offsetof(RecordHeader, sub_versions) + cid * sizeof(sub_version_t);
  }

  // Return the size of [cid1, cid2]
  size_t MultiColumnSize(ColumnId cid1, ColumnId cid2) const {
    INVARIANT(cid1 <= cid2);
    INVARIANT(cid2 < column_infos_.size());
    INVARIANT(0 <= cid1);
    return ColumnOffset(cid2) - ColumnOffset(cid1) + ColumnMemSize(cid2);
  }

  ValueType GetValueType(ColumnId cid) const { return GetColumnInfo(cid).GetValueType(); }

  size_t GetRecordMemSize() const { return record_mem_size_; }

  TableCCLevel GetTableCCLevel() const { return cc_level_; }
  void SetTableCCLevel(TableCCLevel cc_level) { cc_level_ = cc_level; }

 private:
  inline bool cross_cacheline(uint64_t start, uint64_t sz) {
    return (start >> 6) != (start + sz - 1) >> 6;
  }

 private:
  std::vector<ColumnInfo> column_infos_;
  size_t record_mem_size_;
  TableCCLevel cc_level_;
};

// A RecordSchema specifies the data organization of a DbRecord
class RecordValueSchema {
 public:
  RecordValueSchema() = delete;
  RecordValueSchema(const RecordValueSchema &) = default;
  RecordValueSchema &operator=(const RecordValueSchema &) = default;

  RecordValueSchema(SchemaArg args) {
    std::sort(args.begin(), args.end(),
              [](const auto &a, const auto &b) { return a.ckey < b.ckey; });
    // A RecordSchema is basically a flatten serialization of a record
    for (const auto &arg : args) {
      size_t column_sz = arg.GetValueSize();
      column_infos_.emplace_back(arg, column_sz, arg.col_offset, false);
    }
  }

  size_t GetColumnOffset(ColumnId cid) const { return column_infos_[cid].offset; }

  size_t GetColumnSize(ColumnId cid) const { return column_infos_[cid].value_sz; }

  ValueType GetColumnValueType(ColumnId cid) const { return column_infos_[cid].value_type; }

 private:
  // Re-use the ColumnInfo structs
  std::vector<ColumnInfo> column_infos_;
  size_t record_schema_size_;
};