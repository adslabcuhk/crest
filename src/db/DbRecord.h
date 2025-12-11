#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "common/Type.h"
#include "db/Schema.h"
#include "db/ValueType.h"
#include "util/Macros.h"

// DbRecord is a serialization of a database record
// It's the resultant record of transaction's execution results and
// the database's input.
class DbRecord {
 public:
  DbRecord() : data_(nullptr), sz_(0), record_schema_(nullptr) {}

  DbRecord(const DbRecord &) = default;
  DbRecord &operator=(const DbRecord &) = default;

  DbRecord(char *d, size_t sz, const RecordValueSchema *record_schema)
      : data_(d), sz_(sz), record_schema_(record_schema) {}

 public:
  bool Valid() const { return data_ != nullptr && sz_ != 0 && record_schema_ != nullptr; }

  void Reset() {
    data_ = nullptr;
    sz_ = 0;
    record_schema_ = nullptr;
  }

  template <typename T>
  T GetValue(ColumnId cid) {
    ASSERT(Valid(), "");
    ASSERT(record_schema_->GetColumnValueType(cid) != kVarChar, "");
    return *reinterpret_cast<T *>(data_ + record_schema_->GetColumnOffset(cid));
  }

  // Specialized for VarChar type
  template <typename T = char *>
  char *GetValue(ColumnId cid) {
    ASSERT(Valid(), "");
    ASSERT(record_schema_->GetColumnValueType(cid) == kVarChar, "");
    return data_ + record_schema_->GetColumnOffset(cid);
  }

  template <typename T>
  T SetValue(ColumnId cid, const T &v) {
    ASSERT(Valid(), "");
    ASSERT(record_schema_->GetColumnValueType(cid) != kVarChar, "");
    *reinterpret_cast<T *>(data_ + record_schema_->GetColumnOffset(cid)) = v;
  }

  // Specialized for VarChar type
  template <typename T = char *>
  char *SetValue(ColumnId cid, const char *src, size_t sz) {
    ASSERT(Valid(), "");
    ASSERT(record_schema_->GetColumnSize(cid) >= sz, "");
    std::memcpy(data_ + record_schema_->GetColumnOffset(cid), src, sz);
  }

  // Write values of specified column to destination
  void CopyTo(ColumnId cid, char *dst) const {
    ASSERT(Valid(), "");
    std::memcpy(dst, data_ + record_schema_->GetColumnOffset(cid),
                record_schema_->GetColumnSize(cid));
  }

  void CopyFrom(ColumnId cid, const char *src) {
    ASSERT(Valid(), "");
    std::memcpy(data_ + record_schema_->GetColumnOffset(cid), src,
                record_schema_->GetColumnSize(cid));
  }

  void SetRecordSchema(RecordValueSchema* record_schema) {
    record_schema_ = record_schema;
  }

  // Dump this record to string for better illustration
  std::string FormatString() const;

 private:
  char *data_;
  size_t sz_;
  const RecordValueSchema *record_schema_;
};