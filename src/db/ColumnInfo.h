#pragma once

#include "common/Config.h"
#include "common/Type.h"
#include "db/Format.h"
#include "db/ValueType.h"
#include "util/Math.h"

struct ColumnArg {
  ColumnId ckey;
  std::string name;
  ValueType vtype;
  size_t var_size;  // For non-fix sized attribute
  bool is_primary_key;
  int col_offset;

  // A VarsizeType has non-fixed value size, such as VarChar, Blobs
  bool IsVarsizeType() const { return vtype == kVarChar; }

  size_t GetValueSize() const { return IsVarsizeType() ? var_size : GetTypeSize(vtype); }
};

using SchemaArg = std::vector<ColumnArg>;

// A ColumnInfo records the layout and metadata information of a table column
// When accessing a row or a dedicated column, it's necessary to find the
// corresponding column offset and column size within this row and do the
// actual data access accordingly
struct ColumnInfo {
  ColumnId ckey;           // Id of this column
  std::string name;        // Name of this column
  ValueType value_type;    // Value type of this column
  size_t value_sz;         // Value size of this column, might be variable one
  size_t mem_sz;           // Size of memory each value of this column consumes
  size_t offset;           // Offset between this column and the first column
  bool across_cache_line;  // If data layout spans multiple cacheline
  bool primary_key;        // If this column is a primary key column

  ColumnInfo(ColumnArg arg, size_t mem_sz, size_t offset, bool across_cache_line)
      : ckey(arg.ckey),
        name(arg.name),
        value_type(arg.vtype),
        value_sz(arg.IsVarsizeType() ? arg.var_size : GetTypeSize(arg.vtype)),
        mem_sz(mem_sz),
        offset(offset),
        across_cache_line(across_cache_line),
        primary_key(arg.is_primary_key) {}

  ColumnInfo(ColumnId ckey, const std::string &name, const ValueType &vt, size_t value_sz,
             size_t mem_sz, size_t offset, bool across_cache_line, bool is_primary_key)
      : ckey(ckey),
        name(name),
        value_type(vt),
        value_sz(value_sz),
        mem_sz(mem_sz),
        offset(offset),
        across_cache_line(across_cache_line),
        primary_key(is_primary_key) {}

  auto Name() const { return name; }

  auto ColumnKey() const { return ckey; }

  ValueType GetValueType() const { return value_type; }

  size_t GetValueSize() const { return value_sz; }

  size_t GetDataDisplaySize() const {
    if (value_type == kVarChar) {
      return value_sz;
    } else {
      return 10;
    }
  }

  // [DEBUG] Return the number of characters to display a value in this column
  size_t GetDisplaySize() const {
    if (value_type == kVarChar) {
      return 40;
    } else {
      return 10;  // might be OK
    }
  }
};