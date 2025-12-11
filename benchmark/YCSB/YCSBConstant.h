#pragma once

#include <string>

#include "Base/BenchConfig.h"
#include "Base/BenchTypes.h"
#include "common/Type.h"

namespace ycsb {
const uint64_t YCSB_OP_RECORD_MAX_NUM = 12;
constexpr size_t YCSB_TABLE_NUM = 2;
constexpr size_t YCSB_TXN_NUM = 2;

// Table Definitions:
constexpr TableId YCSB_TABLE0 = 0;
constexpr TableId YCSB_TABLE1 = 1;

// Column definition:
// YCSB Table:
const ColumnId YCSB_COLUMN_0 = 0;
const ColumnId YCSB_COLUMN_1 = 1;
const ColumnId YCSB_COLUMN_2 = 2;
const ColumnId YCSB_COLUMN_3 = 3;
const size_t YCSB_COLUMN_NUM = 4;
const size_t YCSB_COLUMN_SIZE = 40; 

const BenchTxnType kUpdate = 0;
const BenchTxnType kRead = 1;
const BenchTxnType kInsert = 2;

constexpr char ycsb_table_name[] = "YCSB";

// const int FREQUENCY_UPDATE = 100;
// const int FREQUENCY_READ = 0;
// const int FREQUENCY_INSERT = 0;
};  // namespace ycsb