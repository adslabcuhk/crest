// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <string>

#include "base/common.h"
#include "config/table_type.h"

const uint64_t MICRO_OP_RECORD_MAX_NUM = 12;

#define FREQUENCY_UPDATE_ONE 100
#define FREQUENCY_READ_ONE 00

// (kqh): This is a microbenchmark workload (YCSB)

union micro_key_t {
  uint64_t micro_id;
  uint64_t item_key;

  micro_key_t() { item_key = 0; }
};

static_assert(sizeof(micro_key_t) == sizeof(uint64_t), "");

enum micro_val_bitmap : int {
  d1 = 0,
  d2,
  d3,
  d4,
  d5,
};

struct micro_val_t {
  // 40 bytes, consistent with FaSST
  // uint64_t d1;
  // uint64_t d2;
  // uint64_t d3;
  // uint64_t d4;
  // uint64_t d5;
  char d1[58];
  char d2[58];
  char d3[58];
  char d4[58];
} __attribute__((packed));

constexpr size_t micro_val_t_size = sizeof(micro_val_t);

// static_assert(sizeof(micro_val_t) == 40, "");

// Magic numbers for debugging. These are unused in the spec.
#define Micro_MAGIC 10 /* Some magic number <= 255 */
#define micro_magic (Micro_MAGIC)

// Helpers for generating workload
#define MICRO_TX_TYPES 2

enum MicroTxType : int { kUpdateOne, kReadOne };

// #define MICRO_TX_TYPES 1
// enum MicroTxType : int {
//   kRWOne,
// };

// enum MicroTxType : int {
//   kTxTest1 = 1,
//   kTxTest2,
//   kTxTest3,
//   kTxTest4,
//   kTxTest5,
//   kTxTest6,
//   kTxTest7,
//   kTxTest8,
//   kTxTest9,
//   kTxTest10,
//   kTxTest11,
//   kTxTest12,
//   kTxTest100,
//   kTxTest101,
//   kRWOne,
// };

// const std::string MICRO_TX_NAME[MICRO_TX_TYPES] = {"RWOne"};

const std::string MICRO_TX_NAME[MICRO_TX_TYPES] = {"RWUpdateOne", "RWReadOne"};

struct RWUpdateOneParam {
  uint64_t num_key;
  uint64_t keys[MICRO_OP_RECORD_MAX_NUM];
  uint64_t cols[MICRO_OP_RECORD_MAX_NUM];
};

struct RWReadOneParam {
  uint64_t num_key;
  uint64_t keys[MICRO_OP_RECORD_MAX_NUM];
};

// Table id
enum MicroTableType : uint64_t {
  kMicroTable0 = TABLE_MICRO,
  kMicroTable1,
};
const int MICRO_TOTAL_TABLES = 2;
