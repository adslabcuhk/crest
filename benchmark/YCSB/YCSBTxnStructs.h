#pragma once

#include <cstdint>

#include "Base/BenchTypes.h"
#include "YCSB/YCSBConstant.h"
#include "YCSB/YCSBContext.h"
#include "YCSB/YCSBTableStructs.h"
#include "common/Type.h"

namespace ycsb {
class UpdateTxnParam : public TxnParam {
   public:
    UpdateTxnParam() : TxnParam(ycsb::kUpdate) {}

    char* Serialize(char* dst) const override { return dst; }

    const char* Deserialize(const char* src) override { return src; }

   public:
    int record_num;  // Number of records to update
    int64_t records_keys_[YCSB_OP_RECORD_MAX_NUM];
    int update_column_[YCSB_OP_RECORD_MAX_NUM];
    uint64_t seq;  // The data used for write
};

class UpdateTxnResult : public TxnResult {
   public:
    UpdateTxnResult() : TxnResult(ycsb::kUpdate) {}

    char* Serialize(char* dst) const override { return dst; }

    const char* Deserialize(const char* src) override { return src; }
};

class ReadTxnParam : public TxnParam {
   public:
    ReadTxnParam() : TxnParam(ycsb::kRead) {}

    char* Serialize(char* dst) const override { return dst; }

    const char* Deserialize(const char* src) override { return src; }

   public:
    int record_num;  // Number of records to read
    int64_t records_keys_[YCSB_OP_RECORD_MAX_NUM];
};

class ReadTxnResult : public TxnResult {
   public:
    ReadTxnResult() : TxnResult(ycsb::kRead) {}

    char* Serialize(char* dst) const override { return dst; }

    const char* Deserialize(const char* src) override { return src; }

   public:
    int record_num;
    ycsb_value_t values_[YCSB_OP_RECORD_MAX_NUM];
};

class InsertTxnParam : public TxnParam {
   public:
    InsertTxnParam() : TxnParam(ycsb::kInsert) {}

    char* Serialize(char* dst) const override { return dst; }

    const char* Deserialize(const char* src) override { return src; }

   public:
    int record_num;
    int64_t records_keys_[YCSB_OP_RECORD_MAX_NUM];  // Need to read the whole row
    ycsb_value_t values_[YCSB_OP_RECORD_MAX_NUM];   // Values for insertion
};

class InsertTxnResult : public TxnResult {
   public:
    InsertTxnResult() : TxnResult(ycsb::kInsert) {}

    char* Serialize(char* dst) const override { return dst; }

    const char* Deserialize(const char* src) override { return src; }
};

}  // namespace ycsb
