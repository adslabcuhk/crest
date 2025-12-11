#pragma once

#include "common/Type.h"

using BenchTxnType = uint32_t;  // Type (or name) of executed transaction

class TxnParam {
 public:
  TxnParam(BenchTxnType txn_type) : txn_type_(txn_type) {}
  TxnParam(const TxnParam&) = default;
  TxnParam& operator=(const TxnParam&) = default;

  BenchTxnType GetTxnType() const { return txn_type_; }

  virtual char* Serialize(char* dst) const = 0;

  virtual const char* Deserialize(const char* src) = 0;

 protected:
  BenchTxnType txn_type_;
};

class TxnResult {
 public:
  TxnResult(BenchTxnType txn_type) : txn_type_(txn_type) {}

  BenchTxnType GetTxnType() const { return txn_type_; }

  virtual char* Serialize(char* dst) const = 0;

  virtual const char* Deserialize(const char* src) = 0;

  timestamp_t GetCommitTimestamp() const { return commit_ts_; }

  void SetCommitTimestamp(timestamp_t commit_ts) { commit_ts_ = commit_ts; }

  TxnId GetTxnId() const { return txn_id_; }

  void SetTxnId(TxnId txn_id) { txn_id_ = txn_id; }

 public:
  size_t cnt;
  TableId dependent_table_ids_[16];
  uint32_t wait_dependent_txn_time_;

 protected:
  BenchTxnType txn_type_;
  timestamp_t commit_ts_;
  TxnId txn_id_;
};