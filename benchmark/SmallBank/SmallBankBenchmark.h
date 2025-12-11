#pragma once

#include <infiniband/verbs_exp.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ostream>

#include "Base/BenchTypes.h"
#include "Base/Benchmark.h"
#include "Generator.h"
#include "SmallBank/SmallBankConstant.h"
#include "SmallBank/SmallBankContext.h"
#include "SmallBank/SmallBankTxnStructs.h"
#include "common/Type.h"
#include "db/Db.h"
#include "mempool/BufferManager.h"
#include "mempool/Coroutine.h"
#include "util/Macros.h"
#include "util/Statistic.h"

#define SMALLBANK_TRANSACTION_NAMES \
  X(AMALGAMATE)                     \
  X(WRITE_CHECK)                    \
  X(TRANSACT_SAVINGS)               \
  X(SEND_PAYMENT)                   \
  X(DEPOSIT_CHECKING)               \
  X(BALANCE)

enum SmallBankBenchHistType {
#define X(name)                                                                                 \
  SMALLBANK_##name##_HIST_MARKER, SMALLBANK_##name##_LAT, SMALLBANK_##name##_DEREF_LAT,         \
      SMALLBANK_##name##_PREPARE_LAT, SMALLBANK_##name##_READ_LAT, SMALLBANK_##name##_EXEC_LAT, \
      SMALLBANK_##name##_VALIDATION_LAT, SMALLBANK_##name##_COMMIT_LAT,                         \
      SMALLBANK_##name##_CHECK_BATCHREAD_RESULTS_LAT, SMALLBANK_##name##_WAIT_BATCHREAD_DATA_LAT,

  SMALLBANK_TRANSACTION_NAMES
#undef X
};

enum SmallBankBenchTickerType {
// Amalgamate Transaction
#define X(name)                                                             \
  SMALLBANK_##name##_TICKER_MARKER, SMALLBANK_##name##_ABORT_CNT,           \
      SMALLBANK_##name##_FAIL_LOCK_CNT, SMALLBANK_##name##_FAIL_INSERT_CNT, \
      SMALLBANK_##name##_READ_COLUMN_LOCKED_CNT,

  SMALLBANK_TRANSACTION_NAMES
#undef X
};

static const std::map<uint32_t, std::string> smallbank_bench_hist = {
#define X(name)                                                                  \
  {SMALLBANK_##name##_HIST_MARKER, "smallbank." #name ".marker"},                \
      {SMALLBANK_##name##_LAT, "smallbank." #name ".lat"},                       \
      {SMALLBANK_##name##_DEREF_LAT, "smallbank." #name ".deref.lat"},           \
      {SMALLBANK_##name##_PREPARE_LAT, "smallbank." #name ".prepare.lat"},       \
      {SMALLBANK_##name##_READ_LAT, "smallbank." #name ".read.lat"},             \
      {SMALLBANK_##name##_EXEC_LAT, "smallbank." #name ".exec.lat"},             \
      {SMALLBANK_##name##_VALIDATION_LAT, "smallbank." #name ".validation.lat"}, \
      {SMALLBANK_##name##_COMMIT_LAT, "smallbank." #name ".commit.lat"},         \
      {SMALLBANK_##name##_CHECK_BATCHREAD_RESULTS_LAT,                           \
       "smallbank." #name ".check_batchread_results.lat"},                       \
      {SMALLBANK_##name##_WAIT_BATCHREAD_DATA_LAT, "smallbank." #name ".wait_batchread_data.lat"},
    SMALLBANK_TRANSACTION_NAMES
#undef X
};

static const std::map<uint32_t, std::string> smallbank_bench_ticker = {
#define X(name)                                                                \
  {SMALLBANK_##name##_TICKER_MARKER, "smallbank." #name ".marker"},            \
      {SMALLBANK_##name##_ABORT_CNT, "smallbank." #name ".abort"},             \
      {SMALLBANK_##name##_FAIL_LOCK_CNT, "smallbank." #name ".fail_lock"},     \
      {SMALLBANK_##name##_FAIL_INSERT_CNT, "smallbank." #name ".fail_insert"}, \
      {SMALLBANK_##name##_READ_COLUMN_LOCKED_CNT, "smallbank." #name ".read_column_locked"},

    SMALLBANK_TRANSACTION_NAMES
#undef X
};

#define X(name)                                                                                    \
  ALWAYS_INLINE                                                                                    \
  void UpdateSmallBank_##name##_Stats(Txn* txn, Statistics* stats, bool committed, uint64_t u) {   \
    const auto& phase_latency = txn->GetTxnPhaseLatency();                                         \
    const auto& txn_detail = txn->GetTxnDetail();                                                  \
    if (committed) {                                                                               \
      stats->RecordHistory(SMALLBANK_##name##_LAT, u);                                             \
      stats->RecordHistory(SMALLBANK_##name##_PREPARE_LAT,                                         \
                           phase_latency.dura[static_cast<int>(Txn::TxnPhase::PREPARE_PHASE)]);    \
      stats->RecordHistory(SMALLBANK_##name##_READ_LAT,                                            \
                           phase_latency.dura[static_cast<int>(Txn::TxnPhase::READ_PHASE)]);       \
      stats->RecordHistory(SMALLBANK_##name##_EXEC_LAT,                                            \
                           phase_latency.dura[static_cast<int>(Txn::TxnPhase::EXECUTION_PHASE)]);  \
      stats->RecordHistory(SMALLBANK_##name##_VALIDATION_LAT,                                      \
                           phase_latency.dura[static_cast<int>(Txn::TxnPhase::VALIDATION_PHASE)]); \
      stats->RecordHistory(SMALLBANK_##name##_COMMIT_LAT,                                          \
                           phase_latency.dura[static_cast<int>(Txn::TxnPhase::COMMIT_PHASE)]);     \
      stats->RecordHistory(SMALLBANK_##name##_CHECK_BATCHREAD_RESULTS_LAT,                         \
                           txn_detail.check_batchread_results_time);                               \
      stats->RecordHistory(SMALLBANK_##name##_WAIT_BATCHREAD_DATA_LAT,                             \
                           txn_detail.wait_batchread_data_dura);                                   \
    } else {                                                                                       \
      stats->RecordTicker(SMALLBANK_##name##_ABORT_CNT, 1);                                        \
    }                                                                                              \
    if (!committed) {                                                                              \
      stats->RecordTicker(SMALLBANK_##name##_FAIL_LOCK_CNT, txn_detail.lock_fail_count);           \
      stats->RecordTicker(SMALLBANK_##name##_FAIL_INSERT_CNT, txn_detail.insert_fail_count);       \
      stats->RecordTicker(SMALLBANK_##name##_READ_COLUMN_LOCKED_CNT,                               \
                          txn_detail.read_column_locked_count);                                    \
    }                                                                                              \
  }
SMALLBANK_TRANSACTION_NAMES
#undef X

class SmallBankBenchmark : public Benchmark {
  static Db* CreateDB(const SmallBankConfig& config);

  Status InitDatabaseMeta(ThreadCtx* t_ctx, char* db_meta_page, bool primary_log);

 public:
  SmallBankBenchmark() = default;

  Status Initialize(const BenchmarkConfig& config) override;

  struct SmallBankTxnExecHistory {
    BenchTxnType type;
    TxnParam* params;
    TxnResult* results;
    uint32_t lat;    // execute latency in microseconds
    bool committed;  // if this transaction is committed

    SmallBankTxnExecHistory(BenchTxnType type, SmallBankContext* tpcc_ctx, bool replay)
        : type(type), committed(false) {
      switch (type) {
        case smallbank::kAmalgamate: {
          params = new smallbank::AmalgamateParam();
          results = replay ? new smallbank::AmalgamateResult() : nullptr;
          return;
        }
        case smallbank::kWriteCheck: {
          params = new smallbank::WriteCheckParam();
          results = replay ? new smallbank::WriteCheckResult() : nullptr;
          return;
        }
        case smallbank::kTransactSaving: {
          params = new smallbank::TransactSavingParam();
          results = replay ? new smallbank::TransactSavingResult() : nullptr;
          return;
        }
        case smallbank::kSendPayment: {
          params = new smallbank::SendPaymentParam();
          results = replay ? new smallbank::SendPaymentResult() : nullptr;
          return;
        }
        case smallbank::kDepositChecking: {
          params = new smallbank::DepositCheckingParam();
          results = replay ? new smallbank::DepositCheckingResult() : nullptr;
          return;
        }
        case smallbank::kBalance: {
          params = new smallbank::BalanceParam();
          results = replay ? new smallbank::BalanceResult() : nullptr;
          return;
        }
        default:
          return;
      }
    }
  };

 public:
  bool Replay();

  void PrepareWorkloads() override;

  virtual Status Run() override;

  static void BenchmarkThread(ThreadCtx* t_ctx, SmallBankBenchmark* bench);

  static void BenchCoroutine(coro_yield_t& yield, coro_id_t coro_id, ThreadCtx* t_ctx,
                             std::vector<SmallBankTxnExecHistory>* batch);

 private:
  void PrepareTxnTypeArrays();

  void GenerateAmalagateParams(ThreadId tid, smallbank::AmalgamateParam* param) {
    param->custid_0_ = GenerateAccountsId(tid);
    do {
      param->custid_1_ = GenerateAccountsId(tid);
    } while (param->custid_0_ == param->custid_1_);
    // sort the order:
    if (param->custid_1_ > param->custid_0_) {
      std::swap(param->custid_0_, param->custid_1_);
    }
  }

  void GenearteWriteCheckParams(ThreadId tid, smallbank::WriteCheckParam* param) {
    param->custid_ = GenerateAccountsId(tid);
    param->amount_ = 5.000;
  }

  void GenerateTransactSavingParams(ThreadId tid, smallbank::TransactSavingParam* param) {
    param->custid_ = GenerateAccountsId(tid);
  }

  void GenerateSendPaymentParams(ThreadId tid, smallbank::SendPaymentParam* param) {
    param->custid_0_ = GenerateAccountsId(tid);
    do {
      param->custid_1_ = GenerateAccountsId(tid);
    } while (param->custid_0_ == param->custid_1_);
    if (param->custid_1_ > param->custid_0_) {
      std::swap(param->custid_0_, param->custid_1_);
    }
  }

  void GenerateDepositCheckingParams(ThreadId tid, smallbank::DepositCheckingParam* param) {
    param->custid_ = GenerateAccountsId(tid);
    param->amount_ = 5.0;
  }

  void GenerateBalanceParams(ThreadId tid, smallbank::BalanceParam* param) {
    param->custid_ = GenerateAccountsId(tid);
  }

  int64_t GenerateAccountsId(ThreadId tid) { return smallbank_ctx_->GenerateAccountsId(); }

  BenchTxnType GenerateTxnType(ThreadId tid) {
    int d = FastRand(&txn_type_gen_[tid]) % 100;
    return txn_type_arrays_[d];
  }

 private:
  bool PopulateDatabaseRecords(BufferManager* buf_manager, Db* db, bool deterministic = false);

  bool PopulateAccountsTable(BufferManager* buf_manager, Db* db, bool deterministic);

  bool PopulateSavingsTable(BufferManager* buf_manager, Db* db, bool deterministic);

  bool PopulateCheckingsTable(BufferManager* buf_manager, Db* db, bool deterministic);

 private:
  SmallBankConfig smallbank_config_;
  SmallBankContext* smallbank_ctx_;

  Db* bench_db_;

  std::vector<SmallBankTxnExecHistory> exec_hist_[Benchmark::kMaxThreadNum * 3];

  FastRandom fast_randoms_[Benchmark::kMaxThreadNum * 3];
  uint64_t txn_type_gen_[Benchmark::kMaxThreadNum * 3];
  BenchTxnType txn_type_arrays_[100];
};