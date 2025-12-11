#pragma once

#include <infiniband/verbs_exp.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ostream>

#include "Base/BenchConfig.h"
#include "Base/BenchTypes.h"
#include "Base/Benchmark.h"
#include "Generator.h"
#include "TPCC/TpccConstant.h"
#include "TPCC/TpccContext.h"
#include "TPCC/TpccTxnStructs.h"
#include "common/Type.h"
#include "db/Db.h"
#include "mempool/BufferManager.h"
#include "mempool/Coroutine.h"
#include "util/Macros.h"
#include "util/Statistic.h"

#define TPCC_TRANSACTION_NAMES \
  X(NEWORDER)                  \
  X(PAYMENT)                   \
  X(DELIVER)                   \
  X(STOCK_LEVEL)               \
  X(ORDER_STATUS)

enum TPCCBenchHistType {
#define X(name)                                                                                    \
  TPCC_##name##_HIST_MARKER, TPCC_##name##_LAT,                            \
      TPCC_##name##_PREPARE_LAT, TPCC_##name##_READ_LAT, TPCC_##name##_EXEC_LAT,                   \
      TPCC_##name##_VALIDATION_LAT, TPCC_##name##_COMMIT_LAT,                                      \
      TPCC_##name##_CHECK_BATCHREAD_RESULTS_LAT, TPCC_##name##_WAIT_BATCHREAD_DATA_LAT,            \
      TPCC_##name##_WAIT_DEPENDENT_TXN_LAT,                                                        
      // TPCC_##name##_ALLOC_BUFFER_SIZE,                       \
      // TPCC_##name##_DEPENDENT_TXN_CNT, TPCC_##name##_RDMA_READ_CNT, TPCC_##name##_RDMA_READ_BYTES, \
      // TPCC_##name##_RDMA_WRITE_CNT, TPCC_##name##_RDMA_WRITE_BYTES, TPCC_##name##_RDMA_ATOMIC_CNT, \
      // TPCC_##name##_RDMA_ATOMIC_BYTES,

  TPCC_TRANSACTION_NAMES
#undef X
};

enum TPCCBenchTickerType {
// Amalgamate Transaction
#define X(name)                                                                                  \
  TPCC_##name##_TICKER_MARKER, TPCC_##name##_ABORT_CNT, TPCC_##name##_FAIL_LOCK_CNT,             \
      TPCC_##name##_FAIL_INSERT_CNT, TPCC_##name##_READ_COLUMN_LOCKED_CNT,                       \
      TPCC_##name##_FAIL_JOIN_BATCH_CNT, TPCC_##name##_RELEASE_LOCK_CNT, TPCC_##name##_LOCK_CNT, \
      TPCC_##name##_LOCAL_SHARED_RECORD_CNT, TPCC_##name##_RECORD_HANDLE_SEARCH_CNT,             \
      TPCC_##name##_RECORD_HANDLE_HIT_CNT, TPCC_##name##_READ_FROM_CACHE_CNT,                    \
      TPCC_##name##_READ_FROM_REMOTE_CNT,

  TPCC_TRANSACTION_NAMES
#undef X
};

static const std::map<uint32_t, std::string> tpcc_bench_hist = {
#define X(name)                                                                                    \
  {TPCC_##name##_HIST_MARKER, "tpcc." #name ".marker"}, {TPCC_##name##_LAT, "tpcc." #name ".lat"}, \
      {TPCC_##name##_PREPARE_LAT, "tpcc." #name ".prepare.lat"},                                   \
      {TPCC_##name##_READ_LAT, "tpcc." #name ".read.lat"},                                         \
      {TPCC_##name##_EXEC_LAT, "tpcc." #name ".exec.lat"},                                         \
      {TPCC_##name##_VALIDATION_LAT, "tpcc." #name ".validation.lat"},                             \
      {TPCC_##name##_COMMIT_LAT, "tpcc." #name ".commit.lat"},                                     \
      {TPCC_##name##_CHECK_BATCHREAD_RESULTS_LAT, "tpcc." #name ".check_batchread_results.lat"},   \
      {TPCC_##name##_WAIT_BATCHREAD_DATA_LAT, "tpcc." #name ".wait_batchread_data.lat"},           \
      {TPCC_##name##_WAIT_DEPENDENT_TXN_LAT, "tpcc." #name ".wait_dependent_txn.lat"},             \
      // {TPCC_##name##_ALLOC_BUFFER_SIZE, "tpcc." #name ".alloc_buffer_size"},                       \
      // {TPCC_##name##_DEPENDENT_TXN_CNT, "tpcc." #name ".dependent_txn.cnt"},                        
      // {TPCC_##name##_RDMA_READ_CNT, "tpcc." #name ".rdma.read.cnt"},                               \
      // {TPCC_##name##_RDMA_READ_BYTES, "tpcc." #name ".rdma.read.bytes"},                           \
      // {TPCC_##name##_RDMA_WRITE_CNT, "tpcc." #name ".rdma.write.cnt"},                             \
      // {TPCC_##name##_RDMA_WRITE_BYTES, "tpcc." #name ".rdma.write.bytes"},                         \
      // {TPCC_##name##_RDMA_ATOMIC_CNT, "tpcc." #name ".rdma.atomic.cnt"},                           \
      // {TPCC_##name##_RDMA_ATOMIC_BYTES, "tpcc." #name ".rdma.atomic.bytes"},
    TPCC_TRANSACTION_NAMES
#undef X
};

static const std::map<uint32_t, std::string> tpcc_bench_ticker = {
#define X(name)                                                                            \
  {TPCC_##name##_TICKER_MARKER, "tpcc." #name ".marker"},                                  \
      {TPCC_##name##_ABORT_CNT, "tpcc." #name ".abort"},                                   \
      {TPCC_##name##_FAIL_LOCK_CNT, "tpcc." #name ".fail_lock"},                           \
      {TPCC_##name##_FAIL_INSERT_CNT, "tpcc." #name ".fail_insert"},                       \
      {TPCC_##name##_READ_COLUMN_LOCKED_CNT, "tpcc." #name ".read_column_locked"},         \
      {TPCC_##name##_FAIL_JOIN_BATCH_CNT, "tpcc." #name ".fail_join_batch"},               \
      {TPCC_##name##_RELEASE_LOCK_CNT, "tpcc." #name ".release_lock"},                     \
      {TPCC_##name##_LOCK_CNT, "tpcc." #name ".lock"},                                     \
      {TPCC_##name##_LOCAL_SHARED_RECORD_CNT, "tpcc." #name ".local_shared.cnt"},          \
      {TPCC_##name##_RECORD_HANDLE_SEARCH_CNT, "tpcc." #name ".record_handle_search.cnt"}, \
      {TPCC_##name##_RECORD_HANDLE_HIT_CNT, "tpcc." #name ".record_handle_hit.cnt"},       \
      {TPCC_##name##_READ_FROM_CACHE_CNT, "tpcc." #name ".read_from_cache.cnt"},           \
      {TPCC_##name##_READ_FROM_REMOTE_CNT, "tpcc." #name ".read_from_remote.cnt"},

    TPCC_TRANSACTION_NAMES
#undef X
};

#define X(name)                                                                                   \
  ALWAYS_INLINE                                                                                   \
  void UpdateTPCC_##name##_Stats(Txn* txn, Statistics* stats, bool committed, uint64_t u) {       \
    const auto& phase_latency = txn->GetTxnPhaseLatency();                                        \
    const auto& txn_detail = txn->GetTxnDetail();                                                 \
    const auto& rdma_stats = txn->GetRdmaAccessStats();                                           \
    if (committed) {                                                                              \
      stats->RecordHistory(TPCC_##name##_LAT, u);                                                 \
      if (BenchConfig::ENABLE_DETAILED_PERF) {                                                    \
        stats->RecordHistory(TPCC_##name##_PREPARE_LAT,                                           \
                             phase_latency.dura[static_cast<int>(Txn::TxnPhase::PREPARE_PHASE)]); \
        stats->RecordHistory(TPCC_##name##_READ_LAT,                                              \
                             phase_latency.dura[static_cast<int>(Txn::TxnPhase::READ_PHASE)]);    \
        stats->RecordHistory(                                                                     \
            TPCC_##name##_EXEC_LAT,                                                               \
            phase_latency.dura[static_cast<int>(Txn::TxnPhase::EXECUTION_PHASE)]);                \
        stats->RecordHistory(                                                                     \
            TPCC_##name##_VALIDATION_LAT,                                                         \
            phase_latency.dura[static_cast<int>(Txn::TxnPhase::VALIDATION_PHASE)]);               \
        stats->RecordHistory(TPCC_##name##_COMMIT_LAT,                                            \
                             phase_latency.dura[static_cast<int>(Txn::TxnPhase::COMMIT_PHASE)]);  \
        stats->RecordHistory(TPCC_##name##_CHECK_BATCHREAD_RESULTS_LAT,                           \
                             txn_detail.check_batchread_results_time);                            \
        stats->RecordHistory(TPCC_##name##_WAIT_BATCHREAD_DATA_LAT,                               \
                             txn_detail.wait_batchread_data_dura);                                \
        stats->RecordHistory(TPCC_##name##_WAIT_DEPENDENT_TXN_LAT,                                \
                             txn_detail.wait_dependent_txn_dura);                                 \
        stats->RecordTicker(TPCC_##name##_LOCK_CNT, txn_detail.lock_cnt);                         \
        stats->RecordTicker(TPCC_##name##_LOCAL_SHARED_RECORD_CNT,                                \
                            txn_detail.local_shared_record_num);                                  \
        stats->RecordTicker(TPCC_##name##_RECORD_HANDLE_SEARCH_CNT,                               \
                            txn_detail.record_handle_search_count);                               \
        stats->RecordTicker(TPCC_##name##_RECORD_HANDLE_HIT_CNT,                                  \
                            txn_detail.record_handle_hit_count);                                  \
        stats->RecordTicker(TPCC_##name##_READ_FROM_CACHE_CNT, txn_detail.read_from_cache_count); \
        stats->RecordTicker(TPCC_##name##_READ_FROM_REMOTE_CNT,                                   \
                            txn_detail.read_from_remote_count);                                   \
      }                                                                                           \
    } else {                                                                                      \
      stats->RecordTicker(TPCC_##name##_ABORT_CNT, 1);                                            \
    }                                                                                             \
    if (!committed) {                                                                             \
      stats->RecordTicker(TPCC_##name##_FAIL_LOCK_CNT, txn_detail.lock_fail_count);               \
      stats->RecordTicker(TPCC_##name##_FAIL_INSERT_CNT, txn_detail.insert_fail_count);           \
      stats->RecordTicker(TPCC_##name##_READ_COLUMN_LOCKED_CNT,                                   \
                          txn_detail.read_column_locked_count);                                   \
      stats->RecordTicker(TPCC_##name##_FAIL_JOIN_BATCH_CNT, txn_detail.join_batch_fail_count);   \
    }                                                                                             \
  }
TPCC_TRANSACTION_NAMES
#undef X

class TpccBenchmark : public Benchmark {
  static Db* CreateDB(const TpccConfig& config);

  Status InitDatabaseMeta(ThreadCtx* t_ctx, char* db_meta_page, bool primary_log);

 public:
  TpccBenchmark() = default;

  ~TpccBenchmark() override {
    if (tpcc_ctx_ != nullptr) {
      delete tpcc_ctx_;
    }
    if (bench_db_ != nullptr) {
      delete bench_db_;
    }
  }

  Status Initialize(const BenchmarkConfig& config) override;

  struct TpccTxnExecHistory {
    BenchTxnType type;
    TxnParam* params;
    TxnResult* results;
    uint32_t lat;    // execute latency in microseconds
    bool committed;  // if this transaction is committed

    TpccTxnExecHistory(BenchTxnType type, TpccContext* tpcc_ctx, bool replay)
        : type(type), committed(false) {
      switch (type) {
        case tpcc::kNewOrder: {
          params = new tpcc::NewOrderTxnParam(tpcc_ctx);
          results = replay ? new tpcc::NewOrderTxnResult() : nullptr;
          return;
        }
        case tpcc::kPayment: {
          params = new tpcc::PaymentTxnParam(tpcc_ctx);
          results = replay ? new tpcc::PaymentTxnResult() : nullptr;
          return;
        }
        case tpcc::kOrderStatus: {
          params = new tpcc::OrderStatusTxnParam(tpcc_ctx);
          results = replay ? new tpcc::OrderStatusTxnResult() : nullptr;
          return;
        }
        case tpcc::kDelivery: {
          params = new tpcc::DeliveryTxnParam(tpcc_ctx);
          results = replay ? new tpcc::DeliveryTxnResult() : nullptr;
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

  void DumpExecutionHist(std::ofstream& of, const TpccTxnExecHistory* exec_record,
                         TxnResult* ref_result);

 private:
  void GenerateNewOrderParams(ThreadId tid, tpcc::NewOrderTxnParam*);

  void GeneratePaymentParams(ThreadId tid, tpcc::PaymentTxnParam*);

  void GenerateOrderStatusParams(ThreadId tid, tpcc::OrderStatusTxnParam*);

  void GenerateDeliveryParams(ThreadId tid, tpcc::DeliveryTxnParam*);

  BenchTxnType GenerateTxnType(ThreadId tid);

  static void BenchCoroutine(coro_yield_t& yield, coro_id_t coro_id, ThreadCtx* t_ctx,
                             std::vector<TpccTxnExecHistory>* batch);

  // Popuate the database results into specific place
  bool PopulateDatabaseRecords(BufferManager* buf_manager, Db* db, bool deterministic = false);

  bool PopulateWarehouseRecords(BufferManager* buf_manager, Db* db, bool deterministic);

  bool PopulateDistrictRecords(BufferManager* buf_manager, Db* db, bool deterministic);

  bool PopulateCustomerRecords(BufferManager* buf_manager, Db* db, bool deterministic);

  bool PopulateHistoryRecords(BufferManager* buf_manager, Db* db, bool deterministic);

  bool PopulateNewOrder_Order_OrderLineTable(BufferManager* buf_manager, Db* db,
                                             bool deterministic);

  bool PopulateItemRecords(BufferManager* buf_manager, Db* db, bool deterministic);

  bool PopulateStockRecords(BufferManager* buf_manager, Db* db, bool deterministic);

  static void BenchmarkThread(ThreadCtx* t_ctx, TpccBenchmark* bench);

  void InitDatabaseMetaPage();

  bool IsCN() const { return config_.node_init_attr.node_type == kCN; }

  bool IsMN() const { return config_.node_init_attr.node_type == kMN; }

  void PrepareTxnTypeArrays();

 private:
  // Replayer related functions
  bool TxnNewOrderReplay(Db* db, tpcc::NewOrderTxnParam* param, tpcc::NewOrderTxnResult* result);

  bool TxnPaymentReplay(Db* db, tpcc::PaymentTxnParam* param, tpcc::PaymentTxnResult* result);

  bool TxnOrderStatusReplay(Db* db, tpcc::OrderStatusTxnParam* param,
                            tpcc::OrderStatusTxnResult* result);

 private:
  TpccConfig tpcc_config_;
  TpccContext* tpcc_ctx_;
  std::vector<TpccTxnExecHistory> exec_hist_[Benchmark::kMaxThreadNum];

  Db* bench_db_;  // For MN loads only

  FastRandom fast_randoms_[Benchmark::kMaxThreadNum];
  uint64_t txn_type_gen_[Benchmark::kMaxThreadNum];
  BenchTxnType txn_type_arrays_[100];

 private:
  static constexpr double DETERMINISTIC_TAX_VALUE = 0.24575;

  std::string DETERMINISTIC_CUSTOMER_DATA = "DETERMINISTIC_CUSTOMER_DATA";

  std::string DETERMINISTIC_CUSTOMER_FIRST = "C_FIRST";

  std::string DETERMINISTIC_CUSTOMER_MIDDLE = "AB";

  std::string DETERMINISTIC_CUSTOMER_LAST = "C_LAST";

  std::string DETERMINISTIC_HISTORY_DATA = "HISTORY_DATA";

  std::string DETERMINISTIC_STOCK_DATA = "STOCK_DATA";

  static constexpr double DETERMINISTIC_DISCOUNT_VALUE = 0.182912;

  static constexpr double DETERMINISTIC_ITEM_PRICE = 2.0;

  std::string DETERMINISTIC_ITEM_NAME = "ITEM_NAME";

  std::string DETERMINISTIC_ITEM_DATA = "DETERMINISTIC_ITEM_DATA";
};
