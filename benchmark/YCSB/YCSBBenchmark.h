#include <infiniband/verbs_exp.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ostream>

#include "Base/BenchTypes.h"
#include "Base/Benchmark.h"
#include "Generator.h"
#include "YCSB/YCSBConstant.h"
#include "YCSB/YCSBContext.h"
#include "YCSB/YCSBTxnStructs.h"
#include "common/Type.h"
#include "db/Db.h"
#include "mempool/BufferManager.h"
#include "mempool/Coroutine.h"
#include "util/Macros.h"
#include "util/Statistic.h"

#define YCSB_TRANSACTION_NAMES \
    X(UPDATE)                  \
    X(READ)                    \
    X(INSERT)

enum YCSBBenchHistType {
#define X(name)                                                                    \
    YCSB_##name##_HIST_MARKER, YCSB_##name##_LAT, YCSB_##name##_DEREF_LAT,         \
        YCSB_##name##_PREPARE_LAT, YCSB_##name##_READ_LAT, YCSB_##name##_EXEC_LAT, \
        YCSB_##name##_VALIDATION_LAT, YCSB_##name##_COMMIT_LAT,                    \
        YCSB_##name##_CHECK_BATCHREAD_RESULTS_LAT, YCSB_##name##_WAIT_BATCHREAD_DATA_LAT,

    YCSB_TRANSACTION_NAMES
#undef X
};

enum YCSBBenchTickerType {
// Amalgamate Transaction
#define X(name)                                                                        \
    YCSB_##name##_TICKER_MARKER, YCSB_##name##_ABORT_CNT, YCSB_##name##_FAIL_LOCK_CNT, \
        YCSB_##name##_FAIL_INSERT_CNT, YCSB_##name##_READ_COLUMN_LOCKED_CNT,

    YCSB_TRANSACTION_NAMES
#undef X
};

static const std::map<uint32_t, std::string> ycsb_bench_hist = {
#define X(name)                                                                                    \
    {YCSB_##name##_HIST_MARKER, "ycsb." #name ".marker"},                                          \
        {YCSB_##name##_LAT, "ycsb." #name ".lat"},                                                 \
        {YCSB_##name##_DEREF_LAT, "ycsb." #name ".deref.lat"},                                     \
        {YCSB_##name##_PREPARE_LAT, "ycsb." #name ".prepare.lat"},                                 \
        {YCSB_##name##_READ_LAT, "ycsb." #name ".read.lat"},                                       \
        {YCSB_##name##_EXEC_LAT, "ycsb." #name ".exec.lat"},                                       \
        {YCSB_##name##_VALIDATION_LAT, "ycsb." #name ".validation.lat"},                           \
        {YCSB_##name##_COMMIT_LAT, "ycsb." #name ".commit.lat"},                                   \
        {YCSB_##name##_CHECK_BATCHREAD_RESULTS_LAT, "ycsb." #name ".check_batchread_results.lat"}, \
        {YCSB_##name##_WAIT_BATCHREAD_DATA_LAT, "ycsb." #name ".wait_batchread_data.lat"},
    YCSB_TRANSACTION_NAMES
#undef X
};

static const std::map<uint32_t, std::string> ycsb_bench_ticker = {
#define X(name)                                                        \
    {YCSB_##name##_TICKER_MARKER, "ycsb." #name ".marker"},            \
        {YCSB_##name##_ABORT_CNT, "ycsb." #name ".abort"},             \
        {YCSB_##name##_FAIL_LOCK_CNT, "ycsb." #name ".fail_lock"},     \
        {YCSB_##name##_FAIL_INSERT_CNT, "ycsb." #name ".fail_insert"}, \
        {YCSB_##name##_READ_COLUMN_LOCKED_CNT, "ycsb." #name ".read_column_locked"},

    YCSB_TRANSACTION_NAMES
#undef X
};

#define X(name)                                                                                    \
    ALWAYS_INLINE                                                                                  \
    void UpdateYCSB_##name##_Stats(Txn* txn, Statistics* stats, bool committed, uint64_t u) {      \
        const auto& phase_latency = txn->GetTxnPhaseLatency();                                     \
        const auto& txn_detail = txn->GetTxnDetail();                                              \
        if (committed) {                                                                           \
            stats->RecordHistory(YCSB_##name##_LAT, u);                                            \
            stats->RecordHistory(                                                                  \
                YCSB_##name##_PREPARE_LAT,                                                         \
                phase_latency.dura[static_cast<int>(Txn::TxnPhase::PREPARE_PHASE)]);               \
            stats->RecordHistory(YCSB_##name##_READ_LAT,                                           \
                                 phase_latency.dura[static_cast<int>(Txn::TxnPhase::READ_PHASE)]); \
            stats->RecordHistory(                                                                  \
                YCSB_##name##_EXEC_LAT,                                                            \
                phase_latency.dura[static_cast<int>(Txn::TxnPhase::EXECUTION_PHASE)]);             \
            stats->RecordHistory(                                                                  \
                YCSB_##name##_VALIDATION_LAT,                                                      \
                phase_latency.dura[static_cast<int>(Txn::TxnPhase::VALIDATION_PHASE)]);            \
            stats->RecordHistory(                                                                  \
                YCSB_##name##_COMMIT_LAT,                                                          \
                phase_latency.dura[static_cast<int>(Txn::TxnPhase::COMMIT_PHASE)]);                \
            stats->RecordHistory(YCSB_##name##_CHECK_BATCHREAD_RESULTS_LAT,                        \
                                 txn_detail.check_batchread_results_time);                         \
            stats->RecordHistory(YCSB_##name##_WAIT_BATCHREAD_DATA_LAT,                            \
                                 txn_detail.wait_batchread_data_dura);                             \
        } else {                                                                                   \
            stats->RecordTicker(YCSB_##name##_ABORT_CNT, 1);                                       \
        }                                                                                          \
        if (!committed) {                                                                          \
            stats->RecordTicker(YCSB_##name##_FAIL_LOCK_CNT, txn_detail.lock_fail_count);          \
            stats->RecordTicker(YCSB_##name##_FAIL_INSERT_CNT, txn_detail.insert_fail_count);      \
            stats->RecordTicker(YCSB_##name##_READ_COLUMN_LOCKED_CNT,                              \
                                txn_detail.read_column_locked_count);                              \
        }                                                                                          \
    }
YCSB_TRANSACTION_NAMES
#undef X

class YCSBBenchmark : public Benchmark {
    static Db* CreateDB(const YCSBConfig& config);

    Status InitDatabaseMeta(ThreadCtx* t_ctx, char* db_meta_page, bool primary_log);

   public:
    YCSBBenchmark() = default;

    Status Initialize(const BenchmarkConfig& config) override;

    struct YCSBTxnExecHistory {
        BenchTxnType type;
        TxnParam* params;
        TxnResult* results;
        uint32_t lat;    // execute latency in microseconds
        bool committed;  // if this transaction is committed

        YCSBTxnExecHistory(BenchTxnType type, YCSBContext* tpcc_ctx, bool replay)
            : type(type), committed(false) {
            switch (type) {
                case ycsb::kUpdate: {
                    params = new ycsb::UpdateTxnParam();
                    results = replay ? new ycsb::UpdateTxnResult() : nullptr;
                    return;
                }
                case ycsb::kRead: {
                    params = new ycsb::ReadTxnParam();
                    results = replay ? new ycsb::ReadTxnResult() : nullptr;
                    return;
                }
                case ycsb::kInsert: {
                    params = new ycsb::InsertTxnParam();
                    results = replay ? new ycsb::InsertTxnResult() : nullptr;
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

    static void BenchmarkThread(ThreadCtx* t_ctx, YCSBBenchmark* bench);

    static void BenchCoroutine(coro_yield_t& yield, coro_id_t coro_id, ThreadCtx* t_ctx,
                               std::vector<YCSBTxnExecHistory>* batch);

   private:
    void PrepareTxnTypeArrays();

    void GenerateUpdateParams(ThreadId tid, ycsb::UpdateTxnParam* param);

    void GenearteReadParams(ThreadId tid, ycsb::ReadTxnParam* param);

    void GenerateInsertParams(ThreadId tid, ycsb::InsertTxnParam* param); 

    int64_t GenerateAccountsId(ThreadId tid) { return ycsb_ctx_->GenerateRecordKey(); }

    BenchTxnType GenerateTxnType(ThreadId tid) {
        int d = FastRand(&txn_type_gen_[tid]) % 100;
        return txn_type_arrays_[d];
    }

   private:
    bool PopulateDatabaseRecords(BufferManager* buf_manager, Db* db, bool deterministic = false);

    bool PopulateYCSBRecords(BufferManager* buf_manager, Db* db, int table_id, bool deterministic = false);

    uint64_t NextSeq() { return ++seq_num_; }

   private:
    YCSBConfig ycsb_config_;
    YCSBContext* ycsb_ctx_;

    Db* bench_db_;

    std::vector<YCSBTxnExecHistory> exec_hist_[Benchmark::kMaxThreadNum * 3];

    FastRandom fast_randoms_[Benchmark::kMaxThreadNum * 3];
    uint64_t txn_type_gen_[Benchmark::kMaxThreadNum * 3];
    BenchTxnType txn_type_arrays_[100];
    uint64_t seq_num_;
};