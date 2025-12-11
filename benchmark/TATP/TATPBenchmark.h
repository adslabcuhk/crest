#include <infiniband/verbs_exp.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ostream>

#include "Base/BenchTypes.h"
#include "Base/Benchmark.h"
#include "Generator.h"
#include "TATP/TATPConstant.h"
#include "TATP/TATPContext.h"
#include "TATP/TATPTxnStructs.h"
#include "common/Type.h"
#include "db/Db.h"
#include "mempool/BufferManager.h"
#include "mempool/Coroutine.h"
#include "util/Macros.h"
#include "util/Statistic.h"

#define TATP_TRANSACTION_NAMES \
    X(GET_SUBSCRIBER)          \
    X(GET_NEW_DESTINATION)     \
    X(GET_ACCESS_DATA)         \
    X(UPDATE_SUBSCRIBER_DATA)  \
    X(UPDATE_LOCATION)         \
    X(INSERT_CALL_FORWARDING)  \
    X(DELETE_CALL_FORWARDING)

enum TATPBenchHistType {
#define X(name)                                                                    \
    TATP_##name##_HIST_MARKER, TATP_##name##_LAT, TATP_##name##_DEREF_LAT,         \
        TATP_##name##_PREPARE_LAT, TATP_##name##_READ_LAT, TATP_##name##_EXEC_LAT, \
        TATP_##name##_VALIDATION_LAT, TATP_##name##_COMMIT_LAT,                    \
        TATP_##name##_CHECK_BATCHREAD_RESULTS_LAT, TATP_##name##_WAIT_BATCHREAD_DATA_LAT,

    TATP_TRANSACTION_NAMES
#undef X
};

enum TATPBenchTickerType {
// Amalgamate Transaction
#define X(name)                                                                        \
    TATP_##name##_TICKER_MARKER, TATP_##name##_ABORT_CNT, TATP_##name##_FAIL_LOCK_CNT, \
        TATP_##name##_FAIL_INSERT_CNT, TATP_##name##_READ_COLUMN_LOCKED_CNT,

    TATP_TRANSACTION_NAMES
#undef X
};

static const std::map<uint32_t, std::string> tatp_bench_hist = {
#define X(name)                                                                                    \
    {TATP_##name##_HIST_MARKER, "tatp." #name ".marker"},                                          \
        {TATP_##name##_LAT, "tatp." #name ".lat"},                                                 \
        {TATP_##name##_DEREF_LAT, "tatp." #name ".deref.lat"},                                     \
        {TATP_##name##_PREPARE_LAT, "tatp." #name ".prepare.lat"},                                 \
        {TATP_##name##_READ_LAT, "tatp." #name ".read.lat"},                                       \
        {TATP_##name##_EXEC_LAT, "tatp." #name ".exec.lat"},                                       \
        {TATP_##name##_VALIDATION_LAT, "tatp." #name ".validation.lat"},                           \
        {TATP_##name##_COMMIT_LAT, "tatp." #name ".commit.lat"},                                   \
        {TATP_##name##_CHECK_BATCHREAD_RESULTS_LAT, "tatp." #name ".check_batchread_results.lat"}, \
        {TATP_##name##_WAIT_BATCHREAD_DATA_LAT, "tatp." #name ".wait_batchread_data.lat"},
    TATP_TRANSACTION_NAMES
#undef X
};

static const std::map<uint32_t, std::string> tatp_bench_ticker = {
#define X(name)                                                        \
    {TATP_##name##_TICKER_MARKER, "tatp." #name ".marker"},            \
        {TATP_##name##_ABORT_CNT, "tatp." #name ".abort"},             \
        {TATP_##name##_FAIL_LOCK_CNT, "tatp." #name ".fail_lock"},     \
        {TATP_##name##_FAIL_INSERT_CNT, "tatp." #name ".fail_insert"}, \
        {TATP_##name##_READ_COLUMN_LOCKED_CNT, "tatp." #name ".read_column_locked"},

    TATP_TRANSACTION_NAMES
#undef X
};

#define X(name)                                                                                    \
    ALWAYS_INLINE                                                                                  \
    void UpdateTATP_##name##_Stats(Txn* txn, Statistics* stats, bool committed, uint64_t u) {      \
        const auto& phase_latency = txn->GetTxnPhaseLatency();                                     \
        const auto& txn_detail = txn->GetTxnDetail();                                              \
        if (committed) {                                                                           \
            stats->RecordHistory(TATP_##name##_LAT, u);                                            \
            stats->RecordHistory(                                                                  \
                TATP_##name##_PREPARE_LAT,                                                         \
                phase_latency.dura[static_cast<int>(Txn::TxnPhase::PREPARE_PHASE)]);               \
            stats->RecordHistory(TATP_##name##_READ_LAT,                                           \
                                 phase_latency.dura[static_cast<int>(Txn::TxnPhase::READ_PHASE)]); \
            stats->RecordHistory(                                                                  \
                TATP_##name##_EXEC_LAT,                                                            \
                phase_latency.dura[static_cast<int>(Txn::TxnPhase::EXECUTION_PHASE)]);             \
            stats->RecordHistory(                                                                  \
                TATP_##name##_VALIDATION_LAT,                                                      \
                phase_latency.dura[static_cast<int>(Txn::TxnPhase::VALIDATION_PHASE)]);            \
            stats->RecordHistory(                                                                  \
                TATP_##name##_COMMIT_LAT,                                                          \
                phase_latency.dura[static_cast<int>(Txn::TxnPhase::COMMIT_PHASE)]);                \
            stats->RecordHistory(TATP_##name##_CHECK_BATCHREAD_RESULTS_LAT,                        \
                                 txn_detail.check_batchread_results_time);                         \
            stats->RecordHistory(TATP_##name##_WAIT_BATCHREAD_DATA_LAT,                            \
                                 txn_detail.wait_batchread_data_dura);                             \
        } else {                                                                                   \
            stats->RecordTicker(TATP_##name##_ABORT_CNT, 1);                                       \
        }                                                                                          \
        if (!committed) {                                                                          \
            stats->RecordTicker(TATP_##name##_FAIL_LOCK_CNT, txn_detail.lock_fail_count);          \
            stats->RecordTicker(TATP_##name##_FAIL_INSERT_CNT, txn_detail.insert_fail_count);      \
            stats->RecordTicker(TATP_##name##_READ_COLUMN_LOCKED_CNT,                              \
                                txn_detail.read_column_locked_count);                              \
        }                                                                                          \
    }
TATP_TRANSACTION_NAMES
#undef X

class TATPBenchmark : public Benchmark {
    static Db* CreateDB(const TATPConfig& config);

    Status InitDatabaseMeta(ThreadCtx* t_ctx, char* db_meta_page, bool primary_log);

   public:
    TATPBenchmark() = default;

    Status Initialize(const BenchmarkConfig& config) override;

    struct TATPTxnExecHistory {
        BenchTxnType type;
        TxnParam* params;
        TxnResult* results;
        uint32_t lat;    // execute latency in microseconds
        bool committed;  // if this transaction is committed

        TATPTxnExecHistory(BenchTxnType type, TATPContext* tatp_ctx, bool replay)
            : type(type), committed(false) {
            switch (type) {
                case tatp::kGetSubscriber: {
                    params = new tatp::GetSubscriberTxnParam(tatp_ctx);
                    results = replay ? new tatp::GetSubscriberTxnResult() : nullptr;
                    return;
                }
                case tatp::kGetNewDestination: {
                    params = new tatp::GetNewDestinationTxnParam(tatp_ctx);
                    results = replay ? new tatp::GetNewDestinationTxnResult() : nullptr;
                    return;
                }
                case tatp::kGetAccessData: {
                    params = new tatp::GetAccessDataTxnParam(tatp_ctx);
                    results = replay ? new tatp::GetAccessDataTxnResult() : nullptr;
                    return;
                }
                case tatp::kUpdateSubscriberData: {
                    params = new tatp::UpdateSubscriberDataTxnParam(tatp_ctx);
                    results = replay ? new tatp::UpdateSubscriberDataTxnResult() : nullptr;
                    return;
                }

                case tatp::kUpdateLocation: {
                    params = new tatp::UpdateLocationTxnParam(tatp_ctx);
                    results = replay ? new tatp::UpdateLocationTxnResult() : nullptr;
                    return;
                }

                case tatp::kInsertCallForwarding: {
                    params = new tatp::InsertCallForwardingTxnParam(tatp_ctx);
                    results = replay ? new tatp::InsertCallForwardingTxnResult() : nullptr;
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

    static void BenchmarkThread(ThreadCtx* t_ctx, TATPBenchmark* bench);

    static void BenchCoroutine(coro_yield_t& yield, coro_id_t coro_id, ThreadCtx* t_ctx,
                               std::vector<TATPTxnExecHistory>* batch);

   private:
    void PrepareTxnTypeArrays();

    void GenerateGetSubscriberParams(ThreadId tid, tatp::GetSubscriberTxnParam* param);

    void GenerateGetNewDestinationParams(ThreadId tid, tatp::GetNewDestinationTxnParam* param);

    void GenerateGetAccessDataParams(ThreadId tid, tatp::GetAccessDataTxnParam* param);

    void GenerateUpdateSubscriberDataParams(ThreadId tid,
                                            tatp::UpdateSubscriberDataTxnParam* param);

    void GenerateUpdateLocationParams(ThreadId tid, tatp::UpdateLocationTxnParam* param);

    void GenerateInsertCallForwardingParams(ThreadId tid,
                                            tatp::InsertCallForwardingTxnParam* param);

    BenchTxnType GenerateTxnType(ThreadId tid) {
        int d = FastRand(&txn_type_gen_[tid]) % 100;
        return txn_type_arrays_[d];
    }

    /* Get a subscriber number from a subscriber ID, simple */
    ALWAYS_INLINE
    tatp::tatp_sub_number SimpleGetSubscribeNumFromSubscribeID(uint32_t s_id) {
#define update_sid()           \
    do {                       \
        s_id = s_id / 10;      \
        if (s_id == 0) {       \
            return sub_number; \
        }                      \
    } while (false)

        tatp::tatp_sub_number sub_number;
        // sub_number.item_key = 0; /* Zero out all digits */
        std::memset(&sub_number, 0, sizeof(tatp::tatp_sub_number));

        sub_number.dec_0 = s_id % 10;
        update_sid();

        sub_number.dec_1 = s_id % 10;
        update_sid();

        sub_number.dec_2 = s_id % 10;
        update_sid();

        sub_number.dec_3 = s_id % 10;
        update_sid();

        sub_number.dec_4 = s_id % 10;
        update_sid();

        sub_number.dec_5 = s_id % 10;
        update_sid();

        sub_number.dec_6 = s_id % 10;
        update_sid();

        sub_number.dec_7 = s_id % 10;
        update_sid();

        sub_number.dec_8 = s_id % 10;
        update_sid();

        sub_number.dec_9 = s_id % 10;
        update_sid();

        sub_number.dec_10 = s_id % 10;
        update_sid();

        assert(s_id == 0);
        return sub_number;
    }

   private:
    bool PopulateDatabaseRecords(BufferManager* buf_manager, Db* db, bool deterministic = false);

    bool PopulateSubscriberRecords(BufferManager* buf_manager, Db* db, bool deterministic = false);

    bool PopulateAccessInfoRecords(BufferManager* buf_manager, Db* db, bool deterministic = false);

    bool PopulateSecondarySubscriberRecords(BufferManager* buf_manager, Db* db,
                                            bool deterministic = false);

    bool Populate_SpecialFacility_And_CallForwardingRecords(BufferManager* buf_manager, Db* db,
                                                            bool deterministic = false);
    uint32_t GetNonUniformSubscriberId(ThreadId tid) {
        (void)tid;
        FastRandom& fast_random = central_randoms_;
        return ((fast_random.NextU32() % tatp_config_.num_subscriber) |
                (fast_random.NextU32() & tatp_config_.A)) %
               tatp_config_.num_subscriber;
    }

   private:
    TATPConfig tatp_config_;
    TATPContext* tatp_ctx_;

    Db* bench_db_;

    std::vector<TATPTxnExecHistory> exec_hist_[Benchmark::kMaxThreadNum];

    FastRandom fast_randoms_[Benchmark::kMaxThreadNum];
    FastRandom central_randoms_;
    uint64_t txn_type_gen_[Benchmark::kMaxThreadNum];
    BenchTxnType txn_type_arrays_[100];
    uint64_t seq_num_;
};