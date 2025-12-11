#include "Base/BenchConfig.h"
#include "Base/BenchTypes.h"
#include "Base/Benchmark.h"
#include "SmallBank/SmallBankBenchmark.h"
#include "SmallBank/SmallBankConstant.h"
#include "SmallBank/SmallBankContext.h"
#include "SmallBank/SmallBankTxnImpl.h"
#include "SmallBank/SmallBankTxnStructs.h"
#include "common/Type.h"
#include "transaction/TimestampGen.h"
#include "util/Timer.h"

using namespace smallbank;

static const std::vector<int> numa_nodes[2] = {
    // NUMA Node 0 in our machine: 0 ~ 23, 48 ~ 71
    {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15,
     16, 17, 18, 19, 20, 21, 22, 23, 48, 49, 50, 51, 52, 53, 54, 55,
     56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71},

    // NUMA Node 1 in our machine:
    {24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
     40, 41, 42, 43, 44, 45, 46, 47, 72, 73, 74, 75, 76, 77, 78, 79,
     80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95}};

void SmallBankBenchmark::PrepareTxnTypeArrays() {
    int base = 0;
    for (int i = 0; i < FREQUENCY_AMALGAMATE; ++i) {
        txn_type_arrays_[i + base] = kAmalgamate;
    }
    base += FREQUENCY_AMALGAMATE;

    for (int i = 0; i < FREQUENCY_WRITE_CHECK; ++i) {
        txn_type_arrays_[i + base] = kWriteCheck;
    }
    base += FREQUENCY_WRITE_CHECK;

    for (int i = 0; i < FREQUENCY_TRANSACT_SAVINGS; ++i) {
        txn_type_arrays_[i + base] = kTransactSaving;
    }
    base += FREQUENCY_TRANSACT_SAVINGS;

    for (int i = 0; i < FREQUENCY_SEND_PAYMENT; ++i) {
        txn_type_arrays_[i + base] = kSendPayment;
    }
    base += FREQUENCY_SEND_PAYMENT;

    for (int i = 0; i < FREQUENCY_DEPOSIT_CHECKING; ++i) {
        txn_type_arrays_[i + base] = kDepositChecking;
    }
    base += FREQUENCY_DEPOSIT_CHECKING;

    for (int i = 0; i < FREQUENCY_BALANCE; ++i) {
        txn_type_arrays_[i + base] = kBalance;
    }
    base += FREQUENCY_BALANCE;
}

Status SmallBankBenchmark::Initialize(const BenchmarkConfig& config) {
    util::Timer timer;
    Status s = Benchmark::Initialize(config);
    ASSERT(s.ok(), "Benchmark Base Initialization failed");

    smallbank_config_ = *(SmallBankConfig*)(config.workload_config);
    smallbank_ctx_ = new SmallBankContext(smallbank_config_);
    bench_db_ = CreateDB(smallbank_config_);

    auto mr_token = this->pool_->GetLocalMemoryRegionToken();
    char* mr_addr = (char*)(mr_token.get_region_addr());
    size_t mr_sz = mr_token.get_region_size();

    if (IsMN()) {
        size_t meta_area_size = 4096;
        size_t log_num = config.node_init_attr.num_cns * config.thread_num * config.coro_num;
        size_t log_area_size = 2 * LogManager::PER_COODINATOR_LOG_SIZE * log_num;

        char* meta_area_addr = mr_addr;
        char* log_area_addr = meta_area_addr + meta_area_size;
        char* table_area_addr = log_area_addr + log_area_size;
        size_t table_area_size = mr_sz - meta_area_size - log_area_size;

        LOG_INFO("MN Log Area addr: %p, size: %lu MiB", log_area_addr, (log_area_size >> 20));

        // Populate all tables
        BufferManager bm(table_area_addr, table_area_size);
        PopulateDatabaseRecords(&bm, bench_db_, config_.replay);
        LOG_INFO("MN Populdate tables done");

        // Write the metadata of the database to the first page
        char* meta_page = meta_area_addr;
        PoolPtr log_address = MakePoolPtr(MyNodeId(), (uint64_t)log_area_addr);

        // Write the address information of each log manager
        WriteNext<PoolPtr>(meta_page, log_address);

        // Write the address information of each database table:
        meta_page = bench_db_->GetTable(ACCOUNTS_TABLE)->SerializeTableInfo(meta_page);
        meta_page = bench_db_->GetTable(SAVINGS_TABLE)->SerializeTableInfo(meta_page);
        meta_page = bench_db_->GetTable(CHECKING_TABLE)->SerializeTableInfo(meta_page);
        LOG_INFO("MN write database metadata done");
        LOG_INFO("MN Initialization Takes %.2lf ms", timer.ms_elapse());

        Status s = this->pool_->BuildConnection(config_.pool_attr, config_.thread_num);
        ASSERT(s.ok(), "MN%d build connection failed", this->pool_->GetNodeId());

    } else if (IsCN()) {
        TimestampGenerator* ts_gen = new TimestampGeneratorImpl(1);

        // Read the metadata page of remote databases, this metadata page is shared among
        // all benchmark threads, so we only need to read it once
        int num_mns = config_.node_init_attr.num_mns;
        std::vector<char*> db_meta_pages;
        for (int id = 0; id < num_mns; ++id) {
            db_meta_pages.push_back(id * 4096 + mr_addr);
            rdma::QueuePair* qp = thread_ctxs_[0].pool->GetQueuePair(id);
            RequestToken token;
            Status s = qp->Read((void*)(qp->GetRemoteMemoryRegionToken().get_region_addr()), 4096,
                                db_meta_pages.back(), &token);
            ASSERT(s.ok(), "fetch database metadata failed");
        }

        rdma::QueuePair* qp = thread_ctxs_[0].pool->GetQueuePair(0);
        RequestToken token;
        uint64_t remote_mr_addr = qp->GetRemoteMemoryRegionToken().get_region_addr();

        // Allocate the local memory region:
        BufferAllocationParam param = ParseAllocationParam(config_, mr_token);
        size_t thread_buffer_size = param.data_buffer_size / config_.thread_num;

        for (int tid = 0; tid < config.thread_num; ++tid) {
            ThreadCtx* t_ctx = &thread_ctxs_[tid];
            ThreadId g_tid = t_ctx->global_tid;

            // Initialize transaction related contexts
            t_ctx->ts_generator = ts_gen;
            t_ctx->address_cache = new AddressCache();
            t_ctx->config = config;
            t_ctx->db = CreateDB(smallbank_config_);
            t_ctx->workload_ctx = smallbank_ctx_;
            t_ctx->record_handle_db->set_db(bench_db_);

            // Initialize the Local Buffer Manager for each thread
            BufferManager* bm = new BufferManager(param.data_buffer_base + thread_buffer_size * tid,
                                                  thread_buffer_size);
            t_ctx->buffer_manager = bm;

            char* thread_log_buffer_base = param.log_buffer_base + param.log_size_per_thread * tid;

            for (coro_id_t cid = 0; cid < config.coro_num; ++cid) {
                t_ctx->txn_log[cid] = new LogManager(thread_log_buffer_base +
                                                     cid * LogManager::PER_COODINATOR_LOG_SIZE * 2);
            }

            // Initialize the database metadata for each thread
            for (node_id_t nid = 0; nid < num_mns; ++nid) {
                bool primary_log_node = (nid == 0);
                Status s = InitDatabaseMeta(t_ctx, db_meta_pages[nid], primary_log_node);
                ASSERT(s.ok(), "Init database metadata failed");
            }

            // Setup the Random Generator
            if (BenchConfig::USE_RANDOM_SEED) {
                // Generate the seed from the timer
                auto t = time(nullptr);
                char d[16];
                *(uint64_t*)d = t;
                *(uint64_t*)(d + 8) = t_ctx->global_tid;
                uint64_t seed = util::Hash(d, 16);
                fast_randoms_[tid] = FastRandom(seed);
            } else {
                fast_randoms_[tid] = FastRandom(t_ctx->global_tid);
            }

            // Register the TPCC-benchmark related statistics
            ASSERT(t_ctx->stats != nullptr, "");
            for (const auto& [h, n] : smallbank_bench_hist) {
                t_ctx->stats->RegisterHist(h, n);
            }
            for (const auto& [t, n] : smallbank_bench_ticker) {
                t_ctx->stats->RegisterTicker(t, n);
            }

            // Initialize global status
            t_ctx->tried_txn_count = &g_tried_txn_count[tid][0];
            t_ctx->committed_txn_count = &g_committed_txn_count[tid][0];
            t_ctx->tried_txn_thpt = &g_tried_txn_thpt[tid];
            t_ctx->committed_txn_thpt = &g_committed_txn_thpt[tid];
            t_ctx->committed_txn_lat = g_txn_lat[tid];
        }
        LOG_INFO("CN initialize benchmark metadata done");
    }
    return Status::OK();
}

Status SmallBankBenchmark::InitDatabaseMeta(ThreadCtx* t_ctx, char* db_meta_page,
                                            bool primary_log) {
    const char* p = db_meta_page;
    PoolPtr log_area = Next<PoolPtr>(p);
    p = t_ctx->db->GetTable(smallbank::ACCOUNTS_TABLE)->DeserializeTableInfo(p);
    p = t_ctx->db->GetTable(smallbank::SAVINGS_TABLE)->DeserializeTableInfo(p);
    p = t_ctx->db->GetTable(smallbank::CHECKING_TABLE)->DeserializeTableInfo(p);

    PoolPtr thread_log_addr_base =
        buf_allocate_param_.log_size_per_thread * t_ctx->global_tid + log_area;

    // Set the primary or backup address for all coroutines of this thread context:
    for (coro_id_t cid = 0; cid < config_.coro_num; ++cid) {
        LogManager* log_manager = t_ctx->txn_log[cid];
        PoolPtr coro_log_addr =
            thread_log_addr_base + cid * LogManager::PER_COODINATOR_LOG_SIZE * 2;
        if (primary_log) {
            log_manager->SetPrimaryAddress(coro_log_addr);
        } else {
            log_manager->AddBackupAddress(coro_log_addr);
        }
    }

    return Status::OK();
}

void SmallBankBenchmark::PrepareWorkloads() {
    if (IsMN()) {
        return;
    }
    util::Timer timer;

    PrepareTxnTypeArrays();

    for (int i = 0; i < config_.txn_num * config_.NumOfCNs(); ++i) {
        ThreadId tid = i % (config_.thread_num * config_.NumOfCNs());
        BenchTxnType tx_type = GenerateTxnType(tid);
        switch (tx_type) {
            case kAmalgamate: {
                exec_hist_[tid].emplace_back(kAmalgamate, smallbank_ctx_, config_.replay);
                GenerateAmalagateParams(tid, (AmalgamateParam*)exec_hist_[tid].back().params);
                break;
            }
            case kWriteCheck: {
                exec_hist_[tid].emplace_back(kWriteCheck, smallbank_ctx_, config_.replay);
                GenearteWriteCheckParams(tid, (WriteCheckParam*)exec_hist_[tid].back().params);
                break;
            }
            case kTransactSaving: {
                exec_hist_[tid].emplace_back(kTransactSaving, smallbank_ctx_, config_.replay);
                GenerateTransactSavingParams(tid,
                                             (TransactSavingParam*)exec_hist_[tid].back().params);
                break;
            }
            case kSendPayment: {
                exec_hist_[tid].emplace_back(kSendPayment, smallbank_ctx_, config_.replay);
                GenerateSendPaymentParams(tid, (SendPaymentParam*)exec_hist_[tid].back().params);
                break;
            }
            case kDepositChecking: {
                exec_hist_[tid].emplace_back(kDepositChecking, smallbank_ctx_, config_.replay);
                GenerateDepositCheckingParams(tid,
                                              (DepositCheckingParam*)exec_hist_[tid].back().params);
                break;
            }
            case kBalance: {
                exec_hist_[tid].emplace_back(kBalance, smallbank_ctx_, config_.replay);
                GenerateBalanceParams(tid, (BalanceParam*)exec_hist_[tid].back().params);
                break;
            }
            default:
                break;
        }
    }

    LOG_INFO("[Prepare Workload Done][Elaps: %.2lf ms][Thread: %d ][TxnNum: %d ]\n",
             timer.ms_elapse(), config_.thread_num, config_.txn_num);
}

void SmallBankBenchmark::BenchmarkThread(ThreadCtx* t_ctx, SmallBankBenchmark* bench) {
    auto tid = t_ctx->local_tid;
    // Bind this thread to the specific CPU core
    // Use numactl to bind both the CPU cores and memory allocation

    // Initialize the coroutine scheduler
    auto sched = t_ctx->pool->GetCoroSched();
    for (coro_id_t cid = 0; cid < t_ctx->config.coro_num; ++cid) {
        if (cid == 0) {
            // The first coroutine is always used for polling the RDMA request completion flag
            sched->GetCoroutine(cid)->func =
                coro_call_t(std::bind(Poll, std::placeholders::_1, t_ctx));
        } else {
            auto g_tid = t_ctx->global_tid;
            sched->GetCoroutine(cid)->func = coro_call_t(std::bind(
                BenchCoroutine, std::placeholders::_1, cid, t_ctx, &(bench->exec_hist_[g_tid])));
        }
    }
    Timer timer;
    // Start running the coroutine function
    sched->GetCoroutine(0)->func();
    uint64_t dura = timer.us_elapse();
    *(t_ctx->tried_txn_thpt) = (double)(*(t_ctx->tried_txn_count)) / dura;
    *(t_ctx->committed_txn_thpt) = (double)(*(t_ctx->committed_txn_count)) / dura;
}

void SmallBankBenchmark::BenchCoroutine(coro_yield_t& yield, coro_id_t coro_id, ThreadCtx* t_ctx,
                                        std::vector<SmallBankTxnExecHistory>* batch) {
    Timer timer;
    FastRandom txn_generator((t_ctx->local_tid << 32 | coro_id));
    Statistics* stats = t_ctx->stats;
    Txn* txn = new Txn(0, t_ctx->local_tid, coro_id, TxnType::READ_WRITE, t_ctx->pool, t_ctx->db,
                       t_ctx->buffer_manager, t_ctx->ts_generator, t_ctx->record_handle_db,
                       t_ctx->txn_log[coro_id]);
    int executed = 0;
    int txn_exec_idx = coro_id;
    int coro_num = t_ctx->config.coro_num;
    bool tx_committed = false;
    bool ret_txn_results = t_ctx->config.replay;
    ThreadId tid = t_ctx->local_tid;

    int txn_exec_num = t_ctx->config.txn_num / t_ctx->config.thread_num;

    int exec_count = 0;

    uint64_t seq_num = 0;

    Timer latency_timer;

    int db_txn_id = 0;

    uint64_t exec_latency = 0, validate_latency = 0, commit_latency = 0;

    while (!t_ctx->signal_stop.load() && txn_exec_idx < txn_exec_num) {
        SmallBankTxnExecHistory& exec = batch->at(txn_exec_idx);
        txn_exec_idx += (t_ctx->config.coro_num - 1);
        TxnId txn_id = TransactionId(t_ctx->global_tid, coro_id, seq_num);
        ++seq_num;
        timer.reset();
        tx_committed = false;
        exec_count = 0;
        latency_timer.reset();
        exec_latency = 0;
        validate_latency = 0;
        commit_latency = 0;

        switch (exec.type) {
            case kAmalgamate: {
                AmalgamateParam* params = (AmalgamateParam*)exec.params;
                AmalgamateResult* results = (AmalgamateResult*)exec.results;
                while (!tx_committed && exec_count++ < BenchConfig::SMALLBANK_MAX_EXEC_COUNT) {
                    tx_committed =
                        TxnAmalgamate(txn, txn_id, params, results, yield, ret_txn_results);
                    exec_latency += (txn->GetPhaseLatency(Txn::TxnPhase::READ_PHASE));
                    exec_latency += (txn->GetPhaseLatency(Txn::TxnPhase::ABORT_PHASE));
                    validate_latency += (txn->GetPhaseLatency(Txn::TxnPhase::VALIDATION_PHASE));
                    commit_latency += (txn->GetPhaseLatency(Txn::TxnPhase::COMMIT_PHASE));
                    ++(*t_ctx->tried_txn_count);
                    uint64_t u = timer.ns_elapse();
                    // It's okay to record the stats even if the transaction is not committed,
                    // the txn latency would be recorded only if the transaction is committed.
                    UpdateSmallBank_AMALGAMATE_Stats(txn, stats, tx_committed, u);
                }
                if (tx_committed) {
                    int id = *t_ctx->committed_txn_count;
                    t_ctx->txn_latency[id] = latency_timer.us_elapse();
                    t_ctx->txn_exec_latency[id] = exec_latency / 1000;
                    t_ctx->txn_validate_latency[id] = validate_latency / 1000;
                    t_ctx->txn_commit_latency[id] = commit_latency / 1000;
                    ++(*t_ctx->committed_txn_count);
                }
                uint64_t u = timer.ns_elapse();
                UpdateSmallBank_AMALGAMATE_Stats(txn, stats, tx_committed, u);
                exec.committed = tx_committed;
                break;
            }
            case kWriteCheck: {
                WriteCheckParam* params = (WriteCheckParam*)exec.params;
                WriteCheckResult* results = (WriteCheckResult*)exec.results;
                while (!tx_committed && exec_count++ < BenchConfig::SMALLBANK_MAX_EXEC_COUNT) {
                    tx_committed =
                        TxnWriteCheck(txn, txn_id, params, results, yield, ret_txn_results);
                    exec_latency += (txn->GetPhaseLatency(Txn::TxnPhase::READ_PHASE));
                    exec_latency += (txn->GetPhaseLatency(Txn::TxnPhase::ABORT_PHASE));
                    validate_latency += (txn->GetPhaseLatency(Txn::TxnPhase::VALIDATION_PHASE));
                    commit_latency += (txn->GetPhaseLatency(Txn::TxnPhase::COMMIT_PHASE));
                    ++(*t_ctx->tried_txn_count);
                    uint64_t u = timer.ns_elapse();
                    UpdateSmallBank_WRITE_CHECK_Stats(txn, stats, tx_committed, u);
                }
                // LOG_DEBUG("T%lu C%d finish WriteCheck: %d\n", t_ctx->local_tid, coro_id,
                //           txn_exec_idx);
                if (tx_committed) {
                    int id = *t_ctx->committed_txn_count;
                    t_ctx->txn_latency[id] = latency_timer.us_elapse();
                    t_ctx->txn_exec_latency[id] = exec_latency / 1000;
                    t_ctx->txn_validate_latency[id] = validate_latency / 1000;
                    t_ctx->txn_commit_latency[id] = commit_latency / 1000;
                    ++(*(t_ctx->committed_txn_count));
                }
                uint64_t u = timer.ns_elapse();
                UpdateSmallBank_WRITE_CHECK_Stats(txn, stats, tx_committed, u);
                exec.committed = tx_committed;
                break;
            }
            case kTransactSaving: {
                TransactSavingParam* params = (TransactSavingParam*)exec.params;
                TransactSavingResult* results = (TransactSavingResult*)exec.results;
                while (!tx_committed && exec_count++ < BenchConfig::SMALLBANK_MAX_EXEC_COUNT) {
                    tx_committed =
                        TxnTransactSaving(txn, txn_id, params, results, yield, ret_txn_results);
                    exec_latency += (txn->GetPhaseLatency(Txn::TxnPhase::READ_PHASE));
                    exec_latency += (txn->GetPhaseLatency(Txn::TxnPhase::ABORT_PHASE));
                    validate_latency += (txn->GetPhaseLatency(Txn::TxnPhase::VALIDATION_PHASE));
                    commit_latency += (txn->GetPhaseLatency(Txn::TxnPhase::COMMIT_PHASE));
                    ++(*t_ctx->tried_txn_count);
                    uint64_t u = timer.ns_elapse();
                    UpdateSmallBank_TRANSACT_SAVINGS_Stats(txn, stats, tx_committed, u);
                }
                if (tx_committed) {
                    int id = *t_ctx->committed_txn_count;
                    t_ctx->txn_latency[id] = latency_timer.us_elapse();
                    t_ctx->txn_exec_latency[id] = exec_latency / 1000;
                    t_ctx->txn_validate_latency[id] = validate_latency / 1000;
                    t_ctx->txn_commit_latency[id] = commit_latency / 1000;
                    ++(*(t_ctx->committed_txn_count));
                }
                uint64_t u = timer.ns_elapse();
                UpdateSmallBank_TRANSACT_SAVINGS_Stats(txn, stats, tx_committed, u);
                exec.committed = tx_committed;
                break;
            }
            case kSendPayment: {
                SendPaymentParam* params = (SendPaymentParam*)exec.params;
                SendPaymentResult* results = (SendPaymentResult*)exec.results;
                while (!tx_committed && exec_count++ < BenchConfig::SMALLBANK_MAX_EXEC_COUNT) {
                    tx_committed =
                        TxnSendPayment(txn, txn_id, params, results, yield, ret_txn_results);
                    exec_latency += (txn->GetPhaseLatency(Txn::TxnPhase::READ_PHASE));
                    exec_latency += (txn->GetPhaseLatency(Txn::TxnPhase::ABORT_PHASE));
                    validate_latency += (txn->GetPhaseLatency(Txn::TxnPhase::VALIDATION_PHASE));
                    commit_latency += (txn->GetPhaseLatency(Txn::TxnPhase::COMMIT_PHASE));
                    ++(*t_ctx->tried_txn_count);
                    uint64_t u = timer.ns_elapse();
                    UpdateSmallBank_SEND_PAYMENT_Stats(txn, stats, tx_committed, u);
                }
                if (tx_committed) {
                    int id = *t_ctx->committed_txn_count;
                    t_ctx->txn_latency[id] = latency_timer.us_elapse();
                    t_ctx->txn_exec_latency[id] = exec_latency / 1000;
                    t_ctx->txn_validate_latency[id] = validate_latency / 1000;
                    t_ctx->txn_commit_latency[id] = commit_latency / 1000;
                    ++(*(t_ctx->committed_txn_count));
                }
                uint64_t u = timer.ns_elapse();
                UpdateSmallBank_SEND_PAYMENT_Stats(txn, stats, tx_committed, u);
                exec.committed = tx_committed;
                break;
            }
            case kDepositChecking: {
                DepositCheckingParam* params = (DepositCheckingParam*)exec.params;
                DepositCheckingResult* results = (DepositCheckingResult*)exec.results;
                while (!tx_committed && exec_count++ < BenchConfig::SMALLBANK_MAX_EXEC_COUNT) {
                    tx_committed =
                        TxnDepositChecking(txn, txn_id, params, results, yield, ret_txn_results);
                    exec_latency += (txn->GetPhaseLatency(Txn::TxnPhase::READ_PHASE));
                    exec_latency += (txn->GetPhaseLatency(Txn::TxnPhase::ABORT_PHASE));
                    validate_latency += (txn->GetPhaseLatency(Txn::TxnPhase::VALIDATION_PHASE));
                    commit_latency += (txn->GetPhaseLatency(Txn::TxnPhase::COMMIT_PHASE));
                    ++(*(t_ctx->tried_txn_count));
                    uint64_t u = timer.ns_elapse();
                    UpdateSmallBank_DEPOSIT_CHECKING_Stats(txn, stats, tx_committed, u);
                }
                if (tx_committed) {
                    int id = *t_ctx->committed_txn_count;
                    t_ctx->txn_latency[id] = latency_timer.us_elapse();
                    t_ctx->txn_exec_latency[id] = exec_latency / 1000;
                    t_ctx->txn_validate_latency[id] = validate_latency / 1000;
                    t_ctx->txn_commit_latency[id] = commit_latency / 1000;
                    ++(*(t_ctx->committed_txn_count));
                }
                uint64_t u = timer.ns_elapse();
                UpdateSmallBank_DEPOSIT_CHECKING_Stats(txn, stats, tx_committed, u);
                exec.committed = tx_committed;
                break;
            }
            case kBalance: {
                BalanceParam* params = (BalanceParam*)exec.params;
                BalanceResult* results = (BalanceResult*)exec.results;
                while (!tx_committed && exec_count++ < BenchConfig::SMALLBANK_MAX_EXEC_COUNT) {
                    tx_committed = TxnBalance(txn, txn_id, params, results, yield, ret_txn_results);
                    exec_latency += (txn->GetPhaseLatency(Txn::TxnPhase::READ_PHASE));
                    exec_latency += (txn->GetPhaseLatency(Txn::TxnPhase::ABORT_PHASE));
                    validate_latency += (txn->GetPhaseLatency(Txn::TxnPhase::VALIDATION_PHASE));
                    commit_latency += (txn->GetPhaseLatency(Txn::TxnPhase::COMMIT_PHASE));
                    ++(*(t_ctx->tried_txn_count));
                    uint64_t u = timer.ns_elapse();
                    UpdateSmallBank_BALANCE_Stats(txn, stats, tx_committed, u);
                }
                if (tx_committed) {
                    int id = *t_ctx->committed_txn_count;
                    t_ctx->txn_latency[id] = latency_timer.us_elapse();
                    t_ctx->txn_exec_latency[id] = exec_latency / 1000;
                    t_ctx->txn_validate_latency[id] = validate_latency / 1000;
                    t_ctx->txn_commit_latency[id] = commit_latency / 1000;
                    ++(*t_ctx->committed_txn_count);
                }
                uint64_t u = timer.ns_elapse();
                UpdateSmallBank_BALANCE_Stats(txn, stats, tx_committed, u);
                exec.committed = tx_committed;
                break;
            }
            default:
                break;
        }
    }
    // LOG_INFO("T%lu C%d Finish running\n", t_ctx->tid, coro_id);
    t_ctx->pool->YieldForFinish(coro_id, yield);
}

Status SmallBankBenchmark::Run() {
    if (IsMN()) {
        // For MN, just spin
        RunDebugMode(bench_db_);
    } else if (IsCN()) {
        memcached_->ConnectToMemcached();
        memcached_->AddServer();
        memcached_->SyncComputeNodes();
        for (int i = 0; i < config_.thread_num; ++i) {
            ThreadCtx* t_ctx = &thread_ctxs_[i];
            t_ctx->signal_stop.store(false);
            t_ctx->t = new std::thread(BenchmarkThread, t_ctx, this);
        }

        RunStatsReporter(config_.dura);

        for (int i = 0; i < config_.thread_num; ++i) {
            ThreadCtx* t_ctx = &thread_ctxs_[i];
            t_ctx->Join();
        }

        // Report the status
        ReportMergedThreadResults();
        if (config_.replay) {
            Replay();
        }
    }

    return Status::OK();
}
