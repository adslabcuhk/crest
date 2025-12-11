#include <infiniband/verbs.h>

#include <algorithm>
#include <chrono>
#include <iostream>

#include "Base/BenchConfig.h"
#include "Base/BenchTypes.h"
#include "Generator.h"
#include "TATP/TATPBenchmark.h"
#include "TATP/TATPConstant.h"
#include "TATP/TATPTxnImpl.h"
#include "TATP/TATPTxnStructs.h"
#include "common/Type.h"
#include "db/AddressCache.h"
#include "mempool/BufferManager.h"
#include "mempool/Coroutine.h"
#include "rdma/QueuePair.h"
#include "transaction/Enums.h"
#include "transaction/TimestampGen.h"
#include "util/Hash.h"
#include "util/Logger.h"
#include "util/Macros.h"
#include "util/Timer.h"

Status TATPBenchmark::InitDatabaseMeta(ThreadCtx* t_ctx, char* db_meta_page, bool primary_log) {
    const char* p = db_meta_page;
    PoolPtr log_area = Next<PoolPtr>(p);
    p = t_ctx->db->GetTable(tatp::SUBSCRIBER_TABLE)->DeserializeTableInfo(p);
    p = t_ctx->db->GetTable(tatp::SECONDARY_SUBSCRIBER_TABLE)->DeserializeTableInfo(p);
    p = t_ctx->db->GetTable(tatp::ACCESSINFO_TABLE)->DeserializeTableInfo(p);
    p = t_ctx->db->GetTable(tatp::SPECIAL_FACILITY_TABLE)->DeserializeTableInfo(p);
    p = t_ctx->db->GetTable(tatp::CALL_FORWARDING_TABLE)->DeserializeTableInfo(p);

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

Status TATPBenchmark::Initialize(const BenchmarkConfig& config) {
    util::Timer timer;
    Status s = Benchmark::Initialize(config);
    ASSERT(s.ok(), "Benchmark Base initialization failed");

    tatp_config_ = *(TATPConfig*)(config.workload_config);
    tatp_ctx_ = new TATPContext(tatp_config_);
    bench_db_ = CreateDB(tatp_config_);

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

        // First write the address information of each log:
        WriteNext<PoolPtr>(meta_page, log_address);

        // Write the address information of each database table:
        meta_page = bench_db_->GetTable(tatp::SUBSCRIBER_TABLE)->SerializeTableInfo(meta_page);
        meta_page =
            bench_db_->GetTable(tatp::SECONDARY_SUBSCRIBER_TABLE)->SerializeTableInfo(meta_page);
        meta_page = bench_db_->GetTable(tatp::ACCESSINFO_TABLE)->SerializeTableInfo(meta_page);
        meta_page =
            bench_db_->GetTable(tatp::SPECIAL_FACILITY_TABLE)->SerializeTableInfo(meta_page);
        meta_page = bench_db_->GetTable(tatp::CALL_FORWARDING_TABLE)->SerializeTableInfo(meta_page);
        LOG_INFO("MN write database metadata done");
        LOG_INFO("MN Initialization Takes %.2lf ms", timer.ms_elapse());

        Status s = this->pool_->BuildConnection(config_.pool_attr, config_.thread_num);
        ASSERT(s.ok(), "MN%d build connection failed", this->pool_->GetNodeId());

        // Initialize the DbRestorer:
        this->restorer_ = new DbRestorer(this->pool_, this->bench_db_);
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
            t_ctx->db = CreateDB(tatp_config_);
            t_ctx->workload_ctx = tatp_ctx_;
            t_ctx->record_handle_db->set_db(bench_db_);

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
            // Register the TATP-benchmark related statistics
            ASSERT(t_ctx->stats != nullptr, "");
            for (const auto& [h, n] : tatp_bench_hist) {
                t_ctx->stats->RegisterHist(h, n);
            }
            for (const auto& [t, n] : tatp_bench_ticker) {
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

void TATPBenchmark::GenerateGetSubscriberParams(ThreadId tid,
                                                tatp::GetSubscriberTxnParam* gs_param) {
    gs_param->s_id_ = GetNonUniformSubscriberId(tid);
}

void TATPBenchmark::GenerateGetNewDestinationParams(ThreadId tid,
                                                    tatp::GetNewDestinationTxnParam* gnd_param) {
    gnd_param->s_id_ = GetNonUniformSubscriberId(tid);
    gnd_param->sf_type_ = fast_randoms_[tid].RandNumber(0, tatp::AI_TYPE_PER_SUBSCRIBER - 1);
    uint64_t start_time = fast_randoms_[tid].RandNumber(0, 2) * 8;
    uint64_t end_time = fast_randoms_[tid].RandNumber(0, 23);

    unsigned cf_to_fetch = (start_time / 8) + 1;
    ASSERT(cf_to_fetch <= 3, "Invalid cf_to_fetch");
    gnd_param->fetch_num_ = cf_to_fetch;
    gnd_param->start_time_ = start_time;
    gnd_param->end_time_ = end_time;
}

void TATPBenchmark::GenerateGetAccessDataParams(ThreadId tid,
                                                tatp::GetAccessDataTxnParam* gad_param) {
    gad_param->s_id_ = GetNonUniformSubscriberId(tid);
    gad_param->ai_type_ = fast_randoms_[tid].RandNumber(0, tatp::AI_TYPE_PER_SUBSCRIBER - 1);
}

void TATPBenchmark::GenerateUpdateSubscriberDataParams(
    ThreadId tid, tatp::UpdateSubscriberDataTxnParam* usd_param) {
    usd_param->s_id_ = GetNonUniformSubscriberId(tid);
    usd_param->sf_type_ = fast_randoms_[tid].RandNumber(0, tatp::SF_TYPE_PER_SUBSCRIBER - 1);
}

void TATPBenchmark::GenerateUpdateLocationParams(ThreadId tid,
                                                 tatp::UpdateLocationTxnParam* ul_param) {
    ul_param->s_id_ = GetNonUniformSubscriberId(tid);
}

void TATPBenchmark::GenerateInsertCallForwardingParams(
    ThreadId tid, tatp::InsertCallForwardingTxnParam* icf_param) {
    icf_param->s_id_ = GetNonUniformSubscriberId(tid);
    icf_param->sf_type_ = fast_randoms_[tid].RandNumber(0, tatp::SF_TYPE_PER_SUBSCRIBER - 1);

    // uint64_t start_time = fast_randoms_[tid].RandNumber(0, 2) * 8;
    uint64_t start_time = fast_randoms_[tid].RandNumber(0, 24);
    uint64_t end_time = fast_randoms_[tid].RandNumber(0, 24);
    icf_param->start_time_ = start_time;
    icf_param->end_time_ = end_time;
}

Status TATPBenchmark::Run() {
    if (IsMN()) {
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

void TATPBenchmark::BenchmarkThread(ThreadCtx* t_ctx, TATPBenchmark* bench) {
    auto tid = t_ctx->local_tid;

    // Initialize the coroutine scheduler
    auto sched = t_ctx->pool->GetCoroSched();
    for (coro_id_t cid = 0; cid < t_ctx->config.coro_num; ++cid) {
        if (cid == 0) {
            // The first coroutine is always used for polling the RDMA request completion flag
            sched->GetCoroutine(cid)->func =
                coro_call_t(std::bind(Poll, std::placeholders::_1, t_ctx));
        } else {
            sched->GetCoroutine(cid)->func = coro_call_t(std::bind(
                BenchCoroutine, std::placeholders::_1, cid, t_ctx, &(bench->exec_hist_[tid])));
        }
    }
    Timer timer;
    // Start running the coroutine function
    sched->GetCoroutine(0)->func();
    uint64_t dura = timer.us_elapse();
    *(t_ctx->tried_txn_thpt) = (double)(*(t_ctx->tried_txn_count)) / dura;
    *(t_ctx->committed_txn_thpt) = (double)(*(t_ctx->committed_txn_count)) / dura;
}

void TATPBenchmark::BenchCoroutine(coro_yield_t& yield, coro_id_t coro_id, ThreadCtx* t_ctx,
                                   std::vector<TATPTxnExecHistory>* batch) {
    Timer timer;
    FastRandom txn_generator((t_ctx->global_tid << 32 | coro_id));
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
    Timer latency_timer;

    uint64_t seq_num = 0;
    while (!t_ctx->signal_stop.load() && txn_exec_idx < txn_exec_num) {
        TATPTxnExecHistory& exec = batch->at(txn_exec_idx);
        txn_exec_idx += (t_ctx->config.coro_num - 1);
        TxnId txn_id = TransactionId(t_ctx->global_tid, coro_id, seq_num);
        ++seq_num;
        // printf("Debug: seq = %lu\n", seq_num);
        tx_committed = false;
        exec_count = 0;
        latency_timer.reset();

        switch (exec.type) {
            case tatp::kGetSubscriber: {
                timer.reset();
                tatp::GetSubscriberTxnParam* param = (tatp::GetSubscriberTxnParam*)exec.params;
                tatp::GetSubscriberTxnResult* results = (tatp::GetSubscriberTxnResult*)exec.results;
                while (!tx_committed && exec_count++ < BenchConfig::TATP_MAX_EXEC_COUNT) {
                    tx_committed = tatp::TxnGetSubscriberData(txn, txn_id, param, results, yield,
                                                              ret_txn_results);
                    ++(*t_ctx->tried_txn_count);
                    uint64_t u = timer.ns_elapse();
                    UpdateTATP_GET_SUBSCRIBER_Stats(txn, stats, tx_committed, u);
                }
                if (tx_committed) {
                    int id = *t_ctx->committed_txn_count;
                    t_ctx->txn_latency[id] = latency_timer.us_elapse();
                    ++(*t_ctx->committed_txn_count);
                }
                uint64_t u = timer.ns_elapse();
                UpdateTATP_GET_SUBSCRIBER_Stats(txn, stats, tx_committed, u);
                exec.committed = tx_committed;
                break;
            }
            case tatp::kGetNewDestination: {
                timer.reset();
                tatp::GetNewDestinationTxnParam* param =
                    (tatp::GetNewDestinationTxnParam*)exec.params;
                tatp::GetNewDestinationTxnResult* results =
                    (tatp::GetNewDestinationTxnResult*)exec.results;
                while (!tx_committed && exec_count++ < BenchConfig::TATP_MAX_EXEC_COUNT) {
                    tx_committed = tatp::TxnGetNewDestination(txn, txn_id, param, results, yield,
                                                              ret_txn_results);
                    ++(*t_ctx->tried_txn_count);
                    uint64_t u = timer.ns_elapse();
                    UpdateTATP_GET_NEW_DESTINATION_Stats(txn, stats, tx_committed, u);
                }
                if (tx_committed) {
                    int id = *t_ctx->committed_txn_count;
                    t_ctx->txn_latency[id] = latency_timer.us_elapse();
                    ++(*(t_ctx->committed_txn_count));
                }
                uint64_t u = timer.ns_elapse();
                UpdateTATP_GET_NEW_DESTINATION_Stats(txn, stats, tx_committed, u);
                exec.committed = tx_committed;
                break;
            }
            case tatp::kGetAccessData: {
                timer.reset();
                tatp::GetAccessDataTxnParam* param = (tatp::GetAccessDataTxnParam*)exec.params;
                tatp::GetAccessDataTxnResult* results = (tatp::GetAccessDataTxnResult*)exec.results;
                while (!tx_committed && exec_count++ < BenchConfig::TATP_MAX_EXEC_COUNT) {
                    tx_committed =
                        tatp::TxnGetAccessData(txn, txn_id, param, results, yield, ret_txn_results);
                    ++(*t_ctx->tried_txn_count);
                    uint64_t u = timer.ns_elapse();
                    UpdateTATP_GET_ACCESS_DATA_Stats(txn, stats, tx_committed, u);
                }
                if (tx_committed) {
                    int id = *t_ctx->committed_txn_count;
                    t_ctx->txn_latency[id] = latency_timer.us_elapse();
                    ++(*(t_ctx->committed_txn_count));
                }
                uint64_t u = timer.ns_elapse();
                UpdateTATP_GET_ACCESS_DATA_Stats(txn, stats, tx_committed, u);
                exec.committed = tx_committed;
                break;
            }
            case tatp::kUpdateLocation: {
                timer.reset();
                tatp::UpdateLocationTxnParam* param = (tatp::UpdateLocationTxnParam*)exec.params;
                tatp::UpdateLocationTxnResult* results =
                    (tatp::UpdateLocationTxnResult*)exec.results;
                while (!tx_committed && exec_count++ < BenchConfig::TATP_MAX_EXEC_COUNT) {
                    tx_committed = tatp::TxnUpdateLocation(txn, txn_id, param, results, yield,
                                                           ret_txn_results);
                    ++(*t_ctx->tried_txn_count);
                    uint64_t u = timer.ns_elapse();
                    UpdateTATP_UPDATE_LOCATION_Stats(txn, stats, tx_committed, u);
                }
                if (tx_committed) {
                    int id = *t_ctx->committed_txn_count;
                    t_ctx->txn_latency[id] = latency_timer.us_elapse();
                    ++(*(t_ctx->committed_txn_count));
                }
                uint64_t u = timer.ns_elapse();
                UpdateTATP_UPDATE_LOCATION_Stats(txn, stats, tx_committed, u);
                exec.committed = tx_committed;
                break;
            }
            case tatp::kUpdateSubscriberData: {
                timer.reset();
                tatp::UpdateSubscriberDataTxnParam* param =
                    (tatp::UpdateSubscriberDataTxnParam*)exec.params;
                tatp::UpdateSubscriberDataTxnResult* results =
                    (tatp::UpdateSubscriberDataTxnResult*)exec.results;
                while (!tx_committed && exec_count++ < BenchConfig::TATP_MAX_EXEC_COUNT) {
                    tx_committed = tatp::TxnUpdateSubscriberData(txn, txn_id, param, results, yield,
                                                                 ret_txn_results);
                    ++(*t_ctx->tried_txn_count);
                    uint64_t u = timer.ns_elapse();
                    UpdateTATP_UPDATE_SUBSCRIBER_DATA_Stats(txn, stats, tx_committed, u);
                }
                if (tx_committed) {
                    int id = *t_ctx->committed_txn_count;
                    t_ctx->txn_latency[id] = latency_timer.us_elapse();
                    ++(*(t_ctx->committed_txn_count));
                }
                uint64_t u = timer.ns_elapse();
                UpdateTATP_UPDATE_SUBSCRIBER_DATA_Stats(txn, stats, tx_committed, u);
                exec.committed = tx_committed;
                break;
            }
            case tatp::kInsertCallForwarding: {
                timer.reset();
                tatp::InsertCallForwardingTxnParam* param =
                    (tatp::InsertCallForwardingTxnParam*)exec.params;
                tatp::InsertCallForwardingTxnResult* results =
                    (tatp::InsertCallForwardingTxnResult*)exec.results;
                while (!tx_committed && exec_count++ < BenchConfig::TATP_MAX_EXEC_COUNT) {
                    tx_committed = tatp::TxnInsertCallForwarding(txn, txn_id, param, results, yield,
                                                                 ret_txn_results);
                    ++(*t_ctx->tried_txn_count);
                    uint64_t u = timer.ns_elapse();
                    UpdateTATP_INSERT_CALL_FORWARDING_Stats(txn, stats, tx_committed, u);
                }
                if (tx_committed) {
                    int id = *t_ctx->committed_txn_count;
                    t_ctx->txn_latency[id] = latency_timer.us_elapse();
                    ++(*(t_ctx->committed_txn_count));
                }
                uint64_t u = timer.ns_elapse();
                UpdateTATP_INSERT_CALL_FORWARDING_Stats(txn, stats, tx_committed, u);
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

void TATPBenchmark::PrepareWorkloads() {
    if (IsMN()) {
        return;
    }

    LOG_INFO("Start Prepare Workloads");

    util::Timer timer;
    for (int i = 0; i < config_.thread_num; ++i) {
        exec_hist_->reserve(config_.txn_num);
    }

    central_randoms_.SetSeed(0x182912);

    PrepareTxnTypeArrays();

    for (int i = 0; i < config_.txn_num; ++i) {
        ThreadId tid = i % config_.thread_num;
        BenchTxnType tx_type = GenerateTxnType(tid);
        switch (tx_type) {
            case tatp::kGetSubscriber: {
                exec_hist_[tid].emplace_back(tatp::kGetSubscriber, tatp_ctx_, config_.replay);
                GenerateGetSubscriberParams(
                    tid, (tatp::GetSubscriberTxnParam*)exec_hist_[tid].back().params);
                break;
            }
            case tatp::kGetNewDestination: {
                exec_hist_[tid].emplace_back(tatp::kGetNewDestination, tatp_ctx_, config_.replay);
                GenerateGetNewDestinationParams(
                    tid, (tatp::GetNewDestinationTxnParam*)exec_hist_[tid].back().params);
                break;
            }
            case tatp::kGetAccessData: {
                exec_hist_[tid].emplace_back(tatp::kGetAccessData, tatp_ctx_, config_.replay);
                GenerateGetAccessDataParams(
                    tid, (tatp::GetAccessDataTxnParam*)exec_hist_[tid].back().params);
                break;
            }
            case tatp::kUpdateSubscriberData: {
                exec_hist_[tid].emplace_back(tatp::kUpdateSubscriberData, tatp_ctx_,
                                             config_.replay);
                GenerateUpdateSubscriberDataParams(
                    tid, (tatp::UpdateSubscriberDataTxnParam*)exec_hist_[tid].back().params);
                break;
            }
            case tatp::kUpdateLocation: {
                exec_hist_[tid].emplace_back(tatp::kUpdateLocation, tatp_ctx_, config_.replay);
                GenerateUpdateLocationParams(
                    tid, (tatp::UpdateLocationTxnParam*)exec_hist_[tid].back().params);
                break;
            }
            case tatp::kInsertCallForwarding: {
                exec_hist_[tid].emplace_back(tatp::kInsertCallForwarding, tatp_ctx_,
                                             config_.replay);
                GenerateInsertCallForwardingParams(
                    tid, (tatp::InsertCallForwardingTxnParam*)exec_hist_[tid].back().params);
                break;
            }
            default:
                break;
        }
    }

    LOG_INFO("[Prepare Workload Done][Elaps: %.2lf ms][Thread: %d ][TxnNum: %d ]\n",
             timer.ms_elapse(), config_.thread_num, config_.txn_num);
}

void TATPBenchmark::PrepareTxnTypeArrays() {
    int base = 0;
    for (int i = 0; i < tatp::FREQUENCY_GET_SUBSCRIBER; ++i) {
        txn_type_arrays_[i] = tatp::kGetSubscriber;
    }
    base += tatp::FREQUENCY_GET_SUBSCRIBER;

    for (int i = 0; i < tatp::FREQUENCY_GET_NEW_DESTINATION; ++i) {
        txn_type_arrays_[i + base] = tatp::kGetNewDestination;
    }
    base += tatp::FREQUENCY_GET_NEW_DESTINATION;

    for (int i = 0; i < tatp::FREQUENCY_GET_ACCESS_DATA; ++i) {
        txn_type_arrays_[i + base] = tatp::kGetAccessData;
    }
    base += tatp::FREQUENCY_GET_ACCESS_DATA;

    for (int i = 0; i < tatp::FREQUENCY_UPDATE_SUBSCRIBER_DATA; ++i) {
        txn_type_arrays_[i + base] = tatp::kUpdateSubscriberData;
    }
    base += tatp::FREQUENCY_UPDATE_SUBSCRIBER_DATA;

    for (int i = 0; i < tatp::FREQUENCY_UPDATE_LOCATION; ++i) {
        txn_type_arrays_[i + base] = tatp::kUpdateLocation;
    }
    base += tatp::FREQUENCY_UPDATE_LOCATION;

    for (int i = 0; i < tatp::FREQUENCY_INSERT_CALL_FORWARDING; ++i) {
        txn_type_arrays_[i + base] = tatp::kInsertCallForwarding;
    }
    base += tatp::FREQUENCY_INSERT_CALL_FORWARDING;

    for (int i = 0; i < tatp::FREQUENCY_DELETE_CALL_FORWARDING; ++i) {
        txn_type_arrays_[i + base] = tatp::kDeleteCallForwarding;
    }
    base += tatp::FREQUENCY_DELETE_CALL_FORWARDING;
}