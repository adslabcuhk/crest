#include <infiniband/verbs.h>

#include <algorithm>
#include <chrono>
#include <iostream>

#include "Base/BenchConfig.h"
#include "Base/BenchTypes.h"
#include "Generator.h"
#include "TPCC/TpccBenchmark.h"
#include "TPCC/TpccConstant.h"
#include "TPCC/TpccTxnImpl.h"
#include "TPCC/TpccTxnStructs.h"
#include "TPCC/TpccUtil.h"
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

uint64_t current_time = 0;

static const std::vector<int> numa_nodes[2] = {
    // NUMA Node 0 in our machine: 0 ~ 23, 48 ~ 71
    {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15,
     16, 17, 18, 19, 20, 21, 22, 23, 48, 49, 50, 51, 52, 53, 54, 55,
     56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71},

    // NUMA Node 1 in our machine:
    {24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
     40, 41, 42, 43, 44, 45, 46, 47, 72, 73, 74, 75, 76, 77, 78, 79,
     80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95}};

void TpccBenchmark::GenerateNewOrderParams(ThreadId tid, tpcc::NewOrderTxnParam* no_param) {
    if (BenchConfig::WORKLOADS_PARTITION) {
        no_param->warehouse_id_ = tid % tpcc_config_.num_warehouse;
    } else {
        no_param->warehouse_id_ =
            PickWarehouseId(fast_randoms_[tid], 0, tpcc_config_.num_warehouse);
    }
    no_param->district_id_ =
        RandomNumber(fast_randoms_[tid], 0, tpcc_config_.num_district_per_warehouse);
    no_param->customer_id_ =
        GetCustomerId(fast_randoms_[tid], tpcc_config_.num_customer_per_district);

    int ol_num = RandomNumber(fast_randoms_[tid], tpcc::MIN_OL_CNT, tpcc::MAX_OL_CNT);
    no_param->ol_num_ = ol_num;

    // Generate ol_number stocks and items:
    std::set<RecordKey> stock_keys;
    for (int i = 0; i < no_param->ol_num_; ++i) {
        int64_t item_id =
            GetItemId(fast_randoms_[tid], tpcc_config_.uniform_item_dist, tpcc_config_.num_item);
        // Generate a supply warehouse id:
        int32_t supply_warehouse_id = 0;
        if ((tpcc_config_.num_warehouse == 1) || RandomNumber(fast_randoms_[tid], 1, 100) > 15) {
            supply_warehouse_id = no_param->warehouse_id_;
        } else {
            do {
                supply_warehouse_id =
                    RandomNumber(fast_randoms_[tid], 0, tpcc_config_.num_warehouse);
            } while (supply_warehouse_id == no_param->warehouse_id_);
        }
        // Generate the stock key:
        RecordKey stock_key = tpcc_ctx_->MakeStockKey(supply_warehouse_id, item_id);
        // Deduplicate stock key, we need to generate another stock key
        if (stock_keys.find(stock_key) != stock_keys.end()) {
            i--;
            continue;
        } else {
            stock_keys.insert(stock_key);
        }
        no_param->item_ids_[i] = item_id;
        no_param->stock_keys_[i] = stock_key;
        no_param->supply_warehouse_ids_[i] = supply_warehouse_id;
        no_param->quantities_[i] = RandomNumber(fast_randoms_[tid], 1, 10);
    }
    std::sort(no_param->item_ids_, no_param->item_ids_ + ol_num);
    std::sort(no_param->stock_keys_, no_param->stock_keys_ + ol_num);
}

void TpccBenchmark::GeneratePaymentParams(ThreadId tid, tpcc::PaymentTxnParam* p_param) {
    if (BenchConfig::WORKLOADS_PARTITION) {
        p_param->warehouse_id_ = tid % tpcc_config_.num_warehouse;
    } else {
        p_param->warehouse_id_ = PickWarehouseId(fast_randoms_[tid], 0, tpcc_config_.num_warehouse);
    }
    p_param->district_id_ =
        RandomNumber(fast_randoms_[tid], 0, tpcc_config_.num_district_per_warehouse);

    int x = RandomNumber(fast_randoms_[tid], 1, 100);
    int y = RandomNumber(fast_randoms_[tid], 1, 100);

    int32_t c_w_id;
    int32_t c_d_id;
    if (tpcc_config_.num_warehouse == 1 || x <= 85) {
        // 85%: paying through own warehouse (or there is only 1 warehouse)
        p_param->c_w_id_ = p_param->warehouse_id_;
        p_param->c_d_id_ = p_param->district_id_;
    } else {
        // 15%: paying through another warehouse:
        // select in range [1, num_warehouses] excluding w_id
        do {
            c_w_id = RandomNumber(fast_randoms_[tid], 0, tpcc_config_.num_warehouse);
        } while (c_w_id == p_param->c_w_id_);
        c_d_id = RandomNumber(fast_randoms_[tid], 0, tpcc_config_.num_district_per_warehouse);
        p_param->c_w_id_ = c_w_id;
        p_param->c_d_id_ = c_d_id;
    }
    uint32_t customer_id = 0;
    // The payment amount (H_AMOUNT) is randomly selected within [1.00 .. 5,000.00].
    // p_param->h_amount_ = (float)RandomNumber(fast_randoms_[tid], 100, 500000) / 100.0;
    p_param->h_amount_ = 5.00;
    p_param->customer_id_ =
        GetCustomerId(fast_randoms_[tid], tpcc_config_.num_customer_per_district);
    p_param->h_date_ = GetCurrentTimeMillis();
    return;
}

void TpccBenchmark::GenerateOrderStatusParams(ThreadId tid, tpcc::OrderStatusTxnParam* os_param) {
    os_param->warehouse_id_ = PickWarehouseId(fast_randoms_[tid], 0, tpcc_config_.num_warehouse);
    os_param->district_id_ =
        RandomNumber(fast_randoms_[tid], 0, tpcc_config_.num_district_per_warehouse);
    os_param->customer_id_ =
        GetCustomerId(fast_randoms_[tid], tpcc_config_.num_customer_per_district);
    os_param->order_id_ =
        RandomNumber(fast_randoms_[tid], 0, tpcc_config_.num_customer_per_district - 1);
    return;
}

void TpccBenchmark::GenerateDeliveryParams(ThreadId tid, tpcc::DeliveryTxnParam* d_params) {
    d_params->warehouse_id_ = PickWarehouseId(fast_randoms_[tid], 0, tpcc_config_.num_warehouse);
    d_params->o_id_ =
        RandomNumber(fast_randoms_[tid], 0, tpcc_config_.num_customer_per_district - 1);
    d_params->o_carrier_id_ = RandomNumber(fast_randoms_[tid], 1, 10);
    d_params->ol_delivery_d_ = GetCurrentTimeMillis();
}


BenchTxnType TpccBenchmark::GenerateTxnType(ThreadId tid) {
    int d = FastRand(&txn_type_gen_[tid]) % 100;
    return txn_type_arrays_[d];
}

void TpccBenchmark::PrepareTxnTypeArrays() {
    // Initialize the txn_type_arrays_ array :
    int base = 0;
    for (int i = 0; i < tpcc::FREQUENCY_NEWORDER; ++i) {
        txn_type_arrays_[i + base] = tpcc::kNewOrder;
    }
    base += tpcc::FREQUENCY_NEWORDER;

    for (int i = 0; i < tpcc::FREQUENCY_PAYMENT; ++i) {
        txn_type_arrays_[i + base] = tpcc::kPayment;
    }
    base += tpcc::FREQUENCY_PAYMENT;

    for (int i = 0; i < tpcc::FREQUENCY_DELIVERY; ++i) {
        txn_type_arrays_[i + base] = tpcc::kDelivery;
    }
    base += tpcc::FREQUENCY_DELIVERY;

    for (int i = 0; i < tpcc::FREQUENCY_ORDER_STATUS; ++i) {
        txn_type_arrays_[i + base] = tpcc::kOrderStatus;
    }
    base += tpcc::FREQUENCY_ORDER_STATUS;

    for (int i = 0; i < tpcc::FREQUENCY_STOCK_LEVEL; ++i) {
        txn_type_arrays_[i + base] = tpcc::kStockLevel;
    }
    base += tpcc::FREQUENCY_STOCK_LEVEL;
}

void TpccBenchmark::PrepareWorkloads() {
    if (IsMN()) {
        return;
    }

    LOG_INFO("Start Prepare Workloads");

    ResetCurrentTime(tpcc_config_.num_warehouse * tpcc_config_.num_district_per_warehouse *
                     tpcc_config_.num_customer_per_district);

    util::Timer timer;
    for (int i = 0; i < config_.thread_num; ++i) {
        exec_hist_->reserve(config_.txn_num);
    }

    PrepareTxnTypeArrays();

    for (int i = 0; i < config_.txn_num; ++i) {
        ThreadId tid = i % config_.thread_num;
        BenchTxnType tx_type = GenerateTxnType(tid);
        switch (tx_type) {
            case tpcc::kNewOrder: {
                exec_hist_[tid].emplace_back(tpcc::kNewOrder, tpcc_ctx_, config_.replay);
                GenerateNewOrderParams(tid, (tpcc::NewOrderTxnParam*)exec_hist_[tid].back().params);
                break;
            }
            case tpcc::kPayment: {
                exec_hist_[tid].emplace_back(tpcc::kPayment, tpcc_ctx_, config_.replay);
                GeneratePaymentParams(tid, (tpcc::PaymentTxnParam*)exec_hist_[tid].back().params);
                break;
            }
            case tpcc::kOrderStatus: {
                exec_hist_[tid].emplace_back(tpcc::kOrderStatus, tpcc_ctx_, config_.replay);
                GenerateOrderStatusParams(
                    tid, (tpcc::OrderStatusTxnParam*)exec_hist_[tid].back().params);
                break;
            }
            case tpcc::kDelivery: {
                exec_hist_[tid].emplace_back(tpcc::kDelivery, tpcc_ctx_, config_.replay);
                GenerateDeliveryParams(tid, (tpcc::DeliveryTxnParam*)exec_hist_[tid].back().params);
            }
            default:
                break;
        }
    }

    LOG_INFO("[Prepare Workload Done][Elaps: %.2lf ms][Thread: %d ][TxnNum: %d ]\n",
             timer.ms_elapse(), config_.thread_num, config_.txn_num);
}

void TpccBenchmark::BenchCoroutine(coro_yield_t& yield, coro_id_t coro_id, ThreadCtx* t_ctx,
                                   std::vector<TpccTxnExecHistory>* batch) {
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

    uint64_t exec_latency = 0, validate_latency = 0, commit_latency = 0;

    while (!t_ctx->signal_stop.load() && txn_exec_idx < txn_exec_num) {
        TpccTxnExecHistory& exec = batch->at(txn_exec_idx);
        txn_exec_idx += (t_ctx->config.coro_num - 1);
        TxnId txn_id = TransactionId(t_ctx->global_tid, coro_id, seq_num);
        ++seq_num;
        // printf("Debug: seq = %lu\n", seq_num);
        tx_committed = false;
        exec_count = 0;
        latency_timer.reset();

        exec_latency = 0;
        validate_latency = 0;
        commit_latency = 0;

        switch (exec.type) {
            case tpcc::kNewOrder: {
                timer.reset();
                tpcc::NewOrderTxnParam* param = (tpcc::NewOrderTxnParam*)exec.params;
                tpcc::NewOrderTxnResult* results = (tpcc::NewOrderTxnResult*)exec.results;
                while (!tx_committed && exec_count++ < BenchConfig::TPCC_MAX_EXEC_COUNT) {
                    tx_committed =
                        tpcc::TxnNewOrder(txn, txn_id, param, results, yield, ret_txn_results);
                    exec_latency += (txn->GetPhaseLatency(Txn::TxnPhase::READ_PHASE));
                    exec_latency += (txn->GetPhaseLatency(Txn::TxnPhase::ABORT_PHASE));
                    validate_latency += (txn->GetPhaseLatency(Txn::TxnPhase::VALIDATION_PHASE));
                    commit_latency += (txn->GetPhaseLatency(Txn::TxnPhase::COMMIT_PHASE));
                    ++(*t_ctx->tried_txn_count);
                    uint64_t u = timer.ns_elapse();
                    UpdateTPCC_NEWORDER_Stats(txn, stats, tx_committed, u);
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
                UpdateTPCC_NEWORDER_Stats(txn, stats, tx_committed, u);
                exec.committed = tx_committed;
                break;
            }
            case tpcc::kPayment: {
                timer.reset();
                tpcc::PaymentTxnParam* param = (tpcc::PaymentTxnParam*)exec.params;
                tpcc::PaymentTxnResult* results = (tpcc::PaymentTxnResult*)exec.results;
                while (!tx_committed && exec_count++ < BenchConfig::TPCC_MAX_EXEC_COUNT) {
                    tx_committed =
                        tpcc::TxnPayment(txn, txn_id, param, results, yield, ret_txn_results);
                    exec_latency += (txn->GetPhaseLatency(Txn::TxnPhase::READ_PHASE));
                    exec_latency += (txn->GetPhaseLatency(Txn::TxnPhase::ABORT_PHASE));
                    validate_latency += (txn->GetPhaseLatency(Txn::TxnPhase::VALIDATION_PHASE));
                    commit_latency += (txn->GetPhaseLatency(Txn::TxnPhase::COMMIT_PHASE));
                    ++(*t_ctx->tried_txn_count);
                    uint64_t u = timer.ns_elapse();
                    UpdateTPCC_PAYMENT_Stats(txn, stats, tx_committed, u);
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
                UpdateTPCC_PAYMENT_Stats(txn, stats, tx_committed, u);
                exec.committed = tx_committed;
                break;
            }
            case tpcc::kOrderStatus: {
                timer.reset();
                tpcc::OrderStatusTxnParam* param = (tpcc::OrderStatusTxnParam*)exec.params;
                tpcc::OrderStatusTxnResult* results = (tpcc::OrderStatusTxnResult*)exec.results;
                tx_committed =
                    tpcc::TxnOrderStatus(txn, txn_id, param, results, yield, ret_txn_results);
                exec_latency += (txn->GetPhaseLatency(Txn::TxnPhase::READ_PHASE));
                exec_latency += (txn->GetPhaseLatency(Txn::TxnPhase::ABORT_PHASE));
                validate_latency += (txn->GetPhaseLatency(Txn::TxnPhase::VALIDATION_PHASE));
                commit_latency += (txn->GetPhaseLatency(Txn::TxnPhase::COMMIT_PHASE));
                ++(*t_ctx->tried_txn_count);
                if (tx_committed) {
                    // int id = *t_ctx->committed_txn_count;
                    // t_ctx->txn_latency[id] = latency_timer.us_elapse();
                    // t_ctx->txn_exec_latency[id] = exec_latency / 1000;
                    // t_ctx->txn_validate_latency[id] = validate_latency / 1000;
                    // t_ctx->txn_commit_latency[id] = commit_latency / 1000;
                    ++(*(t_ctx->committed_txn_count));
                }
                uint64_t u = timer.ns_elapse();
                UpdateTPCC_ORDER_STATUS_Stats(txn, stats, tx_committed, u);
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

Status TpccBenchmark::Initialize(const BenchmarkConfig& config) {
    util::Timer timer;
    Status s = Benchmark::Initialize(config);
    ASSERT(s.ok(), "Benchmark Base initialization failed");

    tpcc_config_ = *(TpccConfig*)(config.workload_config);
    tpcc_ctx_ = new TpccContext(tpcc_config_);
    bench_db_ = CreateDB(tpcc_config_);

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
        meta_page = bench_db_->GetTable(tpcc::WAREHOUSE_TABLE)->SerializeTableInfo(meta_page);
        meta_page = bench_db_->GetTable(tpcc::DISTRICT_TABLE)->SerializeTableInfo(meta_page);
        meta_page = bench_db_->GetTable(tpcc::CUSTOMER_TABLE)->SerializeTableInfo(meta_page);
        meta_page = bench_db_->GetTable(tpcc::HISTORY_TABLE)->SerializeTableInfo(meta_page);
        meta_page = bench_db_->GetTable(tpcc::NEW_ORDER_TABLE)->SerializeTableInfo(meta_page);
        meta_page = bench_db_->GetTable(tpcc::ORDER_TABLE)->SerializeTableInfo(meta_page);
        meta_page = bench_db_->GetTable(tpcc::ORDER_LINE_TABLE)->SerializeTableInfo(meta_page);
        meta_page = bench_db_->GetTable(tpcc::ITEM_TABLE)->SerializeTableInfo(meta_page);
        meta_page = bench_db_->GetTable(tpcc::STOCK_TABLE)->SerializeTableInfo(meta_page);
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
            t_ctx->db = CreateDB(tpcc_config_);
            t_ctx->workload_ctx = tpcc_ctx_;
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
            // Register the TPCC-benchmark related statistics
            ASSERT(t_ctx->stats != nullptr, "");
            for (const auto& [h, n] : tpcc_bench_hist) {
                t_ctx->stats->RegisterHist(h, n);
            }
            for (const auto& [t, n] : tpcc_bench_ticker) {
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

Status TpccBenchmark::InitDatabaseMeta(ThreadCtx* t_ctx, char* db_meta_page, bool primary_log) {
    const char* p = db_meta_page;
    PoolPtr log_area = Next<PoolPtr>(p);
    p = t_ctx->db->GetTable(tpcc::WAREHOUSE_TABLE)->DeserializeTableInfo(p);
    p = t_ctx->db->GetTable(tpcc::DISTRICT_TABLE)->DeserializeTableInfo(p);
    p = t_ctx->db->GetTable(tpcc::CUSTOMER_TABLE)->DeserializeTableInfo(p);
    p = t_ctx->db->GetTable(tpcc::HISTORY_TABLE)->DeserializeTableInfo(p);
    p = t_ctx->db->GetTable(tpcc::NEW_ORDER_TABLE)->DeserializeTableInfo(p);
    p = t_ctx->db->GetTable(tpcc::ORDER_TABLE)->DeserializeTableInfo(p);
    p = t_ctx->db->GetTable(tpcc::ORDER_LINE_TABLE)->DeserializeTableInfo(p);
    p = t_ctx->db->GetTable(tpcc::ITEM_TABLE)->DeserializeTableInfo(p);
    p = t_ctx->db->GetTable(tpcc::STOCK_TABLE)->DeserializeTableInfo(p);

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

void TpccBenchmark::InitDatabaseMetaPage() {}

Status TpccBenchmark::Run() {
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

void TpccBenchmark::BenchmarkThread(ThreadCtx* t_ctx, TpccBenchmark* bench) {
    auto tid = t_ctx->local_tid;
    // Bind this thread to the specific CPU core
    // We don't explicitly bind the CPU cores here, the user should use numactl
    // command to explicitly bind the CPU cores
    //
    // cpu_set_t cpuset;
    // CPU_ZERO(&cpuset);
    // CPU_SET(numa_nodes[0][t_ctx->tid], &cpuset);
    // pthread_t thread = pthread_self();
    // if (pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset) != 0) {
    //   LOG_FATAL("T%d bind CPU core failed", t_ctx->tid);
    // }

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
