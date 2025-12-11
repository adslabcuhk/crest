#include "Base/Benchmark.h"

#include <filesystem>
#include <numeric>

#include "common/Type.h"
#include "db/AddressCache.h"
#include "mempool/BufferManager.h"
#include "mempool/Coroutine.h"
#include "rdma/MemoryRegion.h"
#include "transaction/TimestampGen.h"
#include "transaction/Txn.h"

Status Benchmark::Initialize(const BenchmarkConfig &config) {
    // Store the configuration structs for further use
    config_ = config;
    thread_ctxs_ = new ThreadCtx[config.thread_num];
    thread_num_ = config.thread_num;
    std::memset(g_tried_txn_count, 0, sizeof(g_tried_txn_count));
    std::memset(g_committed_txn_count, 0, sizeof(g_committed_txn_count));

    // Configuration based on this benchmark runs on CN or MN:
    if (IsCN()) {
        // Create memcached wrapper to synchronize different CNs
        // For simplicity, we use the first memory node's IP address as the memcached server
        memcached_ = new MemcachedWrapper(config.node_init_attr.num_cns, config.NodeId(),
                                          config.pool_attr[0].ip, NodeInitAttr::MEMCACHED_PORT);

        // Create the Pool manager for each thread first
        rdma::Context *ctx =
            new rdma::Context(config.node_init_attr.devname, config.node_init_attr.ib_port,
                              config.node_init_attr.gid_idx);
        Status s = ctx->Init();
        ASSERT(s.ok(), "Create context failed");
        rdma::MemoryRegion *mr = new rdma::MemoryRegion(ctx, config.node_init_attr.mr_size);

        rdma::Context *curr_ctx = ctx;
        rdma::MemoryRegion *curr_mr = mr;

        char *buf_addr = (char *)mr->data();
        size_t buf_sz = mr->size();

        const int CtxSharedNum = 64;

        RecordHandleDB *record_handle_db = new RecordHandleDB(16);

        // The base initialization function only needs to create establish the RDMA connection
        for (int tid = 0; tid < config.thread_num; ++tid) {
            ThreadCtx *t_ctx = &thread_ctxs_[tid];

            t_ctx->stats = new Statistics();
            t_ctx->pool = new Pool(config.node_init_attr, t_ctx->stats);
            // All threads share the same local database
            t_ctx->record_handle_db = record_handle_db;

            // t_ctx->pool = new Pool(config.node_init_attr, t_ctx->stats);
            t_ctx->config = config;

            t_ctx->txn_latency = new int[1000000];           // 1M per thread
            t_ctx->txn_exec_latency = new int[1000000];      // 1M per thread
            t_ctx->txn_validate_latency = new int[1000000];  // 1M per thread
            t_ctx->txn_commit_latency = new int[1000000];    // 1M per thread

            // Even though we assign each thread with dedicated QueuePair, they still incurs the
            // implicit contention on the doorbell register and limits the concurrency, which is
            // confirmed by the ASPLOS'24 paper:
            //   * Scaling Up Memory Disaggregated Applications with Smart.
            // According to this paper, the default number of doorbell register allocated from one
            // ibv_context is 16, thus we only allow at most 16 threads to share the same
            // ibv_context. In this way, every thread can read-write a "private" doorbell register
            // and thus avoids contention. However, we find that such optimization has no effect on
            // the overall performance as the transactional system spent most of its time on
            // transaction logic, e.g., check lock results, execute transaction update, and so on.
            if (tid > 0 && tid % CtxSharedNum == 0) {
                curr_ctx =
                    new rdma::Context(config.node_init_attr.devname, config.node_init_attr.ib_port,
                                      config.node_init_attr.gid_idx);
                s = curr_ctx->Init();
                ASSERT(s.ok(), "Create context failed");
                curr_mr = new rdma::MemoryRegion(curr_ctx, buf_addr, buf_sz);
            }

            // CN threads build connection to all MNs
            s = t_ctx->pool->BuildConnection(curr_ctx, curr_mr, config.pool_attr, 1);
            ASSERT(s.ok(), "Thread%d build connection failed", tid);

            t_ctx->local_tid = tid;
            t_ctx->global_tid = config.NodeId() * config.ThreadPerCN() + tid;
            CoroutineScheduler *sched =
                new CoroutineScheduler(t_ctx->local_tid, t_ctx->config.coro_num, t_ctx->stats);
            t_ctx->pool->SetCoroSched(sched);
        }

        // Just simply use one pool structure
        this->pool_ = thread_ctxs_[0].pool;

    } else if (IsMN()) {
        // For memory node, set the memcached server id to be itself
        memcached_ = new MemcachedWrapper(config.node_init_attr.num_cns, config.NodeId(),
                                          config.node_init_attr.ip, NodeInitAttr::MEMCACHED_PORT);
        // Initialize memcached value
        memcached_->ConnectToMemcached();
        memcached_->ResetServerNum();
        Pool *pool = new Pool(config.node_init_attr);
        Status s = pool->CreateLocalMemoryRegion();
        ASSERT(s.ok(), "MN initialize local memory region failed");
        LOG_INFO("Create memory region %#llx, size %lu GiB",
                 pool->GetLocalMemoryRegionToken().get_region_addr(),
                 pool->GetLocalMemoryRegionToken().get_region_size() / (1 << 30));
        this->pool_ = pool;
    }
    return Status::OK();
}

void Benchmark::RunStatsReporter(int sec) {
    uint64_t last_committed = 0, last_tried = 0;
    // Ticker for every one second
    Timer timer;
    timer.reset();
    // Run for 10 seconds
    const int sleep_interval = 500;  // ms
    while (timer.s_elapse() < sec) {
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_interval));
        uint64_t curr_committed = 0, curr_tried = 0;
        for (int tid = 0; tid < thread_num_; ++tid) {
            curr_committed += g_committed_txn_count[tid][0];
            curr_tried += g_tried_txn_count[tid][0];
        }
        LOG_INFO(
            "Tried txn thpt: %.2lf KOPS, committed txn thpt: %.2lf KOPS, abort "
            "rate: %.2lf",
            (curr_tried - last_tried) / 1000.0 * ((double)1000 / sleep_interval),
            (curr_committed - last_committed) / 1000.0 * ((double)1000 / sleep_interval),
            1.0 - (double)curr_committed / curr_tried);
        last_committed = curr_committed;
        last_tried = curr_tried;
    }
}

void Benchmark::ReportMergedThreadResults() {
    Statistics &stats = *thread_ctxs_[0].stats;
    for (int i = 1; i < thread_num_; ++i) {
        stats.Merge(thread_ctxs_[i].stats);
    }
    // Report overall throughput
    uint64_t all_tried = 0, all_committed = 0;
    double tried_thpt = 0.0, committed_thpt = 0.0;
    for (int i = 0; i < thread_num_; ++i) {
        all_tried += g_tried_txn_count[i][0];
        all_committed += g_committed_txn_count[i][0];
        tried_thpt += g_tried_txn_thpt[i];
        committed_thpt += g_committed_txn_thpt[i];
    }

    // Report overall latency
    std::vector<int> all_latency;
    std::vector<int> all_exec_latency;
    std::vector<int> all_validate_latency;
    std::vector<int> all_commit_latency;
    for (int i = 0; i < thread_num_; ++i) {
        ThreadCtx *t_ctx = &thread_ctxs_[i];
        int committed_cnt = g_committed_txn_count[i][0];
        std::vector<int> txn_latency(t_ctx->txn_latency, t_ctx->txn_latency + committed_cnt);
        int discard_size = txn_latency.size() * 0.05;
        txn_latency.erase(txn_latency.begin(), txn_latency.begin() + discard_size);
        txn_latency.erase(txn_latency.end() - discard_size, txn_latency.end());
        all_latency.insert(all_latency.end(), txn_latency.begin(), txn_latency.end());

        std::vector<int> txn_exec_latency(t_ctx->txn_exec_latency,
                                          t_ctx->txn_exec_latency + committed_cnt);
        txn_exec_latency.erase(txn_exec_latency.begin(), txn_exec_latency.begin() + discard_size);
        txn_exec_latency.erase(txn_exec_latency.end() - discard_size, txn_exec_latency.end());
        all_exec_latency.insert(all_exec_latency.end(), txn_exec_latency.begin(),
                                txn_exec_latency.end());

        std::vector<int> txn_validate_latency(t_ctx->txn_validate_latency,
                                              t_ctx->txn_validate_latency + committed_cnt);
        txn_validate_latency.erase(txn_validate_latency.begin(),
                                   txn_validate_latency.begin() + discard_size);
        txn_validate_latency.erase(txn_validate_latency.end() - discard_size,
                                   txn_validate_latency.end());
        all_validate_latency.insert(all_validate_latency.end(), txn_validate_latency.begin(),
                                    txn_validate_latency.end());

        std::vector<int> txn_commit_latency(t_ctx->txn_commit_latency,
                                            t_ctx->txn_commit_latency + committed_cnt);
        txn_commit_latency.erase(txn_commit_latency.begin(),
                                 txn_commit_latency.begin() + discard_size);
        txn_commit_latency.erase(txn_commit_latency.end() - discard_size, txn_commit_latency.end());
        all_commit_latency.insert(all_commit_latency.end(), txn_commit_latency.begin(),
                                  txn_commit_latency.end());
    }
    std::sort(all_latency.begin(), all_latency.end());
    int avg_latency =
        std::accumulate(all_latency.begin(), all_latency.end(), 0) / all_latency.size();
    int p50_latency = all_latency[all_latency.size() * 0.5];
    int p90_latency = all_latency[all_latency.size() * 0.9];
    int p99_latency = all_latency[all_latency.size() * 0.99];
    int p999_latency = all_latency[all_latency.size() * 0.999];

    int avg_exec_latency = std::accumulate(all_exec_latency.begin(), all_exec_latency.end(), 0) /
                           all_exec_latency.size();
    int avg_validate_latency =
        std::accumulate(all_validate_latency.begin(), all_validate_latency.end(), 0) /
        all_validate_latency.size();
    int avg_commit_latency =
        std::accumulate(all_commit_latency.begin(), all_commit_latency.end(), 0) /
        all_commit_latency.size();

    if (!config_.output_dir.empty()) {
        // Create the output directory if not exists
        if (!std::filesystem::exists(config_.output_dir)) {
            std::filesystem::create_directories(config_.output_dir);
        }
        std::string res_file = config_.output_dir + "/results.txt";
        std::string detail_file = config_.output_dir + "/details.txt";

        std::ofstream res_of;
        res_of.open(res_file, std::ofstream::out | std::ofstream::app);
        res_of << "crest " << tried_thpt * 1000.0 << " " << committed_thpt * 1000.0 << " "
               << avg_latency << " " << p50_latency << " " << p90_latency << " " << p99_latency
               << " " << p999_latency << " " << avg_exec_latency << " " << avg_validate_latency
               << " " << avg_commit_latency << "\n";
        res_of.close();

        std::ofstream detail_of;
        detail_of.open(detail_file, std::ofstream::out | std::ofstream::app);
        detail_of << "[Threads: " << config_.thread_num << "  Coroutine: " << config_.coro_num
                  << " ]\n";
        detail_of << stats.ToString() << "\n\n\n";
        detail_of.close();
    }

    // Print to the console for illustration
    std::cout << stats.ToString() << "\n";
    std::cout << "Tried: " << all_tried << " Committed: " << all_committed << "\n";
    std::cout << "Tried Throughput: " << tried_thpt * 1000.0 << " KOPS, "
              << "Commit Troughput: " << committed_thpt * 1000.0 << " KOPS \n";

    std::cout << "Average latency: " << avg_latency << " us, "
              << "P50 latency: " << p50_latency << " us, "
              << "P90 latency: " << p90_latency << " us, "
              << "P99 latency: " << p99_latency << " us\n";
}

BufferAllocationParam Benchmark::ParseAllocationParam(const BenchmarkConfig &config,
                                                      MemoryRegionToken mr_token) {
    BufferAllocationParam param;
    size_t log_size_per_thread = LogManager::PER_COODINATOR_LOG_SIZE * 2 * config.coro_num;
    param.log_size_per_thread = log_size_per_thread;
    char *mr_addr = (char *)(mr_token.get_region_addr());
    param.log_buffer_size = log_size_per_thread * config.thread_num;
    param.log_buffer_base = mr_addr;
    param.data_buffer_size = mr_token.get_region_size() - param.log_buffer_size;
    param.data_buffer_base = mr_addr + param.log_buffer_size;
    this->buf_allocate_param_ = param;
    return param;
}
