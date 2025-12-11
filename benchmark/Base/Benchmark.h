#pragma once

#include <string>
#include <thread>
#include <vector>

#include "Base/BenchConfig.h"
#include "Base/Memcached.h"
#include "common/Type.h"
#include "db/AddressCache.h"
#include "db/Db.h"
#include "mempool/BufferManager.h"
#include "mempool/Coroutine.h"
#include "mempool/Pool.h"
#include "mempool/PoolMeta.h"
#include "rdma/MemoryRegion.h"
#include "transaction/Txn.h"

struct BufferAllocationParam {
    char* log_buffer_base;
    size_t log_buffer_size;
    size_t log_size_per_thread;
    char* data_buffer_base;
    size_t data_buffer_size;
};

class Benchmark {
   public:
    static constexpr size_t kMaxThreadNum = 64;

    static const int kRecordMax = 10000000;

   public:
    struct BenchmarkConfig {
        std::string workload;                   // name of the workload
        int thread_num;                         // number of threads to execute
        int coro_num;                           // number of coroutine for each thread
        int dura;                               // duration to run the benchmark (in seconds)
        int txn_num;                            // Execute #txn_num transactions
        NodeInitAttr node_init_attr;            // Information to start context on this Node
        std::vector<RemoteNodeAttr> pool_attr;  // Information to contact other nodes
        bool replay;
        void* workload_config;   // Workload-specific configuration
        std::string output_dir;  // Path to the output directory

        int NumOfMNs() const { return node_init_attr.num_mns; }

        int NumOfCNs() const { return node_init_attr.num_cns; }

        int ThreadPerCN() const { return thread_num; }

        node_id_t NodeId() const { return node_init_attr.nid; }
    };

    struct ThreadCtx {
        // Initialized by the base class
        ThreadId local_tid;
        ThreadId global_tid;
        Pool* pool;  // Thread local data
        // Pool* pools[8]; // Per-Coroutine Pool
        Statistics* stats;
        TimestampGenerator* ts_generator;
        BufferManager* buffer_manager;
        LogManager* txn_log[8];
        AddressCache* address_cache;
        BenchmarkConfig config;  // configuration for benchmarks
        RecordHandleDB* record_handle_db;

        // Initialize by the derived class
        Db* db;
        void* workload_ctx;
        std::thread* t;

        // pointer to an array recording the transaction each thread has tried
        uint64_t* tried_txn_count;

        // pointer to an array recording the transaction each thread has committed
        uint64_t* committed_txn_count;

        // Latency of the committed transactions
        uint64_t* committed_txn_lat;

        double* tried_txn_thpt;

        double* committed_txn_thpt;

        std::atomic<bool> signal_stop;

        int* txn_latency;

        int* txn_exec_latency;

        int* txn_validate_latency;

        int* txn_commit_latency;

        void Join() { return t->join(); }

        void SignalStop() { signal_stop.store(true); }
    };

   public:
    Benchmark() = default;

    virtual ~Benchmark() {
        if (thread_ctxs_ != nullptr) {
            delete[] thread_ctxs_;
        }
        if (pool_ != nullptr) {
            delete pool_;
        }
        delete memcached_;
    }

    node_id_t MyNodeId() const { return config_.node_init_attr.nid; }

    // The base class initialization only do simple mempool initialization works
    virtual Status Initialize(const BenchmarkConfig& config);

    virtual void PrepareWorkloads() = 0;

    virtual Status Run() = 0;

    bool IsCN() const { return config_.node_init_attr.node_type == kCN; }

    bool IsMN() const { return config_.node_init_attr.node_type == kMN; }

    static void Poll(coro_yield_t& yield, ThreadCtx* t_ctx) {
        auto sched = t_ctx->pool->GetCoroSched();
        while (true) {
            coro_id_t coro_id = -1;
            sched->PollCompletionQueue(&coro_id);
            // Run the next coroutine
            Coroutine* next = sched->Head()->next_coro;
            if (next->coro_id != 0) {
                // Run the next coroutine
                sched->RunCoroutine(next, yield);
            }

            // If all coroutines are done, the poll coroutine should break:
            if (sched->CheckAllCoroutineFinished()) {
                break;
            }
        }
    }

    BufferAllocationParam ParseAllocationParam(const BenchmarkConfig& config,
                                               MemoryRegionToken mr_token);

    void RunStatsReporter(int sec);

    void ReportMergedThreadResults();

    void RunDebugMode(Db* db) {
        INVARIANT(IsMN());
        uint64_t arg_a;
        uint64_t arg_b;
        std::string cmd;
        while (true) {
            std::cout << "input your command:";
            std::cin >> cmd;
            if (cmd == "query") {
                std::cout << "input your argument: <table_id>, <key>\n";
                std::cin >> arg_a >> arg_b;
                TableId tbl_id = static_cast<TableId>(arg_a);
                RecordKey row_key = static_cast<RecordKey>(arg_b);
                if (tbl_id >= db->GetTableNumber()) {
                    std::cout << "TableId out of range" << std::endl;
                    continue;
                }
                std::cout << db->GetTable(tbl_id)->DataString(row_key) << std::endl;
            } else if (cmd == "log") {
                ThreadId tid = static_cast<ThreadId>(arg_a);
                coro_id_t cid = static_cast<coro_id_t>(arg_b);
                std::cout << restorer_->LogEntryToString(tid, cid) << std::endl;
            } else if (cmd == "quit") {
                break;
            }
        }
    }

   protected:
    ThreadCtx* thread_ctxs_ = nullptr;
    int thread_num_;
    uint64_t g_tried_txn_count[kMaxThreadNum][8];
    uint64_t g_committed_txn_count[kMaxThreadNum][8];
    uint64_t g_txn_lat[kMaxThreadNum][kRecordMax];
    double g_committed_txn_thpt[kMaxThreadNum], g_tried_txn_thpt[kMaxThreadNum];
    BenchmarkConfig config_;
    Pool* pool_ = nullptr;
    DbRestorer* restorer_ = nullptr;
    MemcachedWrapper* memcached_ = nullptr;
    BufferAllocationParam buf_allocate_param_;
};
