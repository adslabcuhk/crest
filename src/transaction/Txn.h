#pragma once

#include <absl/container/flat_hash_map.h>
#include <bits/types/struct_FILE.h>

#include <cstddef>
#include <cstdlib>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/Config.h"
#include "common/Type.h"
#include "db/AddressCache.h"
#include "db/Db.h"
#include "db/Format.h"
#include "db/Schema.h"
#include "mempool/BufferManager.h"
#include "mempool/Coroutine.h"
#include "mempool/Pool.h"
#include "rdma/RdmaBatch.h"
#include "transaction/Bitset.h"
#include "transaction/Enums.h"
#include "transaction/LogManager.h"
#include "transaction/RecordHandleIndex.h"
#include "transaction/TxnRecord.h"
#include "util/Logger.h"
#include "util/Macros.h"
#include "util/Status.h"
#include "util/Timer.h"

class TimestampGenerator;

struct TxnRecordRef {
    std::vector<TxnRecord>* c;
    size_t idx;

    TxnRecordRef() = delete;
    TxnRecordRef(std::vector<TxnRecord>* c, size_t idx) : c(c), idx(idx) {}

    TxnRecordRef(const TxnRecordRef&) = default;
    TxnRecordRef& operator=(const TxnRecordRef&) = default;

    TxnRecordRef(TxnRecordRef&&) = default;

    TxnRecord* operator->() {
        ASSERT(idx < c->size(), "");
        return &(c->at(idx));
    }
    TxnRecord* getPtr() {
        ASSERT(idx < c->size(), "");
        return &(c->at(idx));
    }
};

class Txn {
   public:
    enum class TxnPhase {
        PREPARE_PHASE = 1,
        READ_PHASE,
        EXECUTION_PHASE,
        VALIDATION_PHASE,
        COMMIT_PHASE,
        ABORT_PHASE,
        PHASE_MAX_MARKER,
    };

    friend class TxnRecord;

    // Records all Bucket read operations, this may reduce issuing repeated RDMA
    // read operations fetching the same bucket. The bkt_buffer remains valid and
    // accessible until the end of this transaction (either committed or aborted)
    struct BucketRead {
        PoolPtr bkt_ptr;
        char* bkt_buffer;
    };
    typedef std::vector<BucketRead> BucketReadHistory;

    struct TxnPhaseLatency {
        uint32_t dura[static_cast<int>(TxnPhase::PHASE_MAX_MARKER)];
        void Reset() { std::memset(dura, 0, sizeof(dura)); }
    };

    struct TxnDetail {
        // Time spent on different phases of a transaction execution
        // All in nanoseconds
        uint32_t check_batchread_results_time;
        uint32_t wait_batchread_data_dura;
        uint32_t wait_dependent_txn_dura;
        uint32_t local_exec_dura;
        uint32_t leave_batch_time;
        uint32_t read_batch_request_cpu_time;

        int lock_fail_count;
        int join_batch_fail_count;
        int insert_fail_count;
        int read_column_locked_count;
        int read_inconsistent_snapshot_count;
        int release_lock_cnt;
        int lock_cnt;
        int local_shared_record_num;
        int record_handle_hit_count;
        int record_handle_search_count;
        int read_from_cache_count;
        int read_from_remote_count;
        int alloc_buffer_sz;

        void Reset() {
            check_batchread_results_time = 0;
            wait_batchread_data_dura = 0;
            wait_dependent_txn_dura = 0;
            leave_batch_time = 0;

            lock_fail_count = 0;
            insert_fail_count = 0;
            join_batch_fail_count = 0;
            read_column_locked_count = 0;
            read_inconsistent_snapshot_count = 0;
            release_lock_cnt = 0;
            lock_cnt = 0;
            local_shared_record_num = 0;
            record_handle_hit_count = 0;
            record_handle_search_count = 0;
            read_from_cache_count = 0;
            read_from_remote_count = 0;
            read_batch_request_cpu_time = 0;
            alloc_buffer_sz = 0;
        }
    };

    struct RdmaAccessStats {
        int read_cnt;
        int write_cnt;
        int atomic_cnt;
        int read_bytes;
        int write_bytes;
        int atomic_bytes;

        void Reset() {
            read_cnt = 0;
            write_cnt = 0;
            atomic_cnt = 0;
            read_bytes = 0;
            write_bytes = 0;
            atomic_bytes = 0;
        }
    };

    // The IOManager is responsible for read/write records in the memory pool,
    // acquire/release the record/column locks
    struct IOManager {
        coro_id_t coro_id_;
        Pool* pool_;
        BufferManager* buf_manager_;
        Txn* txn_;
        BucketReadHistory bkt_read_hist_;
        Db* db_;

        IOManager(coro_id_t coro_id, Pool* pool, BufferManager* buf_manager, Db* db)
            : coro_id_(coro_id), pool_(pool), buf_manager_(buf_manager), db_(db) {
            Reset();
        }

        IOManager() = delete;
        IOManager(const IOManager&) = delete;
        IOManager& operator=(const IOManager&) = delete;

        void Reset() { bkt_read_hist_.clear(); }

        coro_id_t GetCoroId() { return coro_id_; }

        Status AcquireLockAndRead(TxnRecord* record);

        Status AcquireWriteLock(TxnRecord* record);
        Status ReleaseWriteLock(TxnRecord* record);
        Status ReleaseWriteLockDelegation(TxnRecord* record);

        Status AcquireUsedFlag(TxnRecord* record);
        Status ReleaseUsedFlag(TxnRecord* record);

        Status ReadRecord(TxnRecord* record);
        Status ReadRecordValidation(TxnRecord* record);

        Status WriteRecord(TxnRecord* record, PoolPtr address);
        Status WriteNewRecord(TxnRecord* record, PoolPtr address);

        Status ReadHashBucket(TxnRecord* record, PoolPtr bkt_address);

        Status WriteRedoEntry(RedoEntry* entry);
        Status UpdateLogStatus(RedoEntry* entry);

        bool BucketAlreadyRead(PoolPtr bkt_address, char** bkt_buf);

        Status AllocateBuffer(TxnRecord* record);

        // Status WriteRecordDelegation(TxnRecord* record) {
        //   if (record->is_record_cc()) {
        //     // return WriteRecordDelegationRecordLevel(record);
        //     return WriteRecordDelegationColumnLevel(record);
        //   } else if (record->is_column_cc()) {
        //     return WriteRecordDelegationColumnLevel(record);
        //   } else {
        //     abort();
        //   }
        // }

        Status WriteRecordDelegation(TxnRecord* record, PoolPtr record_address);

        // This is done at the column level
        Status WriteRecordDelegationColumnLevel(TxnRecord* record);

        // This is done at the record level
        Status WriteRecordDelegationRecordLevel(TxnRecord* record);

        Status WriteRecordDelegationPerColumn(TxnRecord* record);

       private:
        template <typename T>
        void write_data(char* d, T v) {
            *reinterpret_cast<T*>(d) = v;
        }
    };

   public:
    // Default and copyable are not allowed
    Txn() = delete;
    Txn(const Txn&) = delete;
    Txn& operator=(const Txn&) = delete;

    // This is the only way the client can create the transaction context
    Txn(TxnId txn_id, ThreadId t_id, coro_id_t coro_id, TxnType txn_type, Pool* pool, Db* db,
        BufferManager* buf_manager, TimestampGenerator* ts_geneartor,
        RecordHandleDB* record_handle_db = nullptr, LogManager* log_manager = nullptr)
        : txn_id_(txn_id),
          t_id_(t_id),
          coro_id_(coro_id),
          txn_type_(txn_type),
          io_(new IOManager(coro_id, pool, buf_manager, db)),
          pool_(pool),
          db_(db),
          buf_manager_(buf_manager),
          ts_generator_(ts_geneartor),
          // local_db_(local_db),
          record_handle_db_(record_handle_db),
          log_manager_(log_manager),
          rdma_batch_(64) {
        start_ts_ = TIMESTAMP_INF;
        commit_ts_ = TIMESTAMP_INF;
        io_->txn_ = this;
    }

   public:
    // ================================ Standard Interfaces ==================================
    void Begin(uint64_t seq, TxnType txn_type, timestamp_t start_ts, const std::string& name = "");

    // SelectRecord returns a reference for accessing and manipulating a specific record in the
    // database.
    // The user should specify the table identifier (`table_id`) and the key of the target record
    // (`record_key`). Specifying the access mode (`access_mode`) is optional. If not provided, it
    // will be inferred by the transaction itself.
    // Parameters:
    //   - table_id: Identifier for the table containing the record.
    //   - record_key: Unique key identifying the record within the table.
    //   - access_mode: Optional. Specifies the desired access mode (e.g., read, write).
    TxnRecordRef SelectRecord(TableId table_id, RecordKey record_key, AccessMode access_mode);

    TxnRecordRef InsertRecord(TableId table_id, RecordKey record_key);

    // The Deref interface fetches and locks the related data records in the memory pool. Then the
    // use may access or manipulate the database records using the returned handler of SelectRecord
    Status Deref(coro_yield_t& yield);

    // The transaction commits all the modifications to the memory pool
    Status Commit(coro_yield_t& yield);

    Status Abort(coro_yield_t& yield);

   private:
    Status Validate(coro_yield_t& yield);
    Status ValidateLocal(coro_yield_t& yield);
    Status ValidateNonLocal(coro_yield_t& yield);
    Status CommitLocalVersion();

    Status CommitWrite(coro_yield_t& yield);
    Status CommitWriteLocal(coro_yield_t& yield);
    Status CommitWriteNonLocal(coro_yield_t& yield);

    Status DerefLocal(coro_yield_t& yield);
    Status DerefNonLocal(coro_yield_t& yield);

    Status LockAndRead(coro_yield_t& yield);

    Status AbortLocal(coro_yield_t& yield);
    Status AbortNonLocal(coro_yield_t& yield);

    Status WritePrimaryRecord(TxnRecord* record);

    Status WriteBackupRecords(TxnRecord* record);

    Status WritePrimaryNewRecord(TxnRecord* record);

    Status WriteBackupNewRecord(TxnRecord* record);

    Status WritePrimaryDelegationRecord(TxnRecord* record);

    Status WriteBackupDelegationRecords(TxnRecord* record);

    CheckResult CheckReadOnlyColumns(TxnRecord* record, ColumnSet* local_columns);

    // ================================ Standard Interfaces Done ==============================

    // ===================== Localization-Related Functions =================

    Status CreateLocalRecords();
    Status DelegateExecute(coro_yield_t& yield);
    Status AccessLocalRecords(coro_yield_t& yield);

    Status DelegateLockAndRead(coro_yield_t& yield, bool* has_rdma_op);

    Status CheckDelegatedReadLockResults(bool last_try);
    Status CheckDelegatedLockResults(bool last_try);
    Status CheckDelegatedReadResults(bool last_try);
    Status CheckDelegatedInsertFlagAcquired(bool last_try);

    // void UpdateDelegationLockStatus(TxnRecord* record, LockStatus s);

    void UpdateDelegationReadData(TxnRecord* record);

    void UpdateDelegationReadDataToLocal(TxnRecord* record, const ColumnSet& bs);

    // void UpdateDelegationLockColumnData(TxnRecord* record);

    Status WriteLatestCommitableVersion(TxnRecord* record);

    // LocalRecord* SelectLocalRecord(TxnRecord* txn_record);

    RecordHandleWrapper SelectRecordHandle(TxnRecord* txn_record);

    void CopyWriterDataToReader(TxnRecord* record, const ColumnSet& copy_columns);

    // ================================ Utility Functions ==================================
   private:
    // The transaction get the pool address of all records, either by reading the hash bucket
    // or by checking the address cache
    Status GetAddress(coro_yield_t& yield);

    Status CheckBucketResults(TxnRecord* record);

    Status CheckLockAndReadResults(bool& insert_fail);

    Status CheckValidationResults();

    Status CheckValidationResultsLocal();

    bool SearchRecordInBucket(TxnRecord* record, char** record_addr);

    bool SearchRecordInsertPos(TxnRecord* record, char** record_addr, bool& already_exists);

    // Log related functions
    Status WriteRedoLog();

    Status UpdateLogStatus(LogStatus s);

    void AddDependentTransaction(TxnInfo* txn_info) { dependent_txn_.push_back(txn_info); }

    void AddDependentTxnTableId(TableId table_id) { dependent_txn_table_ids_.push_back(table_id); }

    bool CheckDependentTxnCommittable(coro_yield_t& yield);

    bool redo_log_written() const { return redo_log_written_; }

    void UnrefDependentLogEntries(bool is_abort);

    // ================================ Utility Functions Done ==============================

   public:
    void UpdateMaxAccessedTimestamp(timestamp_t ts) {
        max_accessed_ts_ = std::max(max_accessed_ts_, ts);
    }
    // Retrive internal data members
    ThreadId GetThreadId() const { return t_id_; }

    coro_id_t GetCoroId() const { return coro_id_; }

    TxnId GetTxnId() const { return txn_id_; }

    void Yield(coro_yield_t& yield) { pool_->YieldForPoll(GetCoroId(), yield); }

    size_t DependentTxnNum() const { return dependent_txn_.size(); }

    timestamp_t GetCommitTimestamp() const { return commit_ts_; }

    const RdmaAccessStats& GetRdmaAccessStats() const { return rdma_access_; }

    const TxnDetail& GetTxnDetail() const { return txn_detail_; }

    const TxnPhaseLatency& GetTxnPhaseLatency() const { return phase_latency_; }

    uint32_t GetPhaseLatency(TxnPhase p) const { return phase_latency_.dura[static_cast<int>(p)]; }

    Db* GetDb() const { return db_; }

    IOManager* IO() { return io_; }

    RecordValueSchema* GetRecordSchema(TableId table_id) const {
        return GetDb()->GetTable(table_id)->GetRecordSchema();
    }

    TxnInfo* txn_info() const { return me_; }

    void CheckPendingRequestNum() const {
#if DEBUG_LOCALIZATION
        coro_id_t coro_id = GetCoroId();
        uint64_t pending_total = 0;
        size_t qp_num = pool_->GetQueueNum();
        CoroutineScheduler* scheduler = pool_->GetCoroSched();
        for (size_t i = 0; i < qp_num; ++i) {
            pending_total += (scheduler->pending_request_num_[coro_id][i]);
        }
        ASSERT(pending_total == 0, "Invalid pending request number");
#endif
    }

#if DEBUG_LOCALIZATION_PERF
    int get_yield_next_count(int idx) { return yield_next_time[idx]; }
#endif

    const std::vector<TableId>& GetDependentTxnTableIds() const { return dependent_txn_table_ids_; }

    uint64_t GetWaitDependentTxnTime() const { return txn_detail_.wait_dependent_txn_dura; }

   private:
    void YieldForPoll(coro_yield_t& yield) { pool_->YieldForPoll(GetCoroId(), yield); }

    void YieldToNext(coro_yield_t& yield) { pool_->YieldToNext(GetCoroId(), yield); }

   public:
    void IncLockFailCount() { txn_detail_.lock_fail_count++; }

    void IncInsertFailCount() { txn_detail_.insert_fail_count++; }

    void IncJoinBatchFailCount() { txn_detail_.join_batch_fail_count++; }

    void IncReadColumnLockedCount() { txn_detail_.read_column_locked_count++; }

    void InConsistentSnapshotCount() { txn_detail_.read_inconsistent_snapshot_count++; }

    void IncAllocBufSize(int sz) { txn_detail_.alloc_buffer_sz += sz; }

    void PHASE_LATENCY_TRACK_START(TxnPhase p) { phase_latency_timer_.reset(); }

    void PHASE_LATENCY_TRACK_END(TxnPhase p) {
        phase_latency_.dura[static_cast<int>(p)] += phase_latency_timer_.ns_elapse();
    }

    void ADD_RDMA_READ_STATS(int cnt, int bytes) {
        rdma_access_.read_cnt += cnt;
        rdma_access_.read_bytes += bytes;
    }

    void ADD_RDMA_WRITE_STATS(int cnt, int bytes) {
        rdma_access_.write_cnt += cnt;
        rdma_access_.write_bytes += bytes;
    }

    void ADD_RDMA_ATOMIC_STATS(int bytes) {
        rdma_access_.atomic_cnt += 1;
        rdma_access_.atomic_bytes += bytes;
    }

   private:
    TxnId txn_id_;                      // Unique identifier of this transaction
    TxnType txn_type_;                  // Transaction Type
    ThreadId t_id_;                     // Thread Id
    coro_id_t coro_id_;                 // The coroutine id
    std::string txn_name_;              // For debugging
    timestamp_t start_ts_, commit_ts_;  // timestamp
    timestamp_t exec_ts_;               // local execution ts
    timestamp_t max_accessed_ts_;       // The max timestamp this transaction has ever see
    Pool* pool_;                        // The memory pool manager
    Db* db_;                            // Context of the whole database
    TxnStatus txn_status_;              // Current status of the transaction
    BufferManager* buf_manager_;        // Manages the RDMA registerred memory
    TimestampGenerator* ts_generator_;  // Generate the commit timestamp
    IOManager* io_;                     // RDMA access
    LogManager* log_manager_;           // The redo log manager
    RedoEntry* redo_entry_;             // The redo entry for current transaction

    // LocalDB* local_db_;                 // The local manager
    RecordHandleDB* record_handle_db_;  // The record handle manager

    std::vector<TxnRecord> selected_records_;
    std::vector<TxnRecord> insert_records_;

    // For tracking transaction dependency
    TxnInfo* me_;
    std::vector<TxnInfo*> dependent_txn_;
    std::vector<TableId> dependent_txn_table_ids_;

    bool committed_;
    bool redo_log_written_;

    // Profiling the transaction execution results
    TxnPhaseLatency phase_latency_;
    util::Timer phase_latency_timer_;
    util::Timer profile_timer_;
    TxnDetail txn_detail_;
    RdmaAccessStats rdma_access_;

    RDMABatch rdma_batch_;

    static constexpr size_t kMaxDbRecordsNum = 32;
    int aborted_count_ = 0;  // records the number of Abort() call

#if DEBUG_LOCALIZATION_PERF
    int yield_next_time[16];
#endif
};
