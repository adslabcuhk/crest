#pragma once

#include <infiniband/verbs_exp.h>

#include <cstddef>
#include <cstring>

#include "TPCC/TpccConstant.h"
#include "common/Config.h"
#include "common/Type.h"
#include "db/AddressCache.h"
#include "db/DbRecord.h"
#include "db/Format.h"
#include "db/Schema.h"
#include "mempool/Coroutine.h"
#include "transaction/Bitset.h"
#include "transaction/Enums.h"
#include "transaction/RecordHandle.h"
#include "transaction/RecordHandleWrapper.h"
#include "transaction/TxnInfo.h"
#include "util/Macros.h"
#include "util/Timer.h"

class Txn;
class TxnRecord {
    friend class Txn;
    friend class LocalRecord;
    friend class CoarseRecordHandle;
    friend class FineRecordHandle;

    // The `DelegationManager` is used for local transaction execution
    // only. After concurrent transactions resolve their conflicts via
    // the RefCounter of LocalRecord, each transaction may need to delegate
    // reads or writes for other transactions. The `DelegationManager`
    // records all these contents, e.g., which columns should this txn reads,
    // locks, or unlocks
    struct DelegationManager {
        // uint64_t dg_ro_bm;                 // bring the data back, `dg` is short for "delegate"
        // uint64_t dg_rw_bm;                 // lock the corresponding columns
        // uint64_t dg_commit_bm;             // inplace update these columns and release locks
        uint64_t dg_commit_version_rw_bm;  // which columns are modified
        bool dg_read_record;               // read the whole record
        bool dg_read_from_cache;           // read the record from cache
        bool dg_rw_record;                 // lock the whole record
        bool dg_commit_record;             // commit the whole record and release lock
        bool dg_lock_acquired;             // If the dg_lock has been acquired
        bool dg_commit_base_version;       // the version for dg commit is the base version
        char* dg_commit_data;              // The pointer to the data that should be committed

        ColumnSet dg_ro_columns;
        ColumnSet dg_rw_columns;
        ColumnSet dg_commit_columns;

        DelegationManager()
            : dg_ro_columns(),
              dg_rw_columns(),
              dg_commit_columns(),
              dg_commit_version_rw_bm(0),
              dg_read_record(false),
              dg_read_from_cache(false),
              dg_rw_record(false),
              dg_commit_record(false),
              dg_lock_acquired(false),
              dg_commit_base_version(false),
              dg_commit_data(nullptr) {}

        DelegationManager(const DelegationManager&) = default;
        DelegationManager& operator=(const DelegationManager&) = default;
    };

   public:
    TxnRecord(TableId table_id, RecordKey record_key, Txn* txn, TxnSchema* txn_schema,
              RecordValueSchema* record_schema, AccessMode access_mode)
        : table_id_(table_id),
          record_key_(record_key),
          access_status_(RecordAccessStatus::INIT),
          dg_access_status_(RecordAccessStatus::INIT),
          txn_(txn),
          record_content_(txn_schema),
          record_schema_(record_schema),
          access_mode_(access_mode),
          rw_columns_(),
          ro_columns_(),
          commit_columns_(),
          remote_validate_columns_(),
          cc_level_(txn_schema->GetTableCCLevel()),
          local_lock_status_(LockStatus::NO_LOCK),
          remote_lock_status_(LockStatus::NO_LOCK),
          finished_(false) {}

   private:
    struct RecordContent {
        char* data_;
        char* header_;
        char* validate_data_;
        char* lock_data_;
        char* prev_version_data_;
        char* base_version_data_;
        TxnSchema* txn_schema_;

        RecordContent(TxnSchema* txn_schema)
            : data_(nullptr),
              header_(nullptr),
              validate_data_(nullptr),
              lock_data_(nullptr),
              prev_version_data_(nullptr),
              base_version_data_(nullptr),
              txn_schema_(txn_schema) {}

        RecordHeader* GetRecordHeader() const { return (RecordHeader*)data_; }

        RecordHeader* GetValidateHeader() const { return (RecordHeader*)validate_data_; }

        ColumnHeader* GetHeader(ColumnId cid) const {
            ColumnHeader* col_header = (ColumnHeader*)(data_ + txn_schema_->ColumnOffset(cid));
            return col_header;
        }

        ColumnHeader* GetPreviousVersionHeader(ColumnId cid) const {
            ColumnHeader* col_header =
                (ColumnHeader*)(prev_version_data_ + txn_schema_->ColumnOffset(cid));
            return col_header;
        }

        ColumnHeader* GetBaseVersionHeader(ColumnId cid) const {
            ColumnHeader* col_header =
                (ColumnHeader*)(base_version_data_ + txn_schema_->ColumnOffset(cid));
            return col_header;
        }

        ColumnHeader* GetValidateHeader(ColumnId cid) const {
            ColumnHeader* col_header =
                (ColumnHeader*)(validate_data_ + txn_schema_->ColumnOffset(cid));
            return col_header;
        }

        void CopyTo(char* dst, ColumnId cid) {
            std::memcpy(dst, GetHeader(cid), txn_schema_->ColumnMemSize(cid));
        }

        void CopyFrom(const char* src, ColumnId cid) {
            std::memcpy(GetHeader(cid), src, txn_schema_->ColumnMemSize(cid));
        }
    };

   public:
    // some getter and setter functions for manipulating internal data fields
    TableId table_id() const { return table_id_; }
    RecordKey record_key() const { return record_key_; }

    RecordAccessStatus access_status() const { return access_status_; }
    void set_access_status(RecordAccessStatus access_status) { access_status_ = access_status; }

    RecordAccessStatus dg_access_status() const { return dg_access_status_; }
    void set_dg_access_status(RecordAccessStatus access_status) {
        dg_access_status_ = access_status;
    }

    // Return corresponding bitsets
    ColumnSet rw_columns() const { return rw_columns_; }
    ColumnSet ro_columns() const { return ro_columns_; }
    ColumnSet commit_columns() const { return commit_columns_; }
    ColumnSet access_columns() const { return ColumnSet::Union(rw_columns_, ro_columns_); }
    ColumnSet dg_lock_columns() const { return dg_manager_.dg_rw_columns; }
    ColumnSet dg_rd_columns() const { return dg_manager_.dg_ro_columns; }

    void set_data(char* d);

    char* get_mr_bound() const;

    void UpdateTxnAccessedTimestamp(timestamp_t ts) const;

    char* get_data() const { return this->record_content_.data_; }
    char* get_record_header() const { return this->record_content_.header_; }

    void set_validate_data(char* d) { this->record_content_.validate_data_ = d; }
    char* get_validate_data() const { return this->record_content_.validate_data_; }

    void set_lock_data(char* d) { this->record_content_.lock_data_ = d; }
    char* get_lock_data() const { return this->record_content_.lock_data_; }

    PoolPtr pool_addr() const { return record_addr_; }
    void set_pool_addr(PoolPtr addr) { record_addr_ = addr; }

    PoolPtr used_flag_addr() const { return record_addr_ + offsetof(RecordHeader, used); }
    PoolPtr lock_addr() const { return record_addr_ + offsetof(RecordHeader, lock); }

    TxnSchema* txn_schema() const { return record_content_.txn_schema_; }

    size_t record_size() const { return txn_schema()->GetRecordMemSize(); }

    uint32_t column_count() const { return txn_schema()->GetColumnCount(); }

    void set_record_handle(RecordHandleWrapper hdl) { record_handle_ = hdl; }

   public:
    bool has_write() const { return !rw_columns_.Empty(); }

    bool has_read() const { return !rw_columns_.Empty() || !ro_columns_.Empty(); }

    bool is_insert() const { return access_mode_ == AccessMode::INSERT; }

    bool is_readonly() const { return !is_insert() && !has_write(); }

    bool read_done() const {
        // The data of this record has been fetched into private buffer
        return access_status_ == RecordAccessStatus::READ_DONE;
    }

    bool dg_read_done() const { return dg_access_status_ == RecordAccessStatus::READ_DONE; }

    // --------------- Bitmap operations ---------------------------

    // BIT_ONE = 100000....0000, 63 zeros
    static constexpr uint64_t BIT_ONE = (1ULL << 63);

    bool is_accessed(uint64_t bm, ColumnId cid) { return bm & (BIT_ONE >> cid); }

    bool is_rw_column(ColumnId cid) const { return rw_columns_.Contain(cid); }

    bool is_ro_column(ColumnId cid) const { return ro_columns_.Contain(cid); }

    bool is_commit_column(ColumnId cid) const { return commit_columns_.Contain(cid); }

    bool is_accessed(ColumnId cid) const {
        return is_insert() || is_rw_column(cid) || is_ro_column(cid);
    }

    bool is_dg_rw_column(ColumnId cid) const { return dg_manager_.dg_rw_columns.Contain(cid); }

    bool is_dg_ro_column(ColumnId cid) const { return dg_manager_.dg_ro_columns.Contain(cid); }

    bool is_dg_commit_column(ColumnId cid) const {
        return (dg_manager_.dg_commit_columns.Uint64() & dg_manager_.dg_commit_version_rw_bm &
                (BIT_ONE >> cid));
    }

    void add_rw_column(ColumnId cid) { rw_columns_.Add(cid); }

    void add_ro_column(ColumnId cid) { ro_columns_.Add(cid); }

    void add_commit_column(ColumnId cid) { commit_columns_.Add(cid); }

    void add_rw_columns(uint64_t bms) { rw_columns_.Add(bms); }

    void add_ro_columns(uint64_t bms) { ro_columns_.Add(bms); }

    bool is_column_cc() const { return cc_level_ == TableCCLevel::CELL_LEVEL; }

    bool is_record_cc() const { return cc_level_ == TableCCLevel::RECORD_LEVEL; }

    // bool is_dg_commit_column(ColumnId cid) {
    //   return dg_manager_.dg_commit_version_rw_bm & (BIT_ONE >> cid);
    // }

    // -------------------- For Operation Delegation ----------------------
    void add_dg_ro_column(ColumnId cid) { dg_manager_.dg_ro_columns.Add(cid); }

    void add_dg_rw_column(ColumnId cid) { dg_manager_.dg_rw_columns.Add(cid); }

    void add_dg_commit_column(ColumnId cid) { dg_manager_.dg_commit_columns.Add(cid); }

    void add_dg_commit_columns(uint64_t bms) { dg_manager_.dg_commit_columns.Add(bms); }

    void set_dg_read_record() { dg_manager_.dg_read_record = true; }

    void set_dg_rw_record() {
        dg_manager_.dg_rw_record = true;
        dg_manager_.dg_rw_columns = rw_columns_;
    }

    void set_dg_read_from_cache() { dg_manager_.dg_read_from_cache = true; }

    void set_dg_commit_record() { dg_manager_.dg_commit_record = true; }

    void set_dg_commit_rw_bitmap(uint64_t bm) { dg_manager_.dg_commit_version_rw_bm = bm; }

    // The transaction either needs to lock the entire record, or lock some specific columns
    bool need_dg_lock() const {
        return dg_manager_.dg_rw_record || !dg_manager_.dg_rw_columns.Empty();
    }

    // The transaction needs to read the record
    bool need_dg_read() const {
        return dg_manager_.dg_read_record || !dg_manager_.dg_ro_columns.Empty();
    }

    // The transaction either needs to write the entire record, or some specific columns
    bool need_dg_commit() const {
        return dg_manager_.dg_commit_record || !dg_manager_.dg_commit_columns.Empty();
    }

    bool read_from_cache() const { return dg_manager_.dg_read_from_cache; }

    void set_dg_lock_acquired() { dg_manager_.dg_lock_acquired = true; }
    bool is_dg_lock_acquired() const { return dg_manager_.dg_lock_acquired; }

    void set_dg_commit_base_version() { dg_manager_.dg_commit_base_version = true; }
    bool is_dg_commit_base_version() const { return dg_manager_.dg_commit_base_version; }

    void set_dg_commit_data(char* d) { dg_manager_.dg_commit_data = d; }
    char* get_dg_commit_data() const { return dg_manager_.dg_commit_data; }

    bool is_finished() const { return finished_; }

    ThreadId thread_id() const;

    coro_id_t coro_id() const;

    TxnId txn_id() const;

    std::string txn_name() const;

    // --------------------- Delegation Operation Done ------------------------

    void write_record_header() {
        RecordHeader* record_header = (RecordHeader*)(record_content_.header_);
        record_header->record_key = record_key();
        record_header->SetInuse();
        record_header->lock = 0;
        std::memset(record_header->sub_versions, 0, sizeof(record_header->sub_versions));
    }

    void finish();

    void add_ro_columns(const std::vector<ColumnId>& cids) {
        for (const auto& cid : cids) {
            this->add_ro_column(cid);
        }
    }

    // For locking
    uint64_t rw_bitmap() const { return rw_columns_.Uint64(); }
    uint64_t ro_bitmap() const { return ro_columns_.Uint64(); }
    uint64_t commit_bitmap() const { return commit_columns_.Uint64(); }
    uint64_t access_bitmap() const { return rw_columns_.Uint64() | ro_columns_.Uint64(); }

    uint64_t dg_lock_bitmap() const { return dg_manager_.dg_rw_columns.Uint64(); }
    uint64_t dg_ro_bitmap() const { return dg_manager_.dg_ro_columns.Uint64(); }
    uint64_t dg_commit_bitmap() const { return dg_manager_.dg_commit_columns.Uint64(); }
    uint64_t dg_commit_version_bitmap() const { return dg_manager_.dg_commit_version_rw_bm; }

    // --------------- Bitmap operations --------------------------

    bool is_remote_locked() const { return remote_lock_status_ == LockStatus::SUCC_LOCK; }
    void set_remote_locked() { this->remote_lock_status_ = LockStatus::SUCC_LOCK; }

    void commit_local_version() {
        // It simply marks the latest version as valid:
        record_handle_.update_commit_index(record_version_idx_);
    }

   public:
    // Access the record contents
    template <typename T>
    T Get(ColumnId cid) const {
        ColumnHeader* header = record_content_.GetHeader(cid);
        UpdateTxnAccessedTimestamp(Timestamp(header->version));
        char* d = header->ColumnData();
        return *reinterpret_cast<T*>(d);
    }

    // Access the address of a specific column
    void* GetAddress(ColumnId cid) const { return record_content_.GetHeader(cid)->ColumnData(); }

    void* GetPreviousVersionAddress(ColumnId cid) const {
        return record_content_.GetPreviousVersionHeader(cid)->ColumnData();
    }

    void* GetBaseVersionAddr(ColumnId cid) const {
        return record_content_.GetBaseVersionHeader(cid)->ColumnData();
    }

    // Write the specific column to be new values
    template <typename T>
    void Write(ColumnId cid, T v) {
        ColumnHeader* header = record_content_.GetHeader(cid);
        UpdateTxnAccessedTimestamp(Timestamp(header->version));
        char* d = header->ColumnData();
        *reinterpret_cast<T*>(d) = v;
        add_commit_column(cid);
    }

    // Construct from a DbRecord, this is used for insertion record
    void Write(const DbRecord& value);

    // Dump all contents of this txn_record to a flat representation of
    // this data record
    void Get(DbRecord* value);

    void Get(char* dst);

    // Write the latest value of modified columns as a log, return the
    // position of the log pointer
    char* WriteLog(char* dst);

    RedoEntry* GetDependentRedoEntry() const { return dependent_redo_entry_; }

    void SetDependentRedoEntry(RedoEntry* entry) { dependent_redo_entry_ = entry; }

    RedoEntry* GetCommitVersionRedoEntry() const { return commit_version_redo_entry_; }

    void SetCommitVersionRedoEntry(RedoEntry* entry) { commit_version_redo_entry_ = entry; }

    RedoEntry* GetMyRedoEntry() const;

    // Recover the TxnRecord from a given memory location
    const char* ReadFromLog(const char* src);

    // Update the column version for each record
    void update_version(version_t v);

    void UpdateVersions(timestamp_t ts);

    timestamp_t GetTsExec() const;

    // Return the version of a specific column
    version_t get_version(ColumnId cid) const {
        INVARIANT(record_content_.data_ != nullptr);
        return record_content_.GetHeader(cid)->version;
    }

    // Return the validation version of a specific column
    version_t get_validate_version(ColumnId cid) const {
        INVARIANT(record_content_.validate_data_ != nullptr);
        return record_content_.GetValidateHeader(cid)->version;
    }

    // Return the version of the whole record, this is used for
    // record-level concurrency control only
    version_t get_record_version() const { return record_content_.GetHeader(0)->version; }

    version_t get_record_validate_version() const {
        return 0;
    }

    void set_record_version_index(int i) { record_version_idx_ = i; }

    int get_record_version_index() const { return record_version_idx_; }

    void AddDependentTransaction(TxnInfo* txn_info);

    TxnInfo* GetTxnInfo() const;

   public:
    //
    // The following functions are used for non-localized execution version only
    //
    // Check if the record fetched back is really the record we want
    bool CheckRecordKeyMatch() const;

    // Check the result of RDMA-CAS execution result.
    bool CheckLockAcquired() const;

    // Check the result of RDMA-CAS execution result for acquiring the
    // used flag of a record (for insertion)
    bool CheckUsedFlagAcquired() const;

    // Check that all read-only columns are not locked by another transaction
    bool CheckROColumnsUnlocked() const;

    // Check that all read-only columns belonging to the same consistent snapshot
    bool CheckConsistentSnapshot() const;

    bool CheckRemoteDataReady() const;

    // Check the version of read-only columns are not changed during execution
    bool CheckValidationVersionMatch() const;

    bool CheckValidationSubVersionMatch();

    // For localized execution case:
    bool CheckDelegatedLockAcquire() const;

    bool CheckDelegationROColumnsUnlocked() const;

    CheckStatus CheckLocalDataReady();

    // Local Access:
    void AcquireLocalLock() {
        INVARIANT(record_handle_.valid());
        // Check double lock
        INVARIANT(local_lock_status_ != LockStatus::SUCC_LOCK);
        if (is_readonly()) {
            record_handle_.acquire_lock(LockMode::RO);
        } else {
            record_handle_.acquire_lock(LockMode::RW);
        }
        local_lock_status_ = LockStatus::SUCC_LOCK;
    }

    void ReleaseLocalLock() {
        INVARIANT(record_handle_.valid());
        INVARIANT(local_lock_status_ == LockStatus::SUCC_LOCK);
        if (is_readonly()) {
            record_handle_.release_lock(LockMode::RO);
        } else {
            record_handle_.release_lock(LockMode::RW);
        }
        local_lock_status_ = LockStatus::NO_LOCK;
    }

#ifdef DEBUG_PAYMENT
   public:
    double rd_w_ytd = 0.0;

    double wr_w_ytd = 0.0;

    double rd_d_ytd = 0.0;

    double wr_d_ytd = 0.0;

    double get_rd_w_ytd() const { return rd_w_ytd; }

    double get_wr_w_ytd() const { return wr_w_ytd; }

    double get_rd_d_ytd() const { return rd_d_ytd; }

    double get_wr_d_ytd() const { return wr_d_ytd; }
#endif

   private:
    TableId table_id_;
    RecordKey record_key_;
    RecordAccessStatus access_status_, dg_access_status_;
    AccessMode access_mode_;
    PoolPtr record_addr_;  // The PRIMARY address of this record in the pool
    RecordValueSchema* record_schema_;
    DbRecord const_value_;
    Txn* txn_;
    TableCCLevel cc_level_;
    RecordContent record_content_;

    ColumnSet rw_columns_;
    ColumnSet ro_columns_;
    ColumnSet commit_columns_;
    ColumnSet remote_validate_columns_;

    LockStatus local_lock_status_;
    LockStatus remote_lock_status_;
    bool finished_;  // If the transaction has finished the execution

    int record_version_idx_;
    RecordHandleWrapper record_handle_;

    DelegationManager dg_manager_;
    RedoEntry* dependent_redo_entry_ = nullptr;
    RedoEntry* commit_version_redo_entry_ = nullptr;
};

// Specialized implementation for VarChar type
template <>
inline char* TxnRecord::Get<char*>(ColumnId cid) const {
    ColumnHeader* header = record_content_.GetHeader(cid);
    UpdateTxnAccessedTimestamp(Timestamp(header->version));
    return record_content_.GetHeader(cid)->ColumnData();
}

template <>
inline void TxnRecord::Write<char*>(ColumnId cid, char* v) {
    ColumnHeader* header = record_content_.GetHeader(cid);
    UpdateTxnAccessedTimestamp(Timestamp(header->version));
    char* d = record_content_.GetHeader(cid)->ColumnData();
    std::strcpy(d, v);
    add_commit_column(cid);
}