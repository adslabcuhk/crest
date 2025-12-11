#pragma once

#include <infiniband/verbs.h>

#include <bitset>
#include <boost/unordered/concurrent_flat_map.hpp>
#include <boost/unordered/concurrent_flat_map_fwd.hpp>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <vector>

#include "common/Config.h"
#include "common/Type.h"
#include "db/Db.h"
#include "db/Format.h"
#include "db/Schema.h"
#include "mempool/Coroutine.h"
#include "rdma/QueuePair.h"
#include "transaction/Bitset.h"
#include "transaction/Enums.h"
#include "transaction/TxnConfig.h"
#include "transaction/TxnInfo.h"
#include "util/Lock.h"
#include "util/Logger.h"
#include "util/Macros.h"

class TxnRecord;
class Txn;
class RedoEntry;

class RecordHandleBase {
   public:
    RecordHandleBase(TableCCLevel cc_level) : cc_level_(cc_level) {}
    virtual ~RecordHandleBase() = default;

    TableCCLevel GetTableCCLevel() const { return cc_level_; }

    bool is_record_cc() const { return cc_level_ == TableCCLevel::RECORD_LEVEL; }

    bool is_cell_cc() const { return cc_level_ == TableCCLevel::CELL_LEVEL; }

   private:
    TableCCLevel cc_level_;
};

struct RecordVersion {
    version_t version;
    char* record_data;
    timestamp_t write_ts{BASE_TIMESTAMP};

    // The rw_bitmap indicates which columns are modified comparing with the
    // BASE VERSION. This bitmap is used to decide which columns should be
    // updated for the committer
    ColumnSet rw_columns;

    // The access_bitmap indicates which columns are accessed in the current version
    // of record. If a column is not included in this bitmap, the transaction needs
    // to fetch this column from the shared record location
    ColumnSet access_columns;

    // Which redo log entry this RecordVersion would be put into
    RedoEntry* redo_entry;

    RecordVersion(char* d)
        : version(INVALID_VERSION), record_data(d), rw_columns(), access_columns() {}

    RecordVersion()
        : version(INVALID_VERSION), record_data(nullptr), rw_columns(), access_columns() {}

    RecordVersion(const RecordVersion&) = default;
    RecordVersion& operator=(const RecordVersion&) = default;

    RedoEntry* GetRedoEntry() const { return redo_entry; }

    void SetRedoEntry(RedoEntry* ent) { this->redo_entry = ent; }
};

class CoarseRecordHandle final : public RecordHandleBase {
    static constexpr int INVALID_COMMIT_INDEX = -1;

   public:
    CoarseRecordHandle(TableId table_id, RecordKey record_key, TxnSchema* txn_schema)
        : RecordHandleBase(TableCCLevel::RECORD_LEVEL),
          ref_lock_(),
          writer_count_(0),
          reader_count_(0),
          txn_count_(0),
          rw_txn_count_(0),
          access_lock_(),
          lock_status_(LockStatus::NO_LOCK),
          reader_fetched_version_(BASE_VERSION),
          writer_fetched_version_(BASE_VERSION),
          commit_version_(BASE_VERSION),
          rw_lock_(),
          txn_schema_(txn_schema),
          init_data_(nullptr),
          pending_versions_(),
          last_writer_(nullptr),
          commit_index_(INVALID_COMMIT_INDEX) {
        init_data_ = new char[txn_schema_->GetRecordMemSize()];
    }

    CoarseRecordHandle() = delete;
    CoarseRecordHandle(const CoarseRecordHandle&) = delete;
    CoarseRecordHandle& operator=(const CoarseRecordHandle&) = delete;

    ~CoarseRecordHandle() override { delete[] init_data_; }

   public:
    // This function reset all the status of the RecordHandle back to an init
    // status. i.e., clear all reference counter, lock status and saved versions
    void ResetStatus();

    // The transaction calls "Acquire" to resolve possible conflicts with other
    // concurrent transaction referencing the same record. Return true if the
    // transaction successfully acquires the record, otherwise return false
    bool Acquire(TxnRecord* txn_record);

    // The tranaction calls "Release" to notify that it will no longer access
    // this local record any, and the reference counter is decremented by 1
    void Release(TxnRecord* txn_record);

    // The transaction call "UpdateAccessStatus" to update the related fields of
    // in the "Access" fields, including the fetched version, lock status and the
    // column values. Note that the transaction can only update the local values
    // if it has checked the concurrency control related metadata (e.g., locks and
    // SubVersions)
    void UpdateWriteAccessStatus(TxnRecord* txn_record);

    // The transaction calls "UpdateWriteLockStatus" to set the status of this lock
    void UpdateWriteLockStatus(TxnRecord* txn_record, LockStatus ls);

    bool UpdateReadAccessStatus(TxnRecord* txn_record, const ColumnSet& bs);

    bool UpdateFailReadAccessStatus(TxnRecord* txn_record, const ColumnSet& bs);

    void UpdateLastWriter(TxnRecord* txn_record);

    void UpdateWriteTimestamp(timestamp_t ts_exec, int v_idx);

    // This function returns the latest commitable version of this record. The
    // transaction will use this version to do in-place update of records
    RecordVersion GetLatestCommitVersion(bool* is_base_version, int* ci_ptr = nullptr);

    // The transaction access the latest created version of this record via this
    // function call.
    // [ACQUIRE]: The thread should have acquired the reader-writer lock of this
    // reocrd before calling this function
    Status Access(TxnRecord* txn_record);

   public:
    int16_t writer_count() const { return writer_count_; }

    int16_t ref_write() {
        int16_t ref = ++writer_count_;
        INVARIANT(ref >= 0);
        return ref;
    }

    int16_t unref_write() {
        int16_t ref = --writer_count_;
        INVARIANT(ref >= 0);
        return ref;
    }

    int16_t reader_count() const { return reader_count_; }

    int16_t ref_read() {
        int16_t ref = ++reader_count_;
        INVARIANT(ref >= 0);
        return ref;
    }

    int16_t unref_read() {
        int ref = --reader_count_;
        INVARIANT(ref >= 0);
        return ref;
    }

    version_t reader_version() const { return ATOMIC_LOAD(&reader_fetched_version_); }

    void set_reader_version(version_t v) {
        INVARIANT(v != BASE_VERSION);
        reader_fetched_version_ = v;
    }

    void set_reader_version_sync(version_t v) {
        INVARIANT(v != BASE_VERSION);
        ATOMIC_STORE(&reader_fetched_version_, v);
    }

    version_t writer_version() const { return ATOMIC_LOAD(&writer_fetched_version_); }

    void set_writer_version(version_t v) {
        INVARIANT(v != BASE_VERSION);
        writer_fetched_version_ = v;
    }

    void set_writer_version_sync(version_t v) {
        INVARIANT(v != BASE_VERSION);
        ATOMIC_STORE(&writer_fetched_version_, v);
    }

    LockStatus lock_status() const { return ATOMIC_LOAD(&lock_status_); }

    void set_lock_status(LockStatus ls) { lock_status_ = ls; }

    void set_lock_status_sync(LockStatus ls) { ATOMIC_STORE(&lock_status_, ls); }

    version_t commit_version() const { return ATOMIC_LOAD(&commit_version_); }

    void update_commit_version(version_t v) {
        // This function implements a MAX register that only accepts a value larger
        // than the register's current value
        version_t old_version = commit_version();
        timestamp_t ts = Timestamp(v);
        while (Timestamp(old_version) < ts &&
               !ATOMIC_COMPARE_AND_SWAP(&commit_version_, old_version, v)) {
            old_version = commit_version();
        }
        v = commit_version();
    }

    void update_commit_index(int idx) {
        int old_index = commit_index_.load();
        while (old_index < idx && !commit_index_.compare_exchange_strong(old_index, idx)) {
            old_index = commit_index_.load();
        }
    }

    void update_version_rw_bitmap(int version_idx, uint64_t bm) {
        pending_versions_[version_idx].rw_columns.Add(bm);
    }

    void update_access_bitmap(int version_idx, uint64_t bm) {
        pending_versions_[version_idx].access_columns.Add(bm);
    }

    uint64_t succ_lock_columns() const {
        return (0x01ULL << 63);
    }

    bool writer_version_valid() {
        version_t v = writer_version();
        return (v != BASE_VERSION) && (v != INVALID_VERSION);
    }

    void acquire_lock(LockMode mode) {
        if (mode == LockMode::RW) {
            return rw_lock_.AcquireWriteLock();
        } else if (mode == LockMode::RO) {
            return rw_lock_.AcquireReadLock();
        }
        ASSERT(false, "Invalid control flow");
    }

    void release_lock(LockMode mode) {
        if (mode == LockMode::RW) {
            return rw_lock_.ReleaseWriteLock();
        } else if (mode == LockMode::RO) {
            return rw_lock_.ReleaseReadLock();
        }
        ASSERT(false, "Invalid control flow");
    }

   public:
    // This function copies the values of a specific column from the base version
    // from base_data -> txn_record
    void copy_column_value(TxnRecord* txn_record, ColumnId cid, bool is_writer);

    // This function copies the values of a specific column from the record to the
    // base version: txn_record -> base_data
    void write_column_value(TxnRecord* txn_record, ColumnId cid, bool is_writer);

    // Transaction calls this function to create a pending version of this record.
    // The transaction specifies the ``prev_rw_columns'' and ``prev_access_columns''
    // for this pending version. The function then updates the corresponding columns
    // for this newly-created version
    void create_version(TxnRecord* txn_record, const ColumnSet& prev_rw_columns,
                        const ColumnSet& prev_access_columns);

   private:
    // Reference Counter:
    util::SpinLock ref_lock_;  // Used for synchronizing internal data members
    int16_t writer_count_;     // Number of writers referencing this record
    int16_t reader_count_;     // Number of readers referencing this record
    int16_t txn_count_;        // Number of transactions referring this record
    int16_t rw_txn_count_;     // Number of transactions that have write access

    // Remote Access:
    util::SpinLock access_lock_;  // Used for synchronizing Access related data members
    LockStatus lock_status_;
    version_t reader_fetched_version_;
    version_t writer_fetched_version_;
    version_t commit_version_;

    // Local Version Management:
    util::RWLock rw_lock_;
    std::vector<RecordVersion> pending_versions_;  // The version created but not committed
    TxnInfo* last_writer_;                         // The last writer of this record
    std::atomic<int> commit_index_;                // The index of the latest commitable value
    timestamp_t write_ts_, read_ts_;               // The timestamp for latest reader and writer
    TxnSchema* txn_schema_;
    char* init_data_;  // The first data read from the memory pool
};

static constexpr size_t RecordHandleCoarseSize = sizeof(CoarseRecordHandle);

class FineRecordHandle final : public RecordHandleBase {
    static constexpr size_t MAX_ALLOCATE_ATTRIBUTE_NUM = 24;
    static constexpr int INVALID_COMMIT_INDEX = -1;

   public:
    FineRecordHandle(TableId table_id, RecordKey record_key, TxnSchema* txn_schema)
        : RecordHandleBase(TableCCLevel::CELL_LEVEL),
          ref_lock_(),
          column_writer_count_(),
          column_reader_count_(),
          txn_count_(0),
          rw_txn_count_(0),
          access_lock_(),
          column_lock_status_(),
          column_reader_fetched_version_(),
          column_writer_fetched_version_(),
          commit_version_(),
          rw_lock_(),
          txn_schema_(txn_schema),
          init_data_(nullptr),
          pending_versions_(),
          commit_index_(INVALID_COMMIT_INDEX),
          write_ts_(BASE_TIMESTAMP),
          read_ts_(BASE_TIMESTAMP) {
        init_data_ = new char[txn_schema_->GetRecordMemSize()];
        for (size_t i = 0; i < MAX_ALLOCATE_ATTRIBUTE_NUM; ++i) {
            column_writer_[i] = nullptr;
        }
        pending_versions_.reserve(64);
    }

    FineRecordHandle(const FineRecordHandle&) = delete;
    FineRecordHandle& operator=(const FineRecordHandle&) = delete;
    ~FineRecordHandle() override { delete[] init_data_; }

   public:
    // This function reset all the status of the RecordHandle back to an init
    // status. i.e., clear all reference counter, lock status and saved versions
    void ResetStatus();

    // The transaction calls "Acquire" to resolve possible conflicts with other
    // concurrent transaction referencing the same record. Return true if the
    // transaction successfully acquires the record, otherwise return false
    bool Acquire(TxnRecord* txn_record);

    // The tranaction calls "Release" to notify that it will no longer access
    // this local record any, and the reference counter is decremented by 1
    void Release(TxnRecord* txn_record);

    // This function returns the latest commitable version of this record. The
    // transaction will use this version to do in-place update of records
    RecordVersion GetLatestCommitVersion(bool* is_base_version, int* ci_ptr = nullptr);

    // The transaction access the latest created version of this record via this
    // function call.
    // [ACQUIRE]: The thread should have acquired the reader-writer lock of this
    // reocrd before calling this function
    Status Access(TxnRecord* txn_record);

    // void TrackLastWriter(TxnRecord* txn_record);

    // The transaction call "UpdateAccessStatus" to update the related fields of
    // in the "Access" fields, including the fetched version, lock status and the
    // column values. Note that the transaction can only update the local values
    // if it has checked the concurrency control related metadata (e.g., locks and
    // SubVersions)
    void UpdateWriteAccessStatus(TxnRecord* txn_record);
    void UpdateWriteLockStatus(TxnRecord* txn_record, LockStatus ls);

    bool UpdateReadAccessStatus(TxnRecord* txn_record, const ColumnSet& bs);

    bool UpdateFailReadAccessStatus(TxnRecord* txn_record, const ColumnSet& bs);

    void UpdateLastWriter(TxnRecord* txn_record);

    void UpdateWriteTimestamp(timestamp_t ts_exec, int v_idx);

   public:
    int16_t writer_count(ColumnId cid) {
        INVARIANT(cid < MAX_ALLOCATE_ATTRIBUTE_NUM);
        return column_writer_count_[cid];
    }

    int16_t ref_write(ColumnId cid) {
        INVARIANT(cid < MAX_ALLOCATE_ATTRIBUTE_NUM);
        int16_t ref = ++column_writer_count_[cid];
        INVARIANT(ref >= 0);
        return ref;
    }

    int16_t unref_write(ColumnId cid) {
        INVARIANT(cid < MAX_ALLOCATE_ATTRIBUTE_NUM);
        int16_t ref = --column_writer_count_[cid];
        INVARIANT(ref >= 0);
        return ref;
    }

    int16_t reader_count(ColumnId cid) {
        INVARIANT(cid < MAX_ALLOCATE_ATTRIBUTE_NUM);
        return column_reader_count_[cid];
    }

    int16_t ref_read(ColumnId cid) {
        INVARIANT(cid < MAX_ALLOCATE_ATTRIBUTE_NUM);
        int16_t ref = ++column_reader_count_[cid];
        INVARIANT(ref >= 0);
        return ref;
    }

    int16_t unref_read(ColumnId cid) {
        INVARIANT(cid < MAX_ALLOCATE_ATTRIBUTE_NUM);
        int16_t ref = --column_reader_count_[cid];
        INVARIANT(ref >= 0);
        return ref;
    }

    version_t reader_version(ColumnId cid) const {
        INVARIANT(cid < MAX_ALLOCATE_ATTRIBUTE_NUM);
        return ATOMIC_LOAD(&column_reader_fetched_version_[cid]);
    }

    void set_reader_version(ColumnId cid, version_t v) {
        INVARIANT(cid < MAX_ALLOCATE_ATTRIBUTE_NUM);
        INVARIANT(v != BASE_VERSION);
        column_reader_fetched_version_[cid] = v;
    }

    void set_reader_version_sync(ColumnId cid, version_t v) {
        INVARIANT(cid < MAX_ALLOCATE_ATTRIBUTE_NUM);
        INVARIANT(v != BASE_VERSION);
        ATOMIC_STORE(&(column_reader_fetched_version_[cid]), v);
    }

    version_t writer_version(ColumnId cid) const {
        INVARIANT(cid < MAX_ALLOCATE_ATTRIBUTE_NUM);
        return ATOMIC_LOAD(&column_writer_fetched_version_[cid]);
    }

    void set_writer_version(ColumnId cid, version_t v) {
        INVARIANT(cid < MAX_ALLOCATE_ATTRIBUTE_NUM);
        INVARIANT(v != BASE_VERSION);
        column_writer_fetched_version_[cid] = v;
    }

    void set_writer_version_sync(ColumnId cid, version_t v) {
        INVARIANT(cid < MAX_ALLOCATE_ATTRIBUTE_NUM);
        INVARIANT(v != BASE_VERSION);
        ATOMIC_STORE(&(column_writer_fetched_version_[cid]), v);
    }

    LockStatus lock_status(ColumnId cid) const {
        INVARIANT(cid < MAX_ALLOCATE_ATTRIBUTE_NUM);
        return ATOMIC_LOAD(&(column_lock_status_[cid]));
    }

    void set_lock_status(ColumnId cid, LockStatus ls) {
        INVARIANT(cid < MAX_ALLOCATE_ATTRIBUTE_NUM);
        column_lock_status_[cid] = ls;
    }

    void set_lock_status_sync(ColumnId cid, LockStatus ls) {
        INVARIANT(cid < MAX_ALLOCATE_ATTRIBUTE_NUM);
        ATOMIC_STORE(&(column_lock_status_[cid]), ls);
    }

    version_t commit_version(ColumnId cid) const {
        INVARIANT(cid < MAX_ALLOCATE_ATTRIBUTE_NUM);
        return ATOMIC_LOAD(&commit_version_[cid]);
    }

    void update_commit_version(ColumnId cid, version_t v) {
        // This function implements a MAX register that only accepts a value larger
        // than the register's current value
        version_t old_version = commit_version(cid);
        timestamp_t ts = Timestamp(v);
        while (Timestamp(old_version) < ts &&
               !ATOMIC_COMPARE_AND_SWAP(&commit_version_[cid], old_version, v)) {
            old_version = commit_version(cid);
        }
        v = commit_version(cid);
    }

    void update_commit_index(int idx) {
        int old_index = commit_index_.load();
        while (old_index < idx && !commit_index_.compare_exchange_strong(old_index, idx)) {
            old_index = commit_index_.load();
        }
    }

    void update_version_rw_bitmap(int version_idx, uint64_t bm) {
        pending_versions_[version_idx].rw_columns.Add(bm);
    }

    void update_access_bitmap(int version_idx, uint64_t bm) {
        pending_versions_[version_idx].access_columns.Add(bm);
    }

    bool writer_version_valid(ColumnId cid) {
        version_t v = writer_version(cid);
        return (v != BASE_VERSION) && (v != INVALID_VERSION);
    }

    void acquire_lock(LockMode mode) {
        if (mode == LockMode::RW) {
            return rw_lock_.AcquireWriteLock();
        } else if (mode == LockMode::RO) {
            return rw_lock_.AcquireReadLock();
        }
        ASSERT(false, "Invalid control flow");
    }

    void release_lock(LockMode mode) {
        if (mode == LockMode::RW) {
            return rw_lock_.ReleaseWriteLock();
        } else if (mode == LockMode::RO) {
            return rw_lock_.ReleaseReadLock();
        }
        ASSERT(false, "Invalid control flow");
    }

   public:
    // [TODO]: These two functions are basically the same as the CoarseRecordHandle
    // version, consider move them to the base class
    void copy_column_value(TxnRecord* txn_record, ColumnId cid, bool is_writer);

    void write_column_value(TxnRecord* txn_record, ColumnId cid, bool is_writer);

    void create_version(TxnRecord* txn_record, const ColumnSet& prev_rw_columns,
                        const ColumnSet& prev_access_columns);

   private:
    // Reference Counter:
    util::SpinLock ref_lock_;  // Used for synchronizing internal data members
    int16_t column_writer_count_[MAX_ALLOCATE_ATTRIBUTE_NUM];  // Number of writers for each column
    int16_t column_reader_count_[MAX_ALLOCATE_ATTRIBUTE_NUM];  // Number of readers for each column
    int16_t txn_count_;     // Number of transactions referring this record
    int16_t rw_txn_count_;  // Number of transactions that have write access

    // Remote Access:
    util::SpinLock access_lock_;  // Used for synchronizing Access related data members
    LockStatus column_lock_status_[MAX_ALLOCATE_ATTRIBUTE_NUM];
    version_t column_reader_fetched_version_[MAX_ALLOCATE_ATTRIBUTE_NUM];
    version_t column_writer_fetched_version_[MAX_ALLOCATE_ATTRIBUTE_NUM];
    version_t commit_version_[MAX_ALLOCATE_ATTRIBUTE_NUM];

    // Local Version Management:
    util::RWLock rw_lock_;
    std::vector<RecordVersion> pending_versions_;         // The version created but not committed
    TxnInfo* column_writer_[MAX_ALLOCATE_ATTRIBUTE_NUM];  // The last writer of each column
    std::atomic<int> commit_index_;   // The index of the latest commitable value
    timestamp_t write_ts_, read_ts_;  // The timestamp for latest reader and writer
    TxnSchema* txn_schema_;
    char* init_data_;  // The first data read from the memory pool
};

static constexpr size_t FineRecordHandleSize = sizeof(FineRecordHandle);