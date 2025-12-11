#include "transaction/RecordHandle.h"

#include <cstring>
#include <exception>
#include <type_traits>

#include "common/Type.h"
#include "db/Format.h"
#include "transaction/Bitset.h"
#include "transaction/Enums.h"
#include "transaction/TxnRecord.h"
#include "util/Macros.h"
#include "util/Status.h"

void CoarseRecordHandle::ResetStatus() {
    // [ACQUIRE]: The thread must have hold the ref_lock_
    // INVARIANTS: These reference counters must have been set to be 0s
    INVARIANT(txn_count_ == 0);
    INVARIANT(rw_txn_count_ == 0);
    INVARIANT(reader_count() == 0);
    INVARIANT(writer_count() == 0);

    // Reset Access:
    // Since the reference counter have beeen set to be 0, it's impossible
    // that there are another writers aiming to modify the following fields
    writer_fetched_version_ = BASE_VERSION;
    reader_fetched_version_ = BASE_VERSION;
    lock_status_ = LockStatus::NO_LOCK;
    // NOTE!!!: The commit version is not reset as the transaction may use
    // after the ResetStatus call

    // Reset the Version Management:
    pending_versions_.clear();
    commit_index_.store(INVALID_COMMIT_INDEX);
    last_writer_ = nullptr;
    write_ts_ = BASE_TIMESTAMP;
    read_ts_ = BASE_TIMESTAMP;
}

bool CoarseRecordHandle::Acquire(TxnRecord* txn_record) {
    ref_lock_.Lock();
    if (txn_record->has_write()) {
        int writers = ref_write();
        int readers = ref_read();
        if (writers == 1) {
            txn_record->set_dg_rw_record();
        }
    } else {
        int readers = ref_read();
        if (readers == 1) {
            txn_record->set_dg_read_record();
            // Directly read from cache
            // if (lr->cache_.valid) {
            //   record->set_dg_read_from_cache();
            // }
        }
    }
    ++txn_count_;
    if (txn_record->has_write()) {
        ++rw_txn_count_;
    }
    ref_lock_.Unlock();
    return true;
}

void CoarseRecordHandle::Release(TxnRecord* txn_record) {
    ref_lock_.Lock();
    if (txn_record->has_write()) {
        int writers = unref_write();
        int readers = unref_read();
        // Can only do commit if the LockStatus is set to be SUCC_LOCK
        // which means the current CN holds the lock
        if (writers == 0 && lock_status() == LockStatus::SUCC_LOCK) {
            txn_record->set_dg_commit_record();
            set_writer_version_sync(INVALID_VERSION);
            set_lock_status_sync(LockStatus::NO_LOCK);
        }
    } else {
        int readers = unref_read();
    }

    --txn_count_;
    if (txn_record->has_write()) {
        --rw_txn_count_;
    }
    if (txn_count_ == 0) {
        bool is_base_version = false;
        RecordVersion rv = GetLatestCommitVersion(&is_base_version);
        if (!is_base_version) {
            txn_record->set_dg_commit_data(rv.record_data);
            txn_record->add_dg_commit_columns(rv.rw_columns.Uint64());
            txn_record->set_dg_commit_rw_bitmap(rv.rw_columns.Uint64());
            txn_record->SetCommitVersionRedoEntry(rv.GetRedoEntry());
        } else {
            txn_record->set_dg_commit_base_version();
            // The transaction will need to use this bitmap to release locks
            // even though it has no newer version to commit
            txn_record->add_dg_commit_columns(succ_lock_columns());
        }
        ResetStatus();
    }
    ref_lock_.Unlock();
}

void CoarseRecordHandle::UpdateWriteAccessStatus(TxnRecord* txn_record) {
    INVARIANT(txn_record->need_dg_lock());
    INVARIANT(txn_record->is_dg_lock_acquired());
    // There are three fields to update:
    //  * The LockStatus data
    //  * The writer_version
    //  * The init_data fetched from the memory pool
    set_lock_status_sync(LockStatus::SUCC_LOCK);
    rw_lock_.AcquireWriteLock();
    // Lock Upgrade from "reader lock" to "writer lock"
    const char* src = txn_record->record_content_.data_;
    char* dst = init_data_;
    std::memcpy(init_data_, src, txn_schema_->GetRecordMemSize());
    version_t record_version = txn_record->get_record_version();
    set_writer_version_sync(record_version);
    // Since the writer also increments the reader count, we also need to
    // update the reader version
    set_reader_version_sync(record_version);
    rw_lock_.ReleaseWriteLock();
}

void CoarseRecordHandle::UpdateWriteLockStatus(TxnRecord* txn_record, LockStatus ls) {
    INVARIANT(txn_record->need_dg_lock());
    set_lock_status_sync(ls);
}

bool CoarseRecordHandle::UpdateReadAccessStatus(TxnRecord* txn_record, const ColumnSet& bs) {
    INVARIANT(txn_record->need_dg_read());
    rw_lock_.AcquireWriteLock();
    // If there is a writer version, and it is valid (i.e., not base version or
    // invalid version). Then the reader can directly use the writer version
    if (writer_version_valid()) {
        set_reader_version(this->writer_fetched_version_);
        rw_lock_.ReleaseWriteLock();
        return true;
    }
    const char* src = txn_record->record_content_.data_;
    char* dst = init_data_;
    std::memcpy(dst, src, txn_record->record_size());
    version_t record_version = txn_record->get_record_version();
    set_reader_version_sync(record_version);
    rw_lock_.ReleaseWriteLock();
    return true;
}

bool CoarseRecordHandle::UpdateFailReadAccessStatus(TxnRecord* txn_record, const ColumnSet& bs) {
    INVARIANT(txn_record->need_dg_read());
    rw_lock_.AcquireWriteLock();
    // If there is a write version, it means that transaction has held the lock
    if (writer_version_valid()) {
        set_reader_version(this->writer_fetched_version_);
        rw_lock_.ReleaseWriteLock();
        return true;
    }
    // Otherwise, set the version to be an invalid version
    set_reader_version_sync(INVALID_VERSION);
    rw_lock_.ReleaseWriteLock();
    return true;
}

RecordVersion CoarseRecordHandle::GetLatestCommitVersion(bool* is_base_version, int* ci_ptr) {
    int ci = commit_index_.load();
    if (ci == INVALID_COMMIT_INDEX) {
        *is_base_version = true;
        return RecordVersion(nullptr);
    }
    *is_base_version = false;
    if (ci_ptr != nullptr) {
        *ci_ptr = ci;
    }
    ASSERT(ci >= 0 && ci < pending_versions_.size(), "Invalid commit index");
    return pending_versions_[ci];
}

Status CoarseRecordHandle::Access(TxnRecord* txn_record) {
    char* src = nullptr;
    ColumnSet prev_rw_columns, prev_access_columns;
    RedoEntry* dep_redo_entry = nullptr;
    if (pending_versions_.empty()) {
        prev_access_columns = ColumnSet::EmptySet();
        src = nullptr;
    } else {
        RecordVersion prev_version = pending_versions_.back();
        src = prev_version.record_data;
        prev_rw_columns = prev_version.rw_columns;
        prev_access_columns = prev_version.access_columns;
        dep_redo_entry = prev_version.GetRedoEntry();
    }

    Status s = Status::OK();

    // Check the write timestamp of the latest version:
    timestamp_t rts = 0;
    if (pending_versions_.empty()) {
        rts = BASE_TIMESTAMP;
    } else {
        rts = pending_versions_.back().write_ts;
    }
    // A higher version of this record is observed, abort this transaction
    timestamp_t exec_ts = txn_record->GetTsExec();
    if (exec_ts != BASE_TIMESTAMP && exec_ts < rts) {
        s = Status(kTxnError, "Transaction is too old");
        return s;
    }

    // Copy from the previous version
    if (src != nullptr) {
        std::memcpy(txn_record->get_data(), src, txn_schema_->GetRecordMemSize());
    }

    // Check missing column values of prev version
    ColumnSet txn_access_columns = txn_record->access_columns();
    ColumnSet::SingleIterator s_iter = txn_access_columns.Iter();
    while (s_iter.Valid()) {
        ColumnId cid = s_iter.Value();
        if (!prev_access_columns.Contain(cid)) {
            // The transaction can reads the previous version of this column
            bool read_from_write = txn_record->is_rw_column(cid) ||
                                   (txn_record->is_ro_column(cid) && writer_version_valid());
            copy_column_value(txn_record, cid, read_from_write);
        }
        s_iter.Next();
    }
    txn_record->SetDependentRedoEntry(dep_redo_entry);

    // Create a new pending version of this record
    if (txn_record->has_write()) {
        create_version(txn_record, prev_rw_columns, prev_access_columns);
    }

    // Add the last_writer as the dependent transaction:
    txn_record->AddDependentTransaction(last_writer_);
    return s;
}

void CoarseRecordHandle::copy_column_value(TxnRecord* txn_record, ColumnId cid, bool is_writer) {
    const char* src = init_data_ + txn_schema_->ColumnOffset(cid);
    char* dst = txn_record->record_content_.data_ + txn_schema_->ColumnOffset(cid);
    size_t column_size = txn_schema_->ColumnMemSize(cid);
    std::memcpy(dst, src, column_size);
    // setup the SubVersion used during validation:
    RecordHeader* dst_record_hdr = (RecordHeader*)(txn_record->record_content_.data_);
    RecordHeader* src_record_hdr = (RecordHeader*)(init_data_);
    dst_record_hdr->sub_versions[cid] = src_record_hdr->sub_versions[cid];
}

void CoarseRecordHandle::write_column_value(TxnRecord* txn_record, ColumnId cid, bool is_writer) {
    const char* src = txn_record->record_content_.data_ + txn_schema_->ColumnOffset(cid);
    char* dst = init_data_ + txn_schema_->ColumnOffset(cid);
    size_t column_size = txn_schema_->ColumnMemSize(cid);
    std::memcpy(dst, src, column_size);
}

void CoarseRecordHandle::create_version(TxnRecord* txn_record, const ColumnSet& prev_rw_columns,
                                        const ColumnSet& prev_access_columns) {
    pending_versions_.emplace_back(txn_record->get_data());
    txn_record->set_record_version_index(pending_versions_.size() - 1);
    RecordVersion& new_version = pending_versions_.back();
    // We will update the rw_columns when the transaction finishes its execution
    new_version.rw_columns = prev_rw_columns;
    new_version.access_columns =
        ColumnSet::Union(prev_access_columns, txn_record->access_columns());
    new_version.redo_entry = txn_record->GetMyRedoEntry();
    return;
}

void CoarseRecordHandle::UpdateLastWriter(TxnRecord* txn_record) {
    ColumnSet rw_columns = txn_record->rw_columns();
    int v_idx = txn_record->record_version_idx_;
    INVARIANT(v_idx >= 0);
    // Update the RW bitmap
    pending_versions_[v_idx].rw_columns.Add(txn_record->rw_bitmap());
    last_writer_ = txn_record->GetTxnInfo();
}

void CoarseRecordHandle::UpdateWriteTimestamp(timestamp_t ts_exec, int v_idx) {
    pending_versions_[v_idx].write_ts = ts_exec;
}

void FineRecordHandle::ResetStatus() {
    INVARIANT(txn_count_ == 0);
    INVARIANT(rw_txn_count_ == 0);

    const size_t column_count = txn_schema_->GetColumnCount();

    // Reset Reference Counter:
    for (size_t i = 0; i < column_count; ++i) {
        ASSERT(column_writer_count_[i] == 0, "Reset for non-zero rw columns");
        ASSERT(column_reader_count_[i] == 0, "Reset for non-zero ro columns");
    }

    // Reset the Access:
    for (size_t i = 0; i < column_count; ++i) {
        column_lock_status_[i] = LockStatus::NO_LOCK;
        column_reader_fetched_version_[i] = BASE_VERSION;
        column_writer_fetched_version_[i] = BASE_VERSION;
    }

    // Reset the Version Management:
    pending_versions_.clear();
    commit_index_.store(INVALID_COMMIT_INDEX);
    for (size_t i = 0; i < column_count; ++i) {
        column_writer_[i] = nullptr;
    }
    write_ts_ = BASE_TIMESTAMP;
    read_ts_ = BASE_TIMESTAMP;
}

void FineRecordHandle::copy_column_value(TxnRecord* txn_record, ColumnId cid, bool is_writer) {
    const char* src = init_data_ + txn_schema_->ColumnOffset(cid);
    char* dst = txn_record->record_content_.data_ + txn_schema_->ColumnOffset(cid);
    size_t column_size = txn_schema_->ColumnMemSize(cid);
    std::memcpy(dst, src, column_size);
    RecordHeader* dst_record_hdr = (RecordHeader*)(txn_record->record_content_.data_);
    RecordHeader* src_record_hdr = (RecordHeader*)(init_data_);
    dst_record_hdr->sub_versions[cid] = src_record_hdr->sub_versions[cid];
}

void FineRecordHandle::write_column_value(TxnRecord* txn_record, ColumnId cid, bool is_writer) {
    const char* src = txn_record->record_content_.data_ + txn_schema_->ColumnOffset(cid);
    char* dst = init_data_ + txn_schema_->ColumnOffset(cid);
    size_t column_size = txn_schema_->ColumnMemSize(cid);
    std::memcpy(dst, src, column_size);
    RecordHeader* dst_record_hdr = (RecordHeader*)(txn_record->record_content_.data_);
    RecordHeader* src_record_hdr = (RecordHeader*)(init_data_);
    dst_record_hdr->sub_versions[cid] = src_record_hdr->sub_versions[cid];
}

void FineRecordHandle::create_version(TxnRecord* txn_record, const ColumnSet& prev_rw_columns,
                                      const ColumnSet& prev_access_columns) {
    pending_versions_.emplace_back(txn_record->get_data());
    txn_record->set_record_version_index(pending_versions_.size() - 1);
    RecordVersion& new_version = pending_versions_.back();
    // We will update the rw_columns when the transaction finishes its execution
    new_version.rw_columns = prev_rw_columns;
    new_version.access_columns =
        ColumnSet::Union(prev_access_columns, txn_record->access_columns());
    new_version.redo_entry = txn_record->GetMyRedoEntry();
    return;
}

bool FineRecordHandle::Acquire(TxnRecord* txn_record) {
    ref_lock_.Lock();
    for (ColumnId cid = 0; cid < txn_record->column_count(); ++cid) {
        if (txn_record->is_rw_column(cid)) {
            // For writer, we need to increment the reference number for
            // both write counts and read counts
            int writers = ref_write(cid);
            int readers = ref_read(cid);
            if (writers == 1) {
                txn_record->add_dg_rw_column(cid);
            }
        }
        if (txn_record->is_ro_column(cid)) {
            int readers = ref_read(cid);
            if (readers == 1) {
                txn_record->add_dg_ro_column(cid);
            }
        }
    }
    if (txn_record->has_write()) {
        ++rw_txn_count_;
    }
    ++txn_count_;
    ref_lock_.Unlock();
    return true;
}

void FineRecordHandle::Release(TxnRecord* txn_record) {
    ref_lock_.Lock();
    for (ColumnId cid = 0; cid < txn_record->column_count(); ++cid) {
        if (txn_record->is_rw_column(cid)) {
            int writers = unref_write(cid);
            int readers = unref_read(cid);
            if (writers == 0 && lock_status(cid) == LockStatus::SUCC_LOCK) {
                txn_record->add_dg_commit_column(cid);
                set_writer_version_sync(cid, INVALID_VERSION);
                // The current compute node no longer holds the lock as the last
                // writer has left this epoch
                set_lock_status_sync(cid, LockStatus::NO_LOCK);
            }
        }

        if (txn_record->is_ro_column(cid)) {
            int readers = unref_read(cid);
        }
    }

    if (txn_record->has_write()) {
        --rw_txn_count_;
    }
    --txn_count_;
    if (txn_record->need_dg_commit()) {
        bool is_base_version = false;
        int ci;
        RecordVersion rv = GetLatestCommitVersion(&is_base_version, &ci);
        txn_record->set_dg_commit_rw_bitmap(rv.rw_columns.Uint64());
        if (!is_base_version) {
            txn_record->set_dg_commit_data(rv.record_data);
            ASSERT(rv.record_data >= txn_record->get_mr_bound(), "");
            txn_record->SetCommitVersionRedoEntry(rv.GetRedoEntry());
        } else {
            txn_record->set_dg_commit_base_version();
        }
    }
    // No other transactions are referring to this record
    if (txn_count_ == 0) {
        ResetStatus();
    }
    ref_lock_.Unlock();
}

RecordVersion FineRecordHandle::GetLatestCommitVersion(bool* is_base_version, int* ci_ptr) {
    int ci = commit_index_.load();
    if (ci == INVALID_COMMIT_INDEX) {
        *is_base_version = true;
        return RecordVersion(nullptr);
    }
    *is_base_version = false;
    if (ci_ptr != nullptr) {
        *ci_ptr = ci;
    }
    return pending_versions_[ci];
}

Status FineRecordHandle::Access(TxnRecord* txn_record) {
    char* src = nullptr;
    ColumnSet prev_rw_columns, prev_access_columns;
    RedoEntry* dep_redo_entry = nullptr;
    if (pending_versions_.empty()) {
        prev_access_columns = ColumnSet::EmptySet();
        src = nullptr;
    } else {
        RecordVersion prev_version = pending_versions_.back();
        src = prev_version.record_data;
        prev_rw_columns = prev_version.rw_columns;
        prev_access_columns = prev_version.access_columns;
        dep_redo_entry = prev_version.GetRedoEntry();
    }

    timestamp_t rts = 0;
    if (pending_versions_.empty()) {
        rts = BASE_TIMESTAMP;
    } else {
        rts = pending_versions_.back().write_ts;
    }
    Status s = Status::OK();
    // A higher version of this record is observed, abort this transaction
    timestamp_t txn_exec_ts = txn_record->GetTsExec();
    if (txn_exec_ts != BASE_TIMESTAMP && txn_exec_ts < rts) {
        s = Status(kTxnError, "Transaction is too old");
        return s;
    }

    // Copy from the previous version
    if (src != nullptr) {
        std::memcpy(txn_record->get_data(), src, txn_schema_->GetRecordMemSize());
    }

    // Check missing column values of prev version
    ColumnSet txn_access_columns = txn_record->access_columns();
    ColumnSet::SingleIterator s_iter = txn_access_columns.Iter();
    for (; s_iter.Valid(); s_iter.Next()) {
        ColumnId cid = s_iter.Value();
        if (!prev_access_columns.Contain(cid)) {
            // The transaction can reads the previous version of this column
            bool read_from_write = txn_record->is_rw_column(cid) ||
                                   (txn_record->is_ro_column(cid) && writer_version_valid(cid));
            copy_column_value(txn_record, cid, read_from_write);
        }
        txn_record->AddDependentTransaction(column_writer_[cid]);
    }
    txn_record->SetDependentRedoEntry(dep_redo_entry);

    // Create a new pending version of this record
    if (txn_record->has_write()) {
        create_version(txn_record, prev_rw_columns, prev_access_columns);
    }
    return s;
}

void FineRecordHandle::UpdateWriteAccessStatus(TxnRecord* txn_record) {
    INVARIANT(txn_record->need_dg_lock());
    INVARIANT(txn_record->is_dg_lock_acquired());
    // There are three fields to update:
    //  * The LockStatus data
    //  * The writer_version
    //  * The init_data fetched from the memory pool
    rw_lock_.AcquireWriteLock();
    ColumnSet dg_lock_columns = txn_record->dg_lock_columns();
    ColumnSet::SingleIterator s_iter = dg_lock_columns.Iter();
    for (; s_iter.Valid(); s_iter.Next()) {
        ColumnId cid = s_iter.Value();
        set_lock_status_sync(cid, LockStatus::SUCC_LOCK);
        write_column_value(txn_record, cid, true);
        version_t column_version = txn_record->get_version(cid);
        set_writer_version_sync(cid, column_version);
        // Since the writer also increments the reader ref count, it also needs to
        // set the reader version
        set_reader_version_sync(cid, column_version);
    }
    rw_lock_.ReleaseWriteLock();
}

void FineRecordHandle::UpdateWriteLockStatus(TxnRecord* txn_record, LockStatus ls) {
    INVARIANT(txn_record->need_dg_lock());
    ColumnSet dg_lock_columns = txn_record->dg_lock_columns();
    ColumnSet::SingleIterator s_iter = dg_lock_columns.Iter();
    for (; s_iter.Valid(); s_iter.Next()) {
        ColumnId cid = s_iter.Value();
        set_lock_status_sync(cid, ls);
    }
}

bool FineRecordHandle::UpdateReadAccessStatus(TxnRecord* txn_record, const ColumnSet& bs) {
    INVARIANT(txn_record->need_dg_read());
    const char* src = txn_record->record_content_.data_;
    char* dst = init_data_;
    ColumnSet dg_rd_columns = txn_record->dg_rd_columns();
    ColumnSet::SingleIterator s_iter = dg_rd_columns.Iter();
    rw_lock_.AcquireWriteLock();
    for (; s_iter.Valid(); s_iter.Next()) {
        ColumnId cid = s_iter.Value();
        // The column has already acquired its writer lock, so we can directly
        // use this writer version
        if (writer_version_valid(cid)) {
            set_reader_version(cid, column_writer_fetched_version_[cid]);
            continue;
        }
        write_column_value(txn_record, cid, false);
        version_t column_version = txn_record->get_version(cid);
        set_reader_version_sync(cid, column_version);
    }
    rw_lock_.ReleaseWriteLock();
    return true;
}

bool FineRecordHandle::UpdateFailReadAccessStatus(TxnRecord* txn_record, const ColumnSet& bs) {
    INVARIANT(txn_record->need_dg_read());
    const char* src = txn_record->record_content_.data_;
    char* dst = init_data_;
    ColumnSet dg_rd_columns = txn_record->dg_rd_columns();
    ColumnSet::SingleIterator s_iter = dg_rd_columns.Iter();
    rw_lock_.AcquireWriteLock();
    for (; s_iter.Valid(); s_iter.Next()) {
        ColumnId cid = s_iter.Value();
        // The column has already acquired its writer lock, so we can directly
        // use this writer version
        if (writer_version_valid(cid)) {
            set_reader_version(cid, column_writer_fetched_version_[cid]);
            continue;
        }
        set_reader_version_sync(cid, INVALID_VERSION);
    }
    rw_lock_.ReleaseWriteLock();
    return true;
}

void FineRecordHandle::UpdateLastWriter(TxnRecord* txn_record) {
    ColumnSet commit_cols = txn_record->commit_columns();
    ColumnSet::SingleIterator s_iter = commit_cols.Iter();
    for (; s_iter.Valid(); s_iter.Next()) {
        ColumnId cid = s_iter.Value();
        column_writer_[cid] = txn_record->GetTxnInfo();
    }

    // Update the read-write bitmap of the corresponding version
    int v_idx = txn_record->record_version_idx_;
    INVARIANT(v_idx >= 0);
    pending_versions_[v_idx].rw_columns.Add(txn_record->rw_bitmap());
}

void FineRecordHandle::UpdateWriteTimestamp(timestamp_t ts_exec, int v_idx) {
    pending_versions_[v_idx].write_ts = ts_exec;
}