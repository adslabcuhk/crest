#include "transaction/TxnRecord.h"

#include <exception>

#include "common/Config.h"
#include "common/Type.h"
#include "db/Format.h"
#include "transaction/Enums.h"
#include "transaction/RecordHandle.h"
#include "transaction/Txn.h"
#include "transaction/TxnConfig.h"
#include "util/Logger.h"
#include "util/Macros.h"

ThreadId TxnRecord::thread_id() const { return txn_->t_id_; }

coro_id_t TxnRecord::coro_id() const { return txn_->coro_id_; }

TxnId TxnRecord::txn_id() const { return txn_->txn_id_; }

std::string TxnRecord::txn_name() const { return txn_->txn_name_; }

void TxnRecord::Write(const DbRecord& value) {
    assert(is_insert());
    // Initialize the header:
    RecordHeader* record_header = (RecordHeader*)(record_content_.header_);
    record_header->record_key = record_key();
    record_header->SetInuse();
    record_header->lock = 0;
    std::memset(record_header->sub_versions, 0, sizeof(record_header->sub_versions));

    // Initialize the per-column value
    int col_number = txn_schema()->GetColumnCount();
    for (ColumnId cid = 0; cid < col_number; ++cid) {
        ColumnHeader* c = record_content_.GetHeader(cid);
        value.CopyTo(cid, c->ColumnData());
        add_commit_column(cid);
        record_header->UpdateSubVersion(0, cid);
    }
}

void TxnRecord::Get(DbRecord* value) {
    INVARIANT(get_data() != nullptr);
    for (ColumnId cid = 0; cid < txn_schema()->GetColumnCount(); ++cid) {
        char* src = record_content_.GetHeader(cid)->ColumnData();
        value->CopyFrom(cid, src);
    }
}

void TxnRecord::Get(char* dst) {
    INVARIANT(get_data() != nullptr);
    for (ColumnId cid = 0; cid < txn_schema()->GetColumnCount(); ++cid) {
        char* src = record_content_.GetHeader(cid)->ColumnData();
        std::memcpy(dst + record_schema_->GetColumnOffset(cid), src,
                    record_schema_->GetColumnSize(cid));
    }
}

char* TxnRecord::WriteLog(char* dst) {
    WriteNext<TableId>(dst, table_id());
    WriteNext<RecordKey>(dst, record_key());

    *(uint64_t*)dst = commit_bitmap();
    dst += sizeof(uint64_t);
    for (ColumnId cid = 0; cid < txn_schema()->GetColumnCount(); ++cid) {
        if (is_commit_column(cid)) {
            record_content_.CopyTo(dst, cid);
            dst += txn_schema()->ColumnMemSize(cid);
        }
    }
    return dst;
}

const char* TxnRecord::ReadFromLog(const char* src) {
    INVARIANT(txn_schema() != nullptr);
    uint64_t bitmap = *(uint64_t*)src;
    src += sizeof(uint64_t);
    for (ColumnId cid = 0; cid < txn_schema()->GetColumnCount(); ++cid) {
        if (bitmap & (BIT_ONE >> cid)) {
            record_content_.CopyFrom(src, cid);
            src += txn_schema()->ColumnMemSize(cid);
        }
    }
    return src;
}

void TxnRecord::update_version(version_t v) {
    int col_number = txn_schema()->GetColumnCount();
    for (ColumnId cid = 0; cid < col_number; ++cid) {
        if (is_rw_column(cid) || is_insert()) {
            ColumnHeader* col = record_content_.GetHeader(cid);
            col->version = v;
        }
    }
}

void TxnRecord::UpdateVersions(timestamp_t commit_ts) {
    int col_number = txn_schema()->GetColumnCount();
    sub_version_t old_sv = 0, new_sv = 0;
    RecordHeader* record_header = record_content_.GetRecordHeader();
    for (ColumnId cid = 0; cid < col_number; ++cid) {
        if (is_rw_column(cid)) {
            old_sv = record_content_.GetRecordHeader()->GetSubVersion(cid);
            new_sv = old_sv + 1;
            version_t new_version = Version(new_sv, commit_ts);
            // Update the version of the corresponding column
            ColumnHeader* col = record_content_.GetHeader(cid);
            col->version = new_version;
            record_header->UpdateSubVersion(new_sv, cid);
            if (is_record_cc()) {
                record_handle_.GetCoarseHandle()->update_commit_version(new_version);
            } else {
                record_handle_.GetFineHandle()->update_commit_version(cid, new_version);
            }
        }
        if (is_insert()) {
            new_sv = 0;
            version_t new_version = Version(new_sv, commit_ts);
            // Update the version of the corresponding column
            ColumnHeader* col = record_content_.GetHeader(cid);
            col->version = new_version;
            record_header->UpdateSubVersion(new_sv, cid);
        }
    }
    return;
}

bool TxnRecord::CheckRecordKeyMatch() const {
    INVARIANT(get_data() != nullptr);
    RecordHeader* record_header = (RecordHeader*)get_data();
    if (likely(record_header->InUse() && record_header->record_key == record_key())) {
        return true;
    }
    return false;
}

bool TxnRecord::CheckLockAcquired() const {
    INVARIANT(has_write());
    INVARIANT(get_lock_data() != nullptr);
    uint64_t lock_value = *(uint64_t*)get_lock_data();
    // The lock is acquired only if all the rw bits are zeros
    if ((lock_value & rw_bitmap()) == 0) {
        return true;
    } else {
        txn_->IncLockFailCount();
        return false;
    }
}

bool TxnRecord::CheckDelegatedLockAcquire() const {
    INVARIANT(has_write());
    INVARIANT(need_dg_lock());
    ASSERT(this->CheckRecordKeyMatch(), "Record key not match");
    uint64_t lock_value = *(uint64_t*)get_lock_data();
    if ((lock_value & dg_lock_bitmap()) == 0) {
        return true;
    } else {
        txn_->IncLockFailCount();
        return false;
    }
}

bool TxnRecord::CheckDelegationROColumnsUnlocked() const {
    INVARIANT(has_read());
    RecordHeader* record_header = (RecordHeader*)(record_content_.header_);
    uint64_t lock_value = record_header->lock;
    return (lock_value & dg_ro_bitmap()) == 0;
}

bool TxnRecord::CheckUsedFlagAcquired() const {
    INVARIANT(is_insert());
    INVARIANT(get_lock_data() != nullptr);
    uint64_t lock_value = *(uint64_t*)get_lock_data();
    if (lock_value == UNUSED) {
        return true;
    } else {
        txn_->IncInsertFailCount();
        return false;
    }
}

bool TxnRecord::CheckROColumnsUnlocked() const {
    INVARIANT(has_read());
    INVARIANT(get_data() != nullptr);
    RecordHeader* record_header = (RecordHeader*)(record_content_.header_);
    uint64_t lock_value = record_header->lock;
    return (lock_value & ro_bitmap()) == 0;
}

bool TxnRecord::CheckConsistentSnapshot() const {
    INVARIANT(has_read());
    INVARIANT(get_data() != nullptr);
    RecordHeader* record_header = record_content_.GetRecordHeader();
    auto s_iter = ro_columns_.Iter();
    // For each read-only columns
    for (; s_iter.Valid(); s_iter.Next()) {
        ColumnId cid = s_iter.Value();
        version_t column_version = get_version(cid);
        if (record_header->sub_versions[cid] != SubVersion(column_version)) {
            return false;
        }
    }
    return true;
}

bool TxnRecord::CheckRemoteDataReady() const {
    return record_content_.data_[record_size() - 1] == VISIBLE_DATA;
}

bool TxnRecord::CheckValidationSubVersionMatch() {
    auto iter = ro_columns_.Iter();
    RecordHeader* validate_header = record_content_.GetValidateHeader();
    RecordHeader* record_header = record_content_.GetRecordHeader();
    for (; iter.Valid(); iter.Next()) {
        ColumnId cid = iter.Value();
        if (is_column_cc()) {
            FineRecordHandle* handle = record_handle_.GetFineHandle();
            sub_version_t latest_subversion = 0;
            sub_version_t fetched_subversion = record_header->GetSubVersion(cid);
            if (handle->writer_version_valid(cid)) {
                // Cached in read-write mode, we can skip validating this one
                latest_subversion = SubVersion(handle->writer_version(cid));
                continue;
            } else {
                // Cached in read-only mode, use the version from validated read
                ColumnSet locked_set = ColumnSet(validate_header->lock);
                if (locked_set.Contain(cid)) {
                    return false;
                }
                latest_subversion = validate_header->GetSubVersion(cid);
            }
            fetched_subversion = latest_subversion;
            if (latest_subversion != fetched_subversion) {
                return false;
            }
        } else if (is_record_cc()) {
            CoarseRecordHandle* handle = record_handle_.GetCoarseHandle();
            sub_version_t latest_subversion = 0;
            sub_version_t fetched_subversion = record_header->GetSubVersion(cid);
            if (handle->writer_version_valid()) {
                latest_subversion = SubVersion(handle->writer_version());
                continue;
            } else {
                ColumnSet locked_set = ColumnSet(validate_header->lock);
                if (locked_set.Contain(cid)) {
                    return false;
                }
                latest_subversion = validate_header->GetSubVersion(cid);
            }
            if (latest_subversion != fetched_subversion) {
                return false;
            }
        }
    }
    return true;
}

bool TxnRecord::CheckValidationVersionMatch() const {
    INVARIANT(get_validate_data() != nullptr);
    auto iter = ro_columns_.Iter();
    RecordHeader* record_header = record_content_.GetRecordHeader();
    RecordHeader* validate_header = record_content_.GetValidateHeader();
    uint64_t lock_value = validate_header->lock;

    // The record is changed by other transactions: e.g., possibly due to deletion
    if (!validate_header->RecordKeyMatch(record_key())) {
        return false;
    }

    // Check if the lock is held by other transactions
    if ((lock_value & ro_bitmap()) != 0) {
        return false;
    }

    // Check if the read-only columns are not changed by other transactions
    for (; iter.Valid(); iter.Next()) {
        ColumnId cid = iter.Value();
        if (record_header->GetSubVersion(cid) != validate_header->GetSubVersion(cid)) {
            return false;
        }
    }
    return true;
}

void TxnRecord::finish() {
    // This function is used to coordinate local execution, set this record as
    // finished meanning this record will not be modified again
    timestamp_t c_ts = txn_->commit_ts_;
    INVARIANT(c_ts != INVALID_TIMESTAMP);
    if (has_write() || is_insert()) {
        update_version(Version(c_ts, commit_bitmap()));
        if (is_insert()) {
            write_record_header();
        }
    }
    // [WARNING] In our current implementation, the inserted record is not locked
    // and it has no corresponding local record object.
    if (TxnConfig::ENABLE_LOCAL_EXECUTION) {
        if (!is_insert()) {
            if (has_write()) {
                record_handle_.UpdateLastWriter(this);
                record_handle_.UpdateWriteTimestamp(txn_->exec_ts_, record_version_idx_);
            }
            ReleaseLocalLock();
        }
    }
    finished_ = true;
}

timestamp_t TxnRecord::GetTsExec() const { return txn_->exec_ts_; }

CheckStatus TxnRecord::CheckLocalDataReady() {
    // Check if the lock for write columns have been acquired:
    if (is_column_cc()) {
        FineRecordHandle* handle = record_handle_.GetFineHandle();
        for (ColumnId cid = 0; cid < column_count(); ++cid) {
            if (is_rw_column(cid)) {
                LockStatus cls = handle->lock_status(cid);
                if (cls == LockStatus::NO_LOCK || cls == LockStatus::LOCKING) {
                    // The transaction will wait until the lock is ready
                    return CheckStatus::RETRY;
                } else if (cls == LockStatus::FAIL_LOCK) {
                    // The current transaction should abort if the lock is not acquired
                    txn_->IncLockFailCount();
                    return CheckStatus::ABORT;
                } else if (cls == LockStatus::SUCC_LOCK) {
                    version_t c_version = handle->writer_version(cid);
                    if (c_version == BASE_VERSION) {
                        return CheckStatus::RETRY;
                    }
                    continue;
                }
            }
            if (is_ro_column(cid)) {
                version_t c_version = handle->reader_version(cid);
                if (c_version == BASE_VERSION) {
                    return CheckStatus::RETRY;
                } else if (c_version == INVALID_VERSION) {
                    txn_->IncReadColumnLockedCount();
                    // This version might have been locked by other transactions
                    return CheckStatus::ABORT;
                }
            }
        }
    } else if (is_record_cc()) {
        if (has_write()) {
            CoarseRecordHandle* handle = record_handle_.GetCoarseHandle();
            LockStatus rls = handle->lock_status();
            // The delegater is doing lock operation
            if (rls == LockStatus::NO_LOCK || rls == LockStatus::LOCKING) {
                return CheckStatus::RETRY;
            } else if (rls == LockStatus::FAIL_LOCK) {
                txn_->IncLockFailCount();
                return CheckStatus::ABORT;
            } else if (rls == LockStatus::SUCC_LOCK) {
                version_t r_version = handle->writer_version();
                if (r_version == BASE_VERSION) {
                    return CheckStatus::RETRY;
                }
            }
        } else if (is_readonly()) {
            version_t r_version = record_handle_.GetCoarseHandle()->reader_version();
            if (r_version == BASE_VERSION) {
                return CheckStatus::RETRY;
            } else if (r_version == INVALID_VERSION) {
                txn_->IncReadColumnLockedCount();
                return CheckStatus::ABORT;
            }
        }
    }

    if (has_write()) {
        // The lock is successfully acquired
        this->set_remote_locked();
    }
    return CheckStatus::READY;
}

void TxnRecord::UpdateTxnAccessedTimestamp(timestamp_t ts) const {
    txn_->UpdateMaxAccessedTimestamp(ts);
}

RedoEntry* TxnRecord::GetMyRedoEntry() const { return txn_->redo_entry_; }

void TxnRecord::AddDependentTransaction(TxnInfo* txn_info) {
    // Do not add the nullptr
    if (txn_info == nullptr) {
        return;
    }
    txn_->AddDependentTransaction(txn_info);
}

TxnInfo* TxnRecord::GetTxnInfo() const { return txn_->txn_info(); }

void TxnRecord::set_data(char* d) {
    this->record_content_.data_ = d;
    this->record_content_.header_ = d;
    // char* mr_bound =
    //     (char*)(txn_->pool_->GetQueuePair(0)->GetLocalMemoryRegionToken().get_region_addr());
    // ASSERT(mr_bound <= d, "Invalid data pointer");
}

char* TxnRecord::get_mr_bound() const {
    char* mr_bound =
        (char*)(txn_->pool_->GetQueuePair(0)->GetLocalMemoryRegionToken().get_region_addr());
    return mr_bound;
}