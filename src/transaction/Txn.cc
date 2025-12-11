#include "transaction/Txn.h"

#include <boost/core/allocator_access.hpp>
#include <cstddef>
#include <cstring>
#include <exception>

#include "common/Config.h"
#include "common/Type.h"
#include "db/Format.h"
#include "db/Schema.h"
#include "db/Table.h"
#include "mempool/Coroutine.h"
#include "rdma/RdmaBatch.h"
#include "transaction/Enums.h"
#include "transaction/TimestampGen.h"
#include "transaction/TxnConfig.h"
#include "util/Logger.h"
#include "util/Macros.h"
#include "util/Status.h"
#include "util/Timer.h"

TxnRecordRef Txn::SelectRecord(TableId table_id, RecordKey record_key, AccessMode access_mode) {
    selected_records_.emplace_back(
        /* table_id      = */ table_id,
        /* record_key    = */ record_key,
        /* db_txn        = */ this,
        /* db_schema     = */ db_->GetTable(table_id)->GetTxnSchema(),
        /* record_schema = */ db_->GetTable(table_id)->GetRecordSchema(),
        /* access_mode   = */ access_mode);
    return TxnRecordRef(&selected_records_, selected_records_.size() - 1);
}

TxnRecordRef Txn::InsertRecord(TableId table_id, RecordKey record_key) {
    insert_records_.emplace_back(
        /* table_id      = */ table_id,
        /* record_key    = */ record_key,
        /* db_txn        = */ this,
        /* db_schema     = */ db_->GetTable(table_id)->GetTxnSchema(),
        /* record_schema = */ db_->GetTable(table_id)->GetRecordSchema(),
        /* access_mode   = */ AccessMode::INSERT);
    return TxnRecordRef(&insert_records_, insert_records_.size() - 1);
}

Status Txn::Deref(coro_yield_t& yield) {
    Status s;
    if (TxnConfig::ENABLE_LOCAL_EXECUTION) {
        return DerefLocal(yield);
    } else {
        return DerefNonLocal(yield);
    }
    return s;
}

Status Txn::LockAndRead(coro_yield_t& yield) {
    Status s;
    for (auto& record : selected_records_) {
        if (record.has_write() && !record.is_remote_locked()) {
            s = IO()->AcquireLockAndRead(&record);
        }
    }

    for (auto& record : selected_records_) {
        if (record.is_readonly() && !record.read_done()) {
            s = IO()->ReadRecord(&record);
        }
    }

    for (auto& record : insert_records_) {
        s = IO()->AcquireUsedFlag(&record);
    }
    return s;
}

Status Txn::DerefNonLocal(coro_yield_t& yield) {
    Status s = CreateLocalRecords();
    if (!s.ok()) {
        return s;
    }

    PHASE_LATENCY_TRACK_START(TxnPhase::READ_PHASE);
    s = GetAddress(yield);
    if (unlikely(!s.ok())) {
        return s;
    }

    int try_cnt = 0;
    while (try_cnt++ < TxnConfig::MAX_EXECUTE_COUNT) {
        LockAndRead(yield);
        YieldForPoll(yield);
        bool insert_fail = false;
        s = CheckLockAndReadResults(insert_fail);
        // If the failed reason is due to insert fail, we can break and report
        // abortion. Failed insertion means there is no empty slot for this
        // record. Thus, keep retrying for this record is meaningless
        if (s.ok() || insert_fail) {
            break;
        }
    }
    PHASE_LATENCY_TRACK_END(TxnPhase::READ_PHASE);

    if (!s.ok()) {
        Status s1 = Abort(yield);
        ASSERT(s1.ok(), "");
    }

    // Acquire timestamp now, for localized execution, the commit timestamp
    // is allocated after the transaction successfully acquired all locks
    commit_ts_ = ts_generator_->AcquireTimestamp();
    return s;
}

Status Txn::Validate(coro_yield_t& yield) {
    if (TxnConfig::ENABLE_LOCAL_EXECUTION) {
        return ValidateLocal(yield);
    } else {
        return ValidateNonLocal(yield);
    }
}

Status Txn::ValidateNonLocal(coro_yield_t& yield) {
    PHASE_LATENCY_TRACK_START(TxnPhase::VALIDATION_PHASE);
    bool has_rdma_op = false;
    Status s;
    if (TxnConfig::ENABLE_REDO_LOG) {
        s = WriteRedoLog();
        ASSERT(s.ok(), "Transaction writes redo log failed: %s", s.error_msg().c_str());
    }

    for (auto& record : selected_records_) {
        if (record.has_read()) {
            IO()->ReadRecordValidation(&record);
            has_rdma_op = true;
        }
    }

    if (has_rdma_op) {
        YieldForPoll(yield);
        has_rdma_op = false;
        this->redo_log_written_ = true;
    }

    PHASE_LATENCY_TRACK_END(TxnPhase::VALIDATION_PHASE);

    // Check the validation results:
    s = CheckValidationResults();
    return s;
}

Status Txn::CommitWrite(coro_yield_t& yield) {
    if (TxnConfig::ENABLE_LOCAL_EXECUTION) {
        return CommitWriteLocal(yield);
    } else {
        return CommitWriteNonLocal(yield);
    }
}

Status Txn::CommitWriteNonLocal(coro_yield_t& yield) {
    PHASE_LATENCY_TRACK_START(TxnPhase::COMMIT_PHASE);

    // Acquire the timestamp for commit
    commit_ts_ = max_accessed_ts_ + 1;

    if (TxnConfig::ENABLE_REDO_LOG && redo_log_written_) {
        UpdateLogStatus(LogStatus::COMMITTED);
    }
    // Commit the read-write records
    for (auto& record : selected_records_) {
        if (record.has_write()) {
            // Update both the version and subversion of the updated columns
            record.UpdateVersions(commit_ts_);
        }
        record.record_handle_.Release(&record);
        if (record.has_write() && record.is_remote_locked()) {
            WritePrimaryRecord(&record);
            WriteBackupRecords(&record);
        }
    }

    for (auto& record : insert_records_) {
        record.UpdateVersions(commit_ts_);
        WritePrimaryNewRecord(&record);
        WriteBackupNewRecord(&record);
    }

    YieldForPoll(yield);

    if (TxnConfig::ENABLE_REDO_LOG && redo_log_written_) {
        for (auto& record : selected_records_) {
            if (record.has_write()) {
                // Since this latest record has been updated, the log is
                // meaningless and can be garbage collected now
                redo_entry_->ReduceLogRef();
            }
        }
    }

    PHASE_LATENCY_TRACK_END(TxnPhase::COMMIT_PHASE);
    return Status::OK();
}

Status Txn::Commit(coro_yield_t& yield) {
    Status s = Validate(yield);
    if (s.ok()) {
        s = CommitWrite(yield);
        ASSERT(s.ok(), "commit write failed");
    } else {
        s = Abort(yield);
        ASSERT(s.ok(), "commit write failed");
        return Status(kTxnError, "Aborted");
    }
    return Status::OK();
}

Status Txn::AbortNonLocal(coro_yield_t& yield) {
    bool has_rdma_op = false;
    if (TxnConfig::ENABLE_REDO_LOG && redo_log_written()) {
        UpdateLogStatus(LogStatus::ABORTED);
        // The redo log entry is also no long needed
        for (auto& record : selected_records_) {
            if (record.has_write()) {
                redo_entry_->ReduceLogRef();
            }
        }
    }

    for (auto& record : selected_records_) {
        record.record_handle_.Release(&record);
        if (record.has_write() && record.is_remote_locked()) {
            Status s = IO()->ReleaseWriteLock(&record);
            ASSERT(s.ok(), "");
            has_rdma_op = true;
        }
    }

    for (auto& record : insert_records_) {
        if (record.is_remote_locked()) {
            Status s = IO()->ReleaseUsedFlag(&record);
            ASSERT(s.ok(), "");
            has_rdma_op = true;
        }
    }

    if (has_rdma_op) {
        YieldForPoll(yield);
    }

    return Status::OK();
}

Status Txn::Abort(coro_yield_t& yield) {
    txn_status_ = TxnStatus::ABORT;
    me_->set_txn_status(TxnStatus::ABORT);
#if DEBUG_COROUTINE
    LOG_INFO("T%lu C%d Txn(%llx) ABORT Txn(%p)", t_id_, coro_id_, txn_id_, me_);
#endif

    if (TxnConfig::ENABLE_LOCAL_EXECUTION) {
        return AbortLocal(yield);
    } else {
        return AbortNonLocal(yield);
    }
}

void Txn::Begin(TxnId txn_id, TxnType txn_type, timestamp_t start_ts, const std::string& txn_name) {
    // A TxnId uniquely identifies a transaction
    txn_id_ = txn_id;
    txn_type_ = txn_type;
    start_ts_ = start_ts;
    txn_name_ = txn_name;

    // Clear some stateful fields
    selected_records_.clear();
    insert_records_.clear();

    IO()->Reset();

    // Reserve some space

    rdma_access_.Reset();
    txn_detail_.Reset();
    phase_latency_.Reset();

    me_ = new TxnInfo(txn_id_, txn_name_);
    dependent_txn_.clear();
    dependent_txn_table_ids_.clear();

    redo_entry_ = log_manager_->AllocateNewLogEntry(txn_id_);
    redo_log_written_ = false;
    commit_ts_ = BASE_TIMESTAMP;
    max_accessed_ts_ = BASE_TIMESTAMP;
    exec_ts_ = BASE_TIMESTAMP;
    aborted_count_ = 0;

#if DEBUG_LOCALIZATION_PERF
    std::memset(yield_next_time, 0, sizeof(yield_next_time));
#endif
}

Status Txn::GetAddress(coro_yield_t& yield) {
    std::vector<TxnRecord*> bkt_read_records;
    for (auto& record : selected_records_) {
        DbTablePtr table = db_->GetTable(record.table_id());
        bool found_address = false;
        PoolPtr address = table->GetCachedAddress(record.record_key());
        Status s = Status::OK();
        if (address == POOL_PTR_NULL) {
            address = table->GetBucketAddress(record.record_key());
            if (table->GetRecordNumPerBucket() == 1) {
                record.set_pool_addr(address);
            } else {
                // Read the whole bucket into the local buffer:
                s = IO()->ReadHashBucket(&record, address);
                ASSERT(s.ok(), "IO::ReadHashBucket failed");
                bkt_read_records.push_back(&record);
            }
        } else {
            record.set_pool_addr(address);
        }
    }

    for (auto& record : insert_records_) {
        // No do process this record if it has been processed
        DbTablePtr table = db_->GetTable(record.table_id());
        bool found_address = false;
        PoolPtr address = table->GetBucketAddress(record.record_key());
        ASSERT(address != POOL_PTR_NULL, "Bucket address not found");
        if (table->GetRecordNumPerBucket() == 1) {
            record.set_pool_addr(address);
        } else {
            Status s = IO()->ReadHashBucket(&record, address);
            ASSERT(s.ok(), "");
            bkt_read_records.push_back(&record);
        }
    }

    if (!bkt_read_records.empty()) {
        YieldForPoll(yield);
        for (auto record : bkt_read_records) {
            Status s = CheckBucketResults(record);
            if (unlikely(!s.ok())) {
                return s;
            }
        }
    }
    return Status::OK();
}

Status Txn::CheckBucketResults(TxnRecord* record) {
    char* record_pos;
    DbTablePtr table = db_->GetTable(record->table_id());
    if (record->is_insert()) {
        bool already_exist = false;
        bool succ = SearchRecordInsertPos(record, &record_pos, already_exist);
        if (unlikely(!succ)) {
            IncInsertFailCount();
            return Status(kTxnError, "Insert fail: no empty slot");
        }
        if (unlikely(already_exist)) {
            return Status(kTxnError, "Insert records already exists");
        }
    } else {
        bool succ = SearchRecordInBucket(record, &record_pos);
        if (unlikely(!succ)) {
            return Status(kTxnError, "Target record not exist");
        }
    }

    PoolPtr record_pool_addr = record->pool_addr() + record_pos - record->get_data();
    record->set_pool_addr(record_pool_addr);
    if (!record->is_insert()) {
        table->UpdateRecordAddress(record->record_key(), record_pool_addr);
    }

    if (record->is_readonly()) {
        // The record can directly use the bucket read results as read results
        record->set_access_status(RecordAccessStatus::READ_RECORD);
        record->set_data(record_pos);
    }
    return Status::OK();
}

bool Txn::SearchRecordInBucket(TxnRecord* record, char** record_addr) {
    INVARIANT(record->access_status() == RecordAccessStatus::READ_BUCKET);

    DbTablePtr table = db_->GetTable(record->table_id());
    char* base_ptr = record->get_data();
    for (size_t i = 0; i < table->GetRecordNumPerBucket(); ++i) {
        RecordHeader* record_header = table->GetRecord(record->get_data(), i);
        if (record_header->InUse()) {
            if (record_header->record_key == record->record_key()) {
                *record_addr = (char*)record_header;
                return true;
            } else if (TxnConfig::UPDATE_ADDRESS_CACHE_WHEN_CHECK_BKT) {
                // Update this record's address even though it's not used now
                table->UpdateRecordAddress(record_header->record_key,
                                           record->pool_addr() + ((char*)record_header - base_ptr));
            }
        }
    }
    return false;
}

bool Txn::SearchRecordInsertPos(TxnRecord* record, char** record_addr, bool& already_exists) {
    INVARIANT(record->access_status() == RecordAccessStatus::READ_BUCKET);
    DbTablePtr table = db_->GetTable(record->table_id());
    char* base_ptr = record->get_data();
    int insert_pos = -1;
    for (size_t i = 0; i < table->GetRecordNumPerBucket(); ++i) {
        RecordHeader* row_header = table->GetRecord(record->get_data(), i);
        if (!row_header->InUse() && insert_pos == -1) {
            insert_pos = i;
        }
        if (row_header->InUse()) {
            if (unlikely(row_header->record_key == record->record_key())) {
                // Just do the search, don't do anything else
                *record_addr = (char*)row_header;
                already_exists = true;
                return false;
            } else if (TxnConfig::UPDATE_ADDRESS_CACHE_WHEN_CHECK_BKT) {
                // Update this record's address even though it's not used now
                table->UpdateRecordAddress(row_header->record_key,
                                           record->pool_addr() + ((char*)row_header - base_ptr));
            }
        }
    }
    // No insertion position
    if (insert_pos == -1) {
        return false;
    }
    RecordHeader* row_header = table->GetRecord(record->get_data(), insert_pos);
    // It is necessary to mark the RowHeader as used on CN, to prevent next
    // records (to be inserted) to be inserted use the same slot
    row_header->used = true;
    *record_addr = (char*)row_header;
    return true;
}

Status Txn::CheckLockAndReadResults(bool& insert_fail) {
    util::Timer timer;
    // 1. Check if all records have been locked
    bool all_locked = true;
    for (auto& record : selected_records_) {
        if (record.has_write() && !record.is_remote_locked()) {
            if (record.CheckLockAcquired()) {
                record.set_remote_locked();
                ColumnSet::SingleIterator s_iter = record.rw_columns().Iter();
                for (; s_iter.Valid(); s_iter.Next()) {
                    ColumnId cid = s_iter.Value();
                    if (record.is_record_cc()) {
                        CoarseRecordHandle* handle = record.record_handle_.GetCoarseHandle();
                        handle->write_column_value(&record, cid, true);
                        handle->set_writer_version_sync(record.get_version(cid));
                        handle->set_lock_status_sync(LockStatus::SUCC_LOCK);
                    } else if (record.is_column_cc()) {
                        FineRecordHandle* handle = record.record_handle_.GetFineHandle();
                        handle->write_column_value(&record, cid, true);
                        handle->set_writer_version_sync(cid, record.get_version(cid));
                        handle->set_lock_status_sync(cid, LockStatus::SUCC_LOCK);
                    }
                }
            } else {
                // Note: we can not break the loop here. The transaction must
                // check the results of all lock requests to ensure it can release
                // all acquired lock when abortion is triggered
                all_locked = false;
            }
        }
    }

    // 2. Check if used flag is acquired for insertion record:
    for (auto& record : insert_records_) {
        if (record.CheckUsedFlagAcquired()) {
            record.set_remote_locked();
            record.set_access_status(RecordAccessStatus::READ_DONE);
        } else {
            insert_fail = true;
            all_locked = false;
        }
    }

    if (!all_locked) {
        return Status(kTxnError, "Lock failed");
    }

    // 2. Check all records' read results
    for (auto& record : selected_records_) {
        bool record_key_match = record.CheckRecordKeyMatch();
        ASSERT(record_key_match, "Record key not match");
        bool read_columns_unlocked = record.CheckROColumnsUnlocked();
        bool consistent_snapshot = record.CheckConsistentSnapshot();

        // If this is a record that requires remote lock.
        bool lock_acquired = true;
        if (record.has_write() && !record.is_remote_locked()) {
            lock_acquired = false;
        }

        if (record_key_match && read_columns_unlocked && consistent_snapshot && lock_acquired) {
            record.set_access_status(RecordAccessStatus::READ_DONE);
            ColumnSet::SingleIterator s_iter = record.ro_columns().Iter();
            for (; s_iter.Valid(); s_iter.Next()) {
                ColumnId cid = s_iter.Value();
                if (record.is_record_cc()) {
                    CoarseRecordHandle* handle = record.record_handle_.GetCoarseHandle();
                    handle->write_column_value(&record, cid, false);
                    handle->set_reader_version_sync(record.get_version(cid));
                } else if (record.is_column_cc()) {
                    FineRecordHandle* handle = record.record_handle_.GetFineHandle();
                    handle->write_column_value(&record, cid, false);
                    handle->set_reader_version_sync(cid, record.get_version(cid));
                }
            }
        } else {
            return Status(kTxnError, "Check ReadColumnUnlocked and ConsistSnapshot failed");
        }
    }
    txn_detail_.check_batchread_results_time += timer.ns_elapse();
    return Status::OK();
}

Status Txn::CheckValidationResults() {
    for (auto& record : selected_records_) {
        if (record.has_read() && !record.CheckValidationVersionMatch()) {
            return Status(kTxnError, "Check Record validation failed");
        }
    }
    return Status::OK();
}

Status Txn::CheckValidationResultsLocal() {
    for (auto& record : selected_records_) {
    if (record.has_read()) {
      if (!record.CheckValidationSubVersionMatch()) {
        return Status(kTxnError, "Check SubVersion failed");
      }
    }
  }
  return Status::OK();
}

Status Txn::WriteRedoLog() {
    redo_entry_->WriteLocalEntryHeader(txn_id_, LogStatus::DOING);
    for (auto& record : selected_records_) {
        if (record.has_write()) {
            redo_entry_->WriteRedoEntry(&record);
        }
    }
    redo_entry_->WriteDependentTransactions();
    redo_entry_->WriteEndMarker();
    Status s = log_manager_->AllocatePoolAddress(redo_entry_);
    if (unlikely(!s.ok())) {
        return s;
    }
    return IO()->WriteRedoEntry(redo_entry_);
}

Status Txn::UpdateLogStatus(LogStatus s) {
    redo_entry_->WriteLocalEntryHeader(txn_id_, s);
    return IO()->UpdateLogStatus(redo_entry_);
}

Status Txn::WritePrimaryRecord(TxnRecord* record) {
    PoolPtr primary_address = record->pool_addr();
    IO()->WriteRecord(record, primary_address);
    IO()->ReleaseWriteLock(record);
    return Status::OK();
}

Status Txn::WriteBackupRecords(TxnRecord* record) {
    DbTablePtr table = db_->GetTable(record->table_id());
    PoolPtr primary_address = record->pool_addr();
    uint64_t offset = primary_address - table->PrimaryTablePoolPtr();
    for (auto table_ptr : table->BackupTablePoolPtr()) {
        PoolPtr backup_address = table_ptr + offset;
        IO()->WriteRecord(record, backup_address);
    }
    return Status::OK();
}

Status Txn::WritePrimaryDelegationRecord(TxnRecord* record) {
    PoolPtr primary_address = record->pool_addr();
    IO()->WriteRecordDelegation(record, primary_address);
    IO()->ReleaseWriteLockDelegation(record);
    return Status::OK();
}

Status Txn::WriteBackupDelegationRecords(TxnRecord* record) {
    DbTablePtr table = db_->GetTable(record->table_id());
    PoolPtr primary_address = record->pool_addr();
    uint64_t offset = primary_address - table->PrimaryTablePoolPtr();
    for (auto table_ptr : table->BackupTablePoolPtr()) {
        PoolPtr backup_address = table_ptr + offset;
        IO()->WriteRecordDelegation(record, backup_address);
    }
    return Status::OK();
}

Status Txn::WritePrimaryNewRecord(TxnRecord* record) {
    PoolPtr primary_address = record->pool_addr();
    IO()->WriteNewRecord(record, primary_address);
    return Status::OK();
}

Status Txn::WriteBackupNewRecord(TxnRecord* record) {
    DbTablePtr table = db_->GetTable(record->table_id());
    PoolPtr primary_address = record->pool_addr();
    uint64_t offset = primary_address - table->PrimaryTablePoolPtr();
    for (auto table_ptr : table->BackupTablePoolPtr()) {
        PoolPtr backup_address = table_ptr + offset;
        IO()->WriteNewRecord(record, backup_address);
    }
    return Status::OK();
}