#include <algorithm>
#include <cstdint>
#include <cstring>

#include "common/Type.h"
#include "db/Format.h"
#include "db/Schema.h"
#include "mempool/Coroutine.h"
#include "transaction/Bitset.h"
#include "transaction/Enums.h"
#include "transaction/TimestampGen.h"
#include "transaction/Txn.h"
#include "transaction/TxnConfig.h"
#include "util/Logger.h"
#include "util/Macros.h"
#include "util/Status.h"
#include "util/Timer.h"

RecordHandleWrapper Txn::SelectRecordHandle(TxnRecord* txn_record) {
    bool create_new = false;
    RecordHandleIndex* hdl_index = record_handle_db_->get_index(txn_record->table_id());
    RecordHandleWrapper wrapper = hdl_index->SelectRecord(
        txn_record->record_key(), txn_record->access_mode_, create_new, t_id_, coro_id_);
    txn_detail_.record_handle_search_count++;
    txn_detail_.record_handle_hit_count += !create_new;
    return wrapper;
}

Status Txn::DerefLocal(coro_yield_t& yield) {
    Status s = CreateLocalRecords();
    if (!s.ok()) {
        goto ABORT;
    }

    s = DelegateExecute(yield);
    if (!s.ok()) {
        goto ABORT;
    }

    s = AccessLocalRecords(yield);
    if (!s.ok()) {
        goto ABORT;
    }

    if (commit_ts_ == BASE_TIMESTAMP) {
        // Only acquire timestamp for once
        commit_ts_ = ts_generator_->AcquireTimestamp();
    }

    if (exec_ts_ == BASE_TIMESTAMP) {
        exec_ts_ = ts_generator_->AcquireTimestamp();
    }

    return Status::OK();

ABORT:
    s = Abort(yield);
    ASSERT(s.ok(), "Abort transaction failed");
    return Status(kTxnError, "Transaction Aborted");
}

Status Txn::CreateLocalRecords() {
    // INVARIANT(local_db_ != nullptr);
    INVARIANT(record_handle_db_ != nullptr);
    PHASE_LATENCY_TRACK_START(TxnPhase::PREPARE_PHASE);
    for (auto& record : selected_records_) {
        if (record.record_handle_.valid() || record.is_finished()) {
            continue;
        }
        RecordHandleWrapper hdl = SelectRecordHandle(&record);
        if (!hdl.Acquire(&record)) {
            // Did not resolve the conflict successfully, the transaction can
            // no proceed to the next phase, so simply abort itself
            IncJoinBatchFailCount();
            return Status(kTxnError, "AccessLocal failed");
        }
        // Only assign the local record if the conflicts is resolved
        // This prevents false Unref() during abort
        record.set_record_handle(hdl);
    }

    PHASE_LATENCY_TRACK_END(TxnPhase::PREPARE_PHASE);
    return Status::OK();
}

Status Txn::AccessLocalRecords(coro_yield_t& yield) {
    PHASE_LATENCY_TRACK_START(TxnPhase::EXECUTION_PHASE);
    util::Timer timer;
    // The transaction can enter its next phase execution until all
    // the data it needed has been brought back. The check condition
    // includes:
    //  * All required locks have been locked by this CN. This is done
    //    by checking if the status of record lock or cell lock is
    //    SUCC_LOCK
    // *  All read parts have been retrieved from MN, and the data
    //    consistency have been checked, i.e., the read-only columns are
    //    not locked by another transaction, the column forms a consistent
    //    snapshot.
    for (auto& record : selected_records_) {
        if (record.is_finished()) {
            continue;
        }
        util::Timer timer;
        while (true) {
            CheckStatus s = record.CheckLocalDataReady();
            if (s == CheckStatus::ABORT) {
                return Status(kTxnError, "Transaction aborted");
            } else if (s == CheckStatus::RETRY) {
                YieldToNext(yield);
            } else if (s == CheckStatus::READY) {
                break;
            }
            // It seems like impossible that fetching data from MN takes more than 1 second
            if (timer.ms_elapse() > 10000) {
                LOG_FATAL("Transaction %s is stuck in AccessLocalRecords", txn_name_.c_str());
            }
        }
    }
    txn_detail_.wait_batchread_data_dura += timer.ns_elapse();

    // The transaction then access the local records. The serializability
    // is achived by using a concurrency control scheme, e.g., 2PL or OCC
    // We choose to use 2PL and the lock granularity is set to be record
    // level. It should not bring too many negative effects for the system
    // throughput as localized execution is fast
    Status s = Status::OK();
    for (auto& record : selected_records_) {
        if (record.is_finished()) {
            continue;
        }
        RecordHandleWrapper hdl = record.record_handle_;
        record.AcquireLocalLock();
        Status access_s = hdl.Access(&record);
        if (!access_s.ok()) {
            s = access_s;
            record.ReleaseLocalLock();
        }
    }
    PHASE_LATENCY_TRACK_END(TxnPhase::EXECUTION_PHASE);
    return s;
}

Status Txn::DelegateLockAndRead(coro_yield_t& yield, bool* has_rdma_op) {
    Status s = Status::OK();
    for (auto& record : selected_records_) {
        // Skip this record if the transaction has finished processing this record
        if (record.is_finished()) {
            continue;
        }
        // The current transaction may need to acquire the lock for this
        // record (either record-level or cell-level lock). need_dg_lock()
        // returns true means it's current transaction's responsibility to
        // lock the remote data, the !is_dg_lock_acquired() means the
        // transaction has not successfully acquired the remote lock for
        // the whole batch yet and it must retry
        if (record.has_write()) {
            // We only acquire the lock if the record's dg lock is not acquired
            if (record.need_dg_lock() && !record.is_dg_lock_acquired()) {
                s = IO()->AcquireLockAndRead(&record);
                record.record_handle_.UpdateWriteLockStatus(&record, LockStatus::LOCKING);
                *has_rdma_op = true;
            }
        }

        // For read-write objects or read-only objects, bring the whole record
        // data back to compute nodes, we do not distinguish if we should only
        // read some specific columns
        // if (record.need_dg_lock() || record.need_dg_read()) {
        if (record.need_dg_read() && record.is_readonly() && !record.dg_read_done()) {
            if (record.read_from_cache()) {
                IO()->AllocateBuffer(&record);
            } else {
                s = IO()->ReadRecord(&record);
                *has_rdma_op = true;
            }
        }
        // Even though this transaction does not need to access the record
        // data in the memory pool, it still needs to allocate the private
        // buffer for later process
        if (!record.need_dg_lock() && !record.need_dg_read()) {
            IO()->AllocateBuffer(&record);
        }
    }

    // For insertion records, acquire the USED flag. There is no delegation
    for (auto& record : insert_records_) {
        if (record.is_finished()) {
            continue;
        }
        if (!record.is_remote_locked()) {
            s = IO()->AcquireUsedFlag(&record);
            *has_rdma_op = true;
        }
    }
    return Status::OK();
}

Status Txn::CheckDelegatedReadLockResults(bool last_try) {
    util::Timer timer;
    Status s1 = CheckDelegatedLockResults(last_try);
    Status s2 = CheckDelegatedReadResults(last_try);
    Status s3 = CheckDelegatedInsertFlagAcquired(last_try);
    txn_detail_.check_batchread_results_time += timer.ns_elapse();

    if (!s1.ok()) {
        return s1;
    }

    if (!s2.ok()) {
        return s2;
    }

    if (!s3.ok()) {
        return s3;
    }

    return Status::OK();
}

Status Txn::CheckDelegatedLockResults(bool last_try) {
    bool all_locked = true;
    for (auto& record : selected_records_) {
        RecordHandleWrapper hdl = record.record_handle_;
        if (record.need_dg_lock() && !record.is_dg_lock_acquired()) {
            bool succ = record.CheckDelegatedLockAcquire();
            if (succ) {
                record.set_dg_lock_acquired();
                record.set_dg_access_status(RecordAccessStatus::READ_DONE);
                hdl.UpdateWriteAccessStatus(&record);
            } else if (last_try && !succ) {
                hdl.UpdateWriteLockStatus(&record, LockStatus::FAIL_LOCK);
            }
            all_locked &= succ;
        }
    }
    if (!all_locked) {
        return Status(kTxnError, "Fail to acquire all locks");
    }
    return Status::OK();
}

CheckResult Txn::CheckReadOnlyColumns(TxnRecord* record, ColumnSet* local_columns) {
    INVARIANT(record->has_read());
    // ASSERT(record->CheckRecordKeyMatch(), "Record key not match");
    RecordHeader* record_header = (RecordHeader*)(record->get_record_header());
    uint64_t lock_value = record_header->lock;
    ColumnSet locked_columns = ColumnSet(lock_value & record->ro_bitmap());
    if (locked_columns.Empty()) {
        return CheckResult::SUCC;
    } else {
        // There are some read-only columns being locked by other transactions.
        // However, it's possible that another local transaction is holding the
        // lock and the data is already in the local memory
        if (record->is_record_cc()) {
            CoarseRecordHandle* handle = record->record_handle_.GetCoarseHandle();
            int writers = handle->writer_count();
            LockStatus ls = handle->lock_status();
            if (writers > 0 && ls == LockStatus::SUCC_LOCK) {
                return CheckResult::LOCK_BY_LOCAL;
            } else {
                // Actually, failed in the above condition does not mean the lock
                // is held by a remote transaction. For simplicity, we just let the
                // current transaction to abort and retry
                return CheckResult::LOCK_BY_REMOTE;
            }
        } else {
            INVARIANT(record->is_column_cc());
            // Check each involved column:
            FineRecordHandle* handle = record->record_handle_.GetFineHandle();
            ColumnSet::SingleIterator s_iter = locked_columns.Iter();
            for (; s_iter.Valid(); s_iter.Next()) {
                ColumnId cid = s_iter.Value();
                int writers = handle->writer_count(cid);
                LockStatus ls = handle->lock_status(cid);
                if (writers > 0 && ls == LockStatus::SUCC_LOCK) {
                    local_columns->Add(cid);
                } else {
                    return CheckResult::LOCK_BY_REMOTE;
                }
            }
            return CheckResult::LOCK_BY_LOCAL;
        }
    }
    return CheckResult::SUCC;
}

Status Txn::CheckDelegatedReadResults(bool last_try) {
    for (auto& record : selected_records_) {
        if (record.is_finished()) {
            continue;
        }
        RecordHandleWrapper hdl = record.record_handle_;
        // If the records are not fetched from the cache
        if (record.need_dg_read() && !record.read_from_cache()) {
            // local_columns stands for the columns that have already been fetched
            // by the writer. In this case, this transaction only needs to
            ColumnSet local_columns = ColumnSet::EmptySet();
            CheckResult check_result = CheckReadOnlyColumns(&record, &local_columns);
            if (check_result == CheckResult::SUCC) {
                hdl.UpdateReaderAccessStatus(&record, local_columns);
                record.set_dg_access_status(RecordAccessStatus::READ_DONE);
            } else if (check_result == CheckResult::LOCK_BY_LOCAL) {
                // These cases are unlikely to happen currently
                hdl.UpdateReaderAccessStatus(&record, local_columns);
                record.set_dg_access_status(RecordAccessStatus::READ_DONE);
            } else if (check_result == CheckResult::LOCK_BY_REMOTE) {
                // Mark the Version as invalid, notify other readers to abort:
                hdl.UpdateFailReadAccessStatus(&record, local_columns);
                return Status(kTxnError, "CheckDelegatedROColumns Failed");
            }
        }
    }
    return Status::OK();
}

Status Txn::CheckDelegatedInsertFlagAcquired(bool last_try) {
    bool all_locked = true;
    for (auto& record : insert_records_) {
        if (record.is_remote_locked()) {
            continue;
        }
        bool succ = record.CheckUsedFlagAcquired();
        if (succ) {
            record.set_access_status(RecordAccessStatus::READ_DONE);
            record.set_remote_locked();
        }
        all_locked &= succ;
    }
    if (!all_locked) {
        return Status(kTxnError, "Insertion flag is not acquired");
    }
    return Status::OK();
}

Status Txn::DelegateExecute(coro_yield_t& yield) {
    PHASE_LATENCY_TRACK_START(TxnPhase::READ_PHASE);
    Status s = GetAddress(yield);
    if (unlikely(!s.ok())) {
        return s;
    }

    int try_cnt = 0;
    bool has_rdma_op = false;
    while (++try_cnt <= TxnConfig::MAX_EXECUTE_COUNT) {
        DelegateLockAndRead(yield, &has_rdma_op);
        if (has_rdma_op) {
            YieldForPoll(yield);
        }
        // CheckPendingRequestNum();
        bool last_try = (try_cnt == TxnConfig::MAX_EXECUTE_COUNT);
        s = CheckDelegatedReadLockResults(last_try);
        if (s.ok()) {
            break;
        }
    }
    PHASE_LATENCY_TRACK_END(TxnPhase::READ_PHASE);
    return s;
}

void Txn::UpdateDelegationReadData(TxnRecord* record) {}

void Txn::UpdateDelegationReadDataToLocal(TxnRecord* record, const ColumnSet& bs) {}

Status Txn::AbortLocal(coro_yield_t& yield) {
    PHASE_LATENCY_TRACK_START(TxnPhase::ABORT_PHASE);

    bool has_rdma_op = false;

    // If the redo log has been written, the transaction needs to
    // abort the redo log in-case that the wrong log entry is used
    // for crash recovery
    if (TxnConfig::ENABLE_REDO_LOG && redo_log_written()) {
        UpdateLogStatus(LogStatus::ABORTED);
        has_rdma_op = true;
    }
    ++aborted_count_;

    for (auto& record : selected_records_) {
        // record.Unref();
        record.record_handle_.Release(&record);
        if (record.need_dg_commit()) {
            Status s = WriteLatestCommitableVersion(&record);
            has_rdma_op = true;
        }
    }

    // The transaction should release the acquired used flag when abort
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
    CheckPendingRequestNum();

    if (TxnConfig::ENABLE_REDO_LOG) {
        UnrefDependentLogEntries(true);
    }

    PHASE_LATENCY_TRACK_END(TxnPhase::ABORT_PHASE);
    return Status::OK();
}

Status Txn::ValidateLocal(coro_yield_t& yield) {
    PHASE_LATENCY_TRACK_START(TxnPhase::VALIDATION_PHASE);
    // The redo-log is a must-thing either for local or non-local
    // execution
    bool has_rdma_op = false;
    if (TxnConfig::ENABLE_REDO_LOG) {
        if (txn_type_ == TxnType::READ_WRITE) {
            WriteRedoLog();
            has_rdma_op = true;
        }
    }
    for (auto& record : selected_records_) {
        if (record.has_read()) {
            IO()->ReadRecordValidation(&record);
            has_rdma_op = true;
        }
    }

    if (has_rdma_op) {
        YieldForPoll(yield);
    }
    CheckPendingRequestNum();

    redo_log_written_ = true;

    Status check_s = CheckValidationResultsLocal();
    if (!check_s.ok()) {
        return Status(kTxnError, "CheckValidationResultsLocal failed");
    }

    //
    // A transaction is marked as COMMITTABLE, only if the following three
    // conditions are met:
    //
    //   * The transaction has no conflicts with concurrent txns, by checking
    //     the read-only parts
    //   * The transaction has written its redo log to the memory pool
    //   * All dependent transactions of this txn are COMMITTABLE
    //
    // The first condition is for concurrency control and ensuring
    // serializability and the second one is used to ensure the
    // correctness of failure recovery.
    //
    // Marking a transaction as COMMITTABLE does not mean the transaction
    // is committed. It may either commit: if its redo log is marked as
    // COMMITTED, or the log is marked as ABORT. Another case is that
    // the log is marked as DOING, in this case, it may either redo the
    // whole log if a log that depends on this log is marked as COMMITTED
    //
    bool s = CheckDependentTxnCommittable(yield);
    if (!s) {
        return Status(kTxnError, "Dependent transaction failed");
    } else {
        // Assign the commit timestamp for this transaction:
        commit_ts_ = max_accessed_ts_ + 1;
        me_->set_commit_ts(commit_ts_);
        me_->set_txn_status(TxnStatus::COMMITABLE);
        CommitLocalVersion();
    }

    PHASE_LATENCY_TRACK_END(TxnPhase::VALIDATION_PHASE);
    return Status::OK();
}

bool Txn::CheckDependentTxnCommittable(coro_yield_t& yield) {
    // Deduplicate the dependent transactions:
    std::sort(dependent_txn_.begin(), dependent_txn_.end());
    dependent_txn_.erase(std::unique(dependent_txn_.begin(), dependent_txn_.end()),
                         dependent_txn_.end());
    util::Timer timer;
    for (auto& txn : dependent_txn_) {
        INVARIANT(txn != nullptr);
        while (true) {
            TxnStatus stats = txn->get_txn_status();
            if (stats == TxnStatus::ABORT) {
                return false;
            }
            if (stats == TxnStatus::COMMITABLE) {
                // Update the commit timestamp now:
                timestamp_t ts = txn->get_commit_ts();
                UpdateMaxAccessedTimestamp(ts);
                break;
            }
            YieldToNext(yield);
        }
    }
    txn_detail_.wait_dependent_txn_dura += timer.ns_elapse();
    return true;
}

Status Txn::CommitLocalVersion() {
    for (auto& record : selected_records_) {
        if (record.has_write()) {
            record.commit_local_version();
        }
    }
    return Status::OK();
}

Status Txn::CommitWriteLocal(coro_yield_t& yield) {
    // For read-only transaction, no need to do any write
    if (txn_type_ == TxnType::READ_ONLY) {
        return Status::OK();
    }

    PHASE_LATENCY_TRACK_START(TxnPhase::COMMIT_PHASE);
    bool has_rdma_op = false;

    if (TxnConfig::ENABLE_REDO_LOG && redo_log_written()) {
        UpdateLogStatus(LogStatus::COMMITTED);
        has_rdma_op = true;
    }

    for (auto& record : selected_records_) {
        // Update the commit versions once the transaction ensures that it can
        // commit this record. However, the transaction must update the version
        // before it reduce the reference count of this record. This
        // ensures the last committer always see the correct commit timestamp
        if (record.has_write()) {
            record.UpdateVersions(commit_ts_);
        }
        record.record_handle_.Release(&record);
        if (record.has_write() && record.need_dg_commit()) {
            WriteLatestCommitableVersion(&record);
            has_rdma_op = true;
        }
    }

    for (auto& record : insert_records_) {
        record.UpdateVersions(commit_ts_);
        WritePrimaryNewRecord(&record);
        WriteBackupNewRecord(&record);
        has_rdma_op = true;
    }

    if (has_rdma_op) {
        YieldForPoll(yield);
    }
    CheckPendingRequestNum();

    if (TxnConfig::ENABLE_REDO_LOG) {
        UnrefDependentLogEntries(false);
    }

    PHASE_LATENCY_TRACK_END(TxnPhase::COMMIT_PHASE);
    return Status::OK();
}

Status Txn::WriteLatestCommitableVersion(TxnRecord* record) {
    if (!record->is_dg_commit_base_version()) {
        WritePrimaryDelegationRecord(record);
        WriteBackupDelegationRecords(record);
    } else {
        IO()->ReleaseWriteLockDelegation(record);
    }

    return Status::OK();
}

void Txn::UnrefDependentLogEntries(bool is_abort) {
    // A record within a Redo Log entry can be eliminated if:
    //  * The transaction creates this record is aborted
    //  * A higher version of this record is committed: either in the log
    //    or in the table
    if (is_abort) {
        // If a transaction is aborted, all log entries belonging to this
        // transaction are discarded
        redo_entry_->ResetLogRef();
    }

    for (auto& record : selected_records_) {
        if (!record.has_write()) {
            continue;
        }
        if (!is_abort) {
            // If I am not abort, then I either write the committed redo log or I
            // write the table. In both cases, the dependent entries can be released
            RedoEntry* dep_redo_entry = record.GetDependentRedoEntry();
            if (dep_redo_entry != nullptr) {
                dep_redo_entry->ReduceLogRef();
            }
        }
        // If I write the table, then the redo entry of the committed version can
        // be released. Note that the redo entry must not have been reduced before,
        // this is because if Txn2 reduced the redo entry of Txn1, then either Txn2
        // is the commiter, or Txn2 creates a newest commitable version
        if (record.need_dg_commit()) {
            RedoEntry* commit_version_redo_entry = record.GetCommitVersionRedoEntry();
            if (commit_version_redo_entry != nullptr) {
                commit_version_redo_entry->ReduceLogRef();
            }
        }
    }
    return;
}

void Txn::CopyWriterDataToReader(TxnRecord* record, const ColumnSet& copy_columns) {}