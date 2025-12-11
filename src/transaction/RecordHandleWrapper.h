#pragma once

#include "common/Type.h"
#include "db/Schema.h"
#include "transaction/Bitset.h"
#include "transaction/Enums.h"
#include "transaction/RecordHandle.h"

class TxnRecord;

class RecordHandleWrapper {
   public:
    RecordHandleWrapper(TableId table_id, RecordKey record_key, RecordHandleBase* handle_impl,
                        TableCCLevel cc_level)
        : cc_level_(cc_level), handle_impl_(handle_impl) {}

    RecordHandleWrapper() : cc_level_(TableCCLevel::RECORD_LEVEL), handle_impl_(nullptr) {}
    RecordHandleWrapper(const RecordHandleWrapper&) = default;
    RecordHandleWrapper& operator=(const RecordHandleWrapper&) = default;

    CoarseRecordHandle* GetCoarseHandle() {
        INVARIANT(cc_level_ == TableCCLevel::RECORD_LEVEL);
        return (CoarseRecordHandle*)handle_impl_;
    }

    FineRecordHandle* GetFineHandle() {
        INVARIANT(cc_level_ == TableCCLevel::CELL_LEVEL);
        return (FineRecordHandle*)handle_impl_;
    }

    // Wrapper functions:
    bool Acquire(TxnRecord* txn_record) {
        if (cc_level_ == TableCCLevel::RECORD_LEVEL) {
            return GetCoarseHandle()->Acquire(txn_record);
        } else if (cc_level_ == TableCCLevel::CELL_LEVEL) {
            return GetFineHandle()->Acquire(txn_record);
        }
        ASSERT(false, "Impossible control flow");
    }

    void Release(TxnRecord* txn_record) {
        if (cc_level_ == TableCCLevel::RECORD_LEVEL) {
            return GetCoarseHandle()->Release(txn_record);
        } else if (cc_level_ == TableCCLevel::CELL_LEVEL) {
            return GetFineHandle()->Release(txn_record);
        }
        ASSERT(false, "Impossible control flow");
    }

    Status Access(TxnRecord* txn_) {
        if (cc_level_ == TableCCLevel::RECORD_LEVEL) {
            return GetCoarseHandle()->Access(txn_);
        } else if (cc_level_ == TableCCLevel::CELL_LEVEL) {
            return GetFineHandle()->Access(txn_);
        }
        ASSERT(false, "Impossible control flow");
    }

    void acquire_lock(LockMode mode) {
        if (cc_level_ == TableCCLevel::RECORD_LEVEL) {
            return GetCoarseHandle()->acquire_lock(mode);
        } else if (cc_level_ == TableCCLevel::CELL_LEVEL) {
            return GetFineHandle()->acquire_lock(mode);
        }
        ASSERT(false, "Impossible control flow");
    }

    void release_lock(LockMode mode) {
        if (cc_level_ == TableCCLevel::RECORD_LEVEL) {
            return GetCoarseHandle()->release_lock(mode);
        } else if (cc_level_ == TableCCLevel::CELL_LEVEL) {
            return GetFineHandle()->release_lock(mode);
        }
        ASSERT(false, "Impossible control flow");
    }

    void update_commit_index(int idx) {
        if (cc_level_ == TableCCLevel::RECORD_LEVEL) {
            return GetCoarseHandle()->update_commit_index(idx);
        } else if (cc_level_ == TableCCLevel::CELL_LEVEL) {
            return GetFineHandle()->update_commit_index(idx);
        }
        ASSERT(false, "Impossible control flow");
    }

    void UpdateWriteAccessStatus(TxnRecord* txn_record) {
        if (cc_level_ == TableCCLevel::RECORD_LEVEL) {
            return GetCoarseHandle()->UpdateWriteAccessStatus(txn_record);
        } else if (cc_level_ == TableCCLevel::CELL_LEVEL) {
            return GetFineHandle()->UpdateWriteAccessStatus(txn_record);
        }
        ASSERT(false, "Impossible control flow");
    }

    void UpdateWriteLockStatus(TxnRecord* txn_record, LockStatus ls) {
        if (cc_level_ == TableCCLevel::RECORD_LEVEL) {
            return GetCoarseHandle()->UpdateWriteLockStatus(txn_record, ls);
        } else if (cc_level_ == TableCCLevel::CELL_LEVEL) {
            return GetFineHandle()->UpdateWriteLockStatus(txn_record, ls);
        }
        ASSERT(false, "Impossible control flow");
    }

    bool UpdateReaderAccessStatus(TxnRecord* txn_record, const ColumnSet& bs) {
        if (cc_level_ == TableCCLevel::RECORD_LEVEL) {
            return GetCoarseHandle()->UpdateReadAccessStatus(txn_record, bs);
        } else if (cc_level_ == TableCCLevel::CELL_LEVEL) {
            return GetFineHandle()->UpdateReadAccessStatus(txn_record, bs);
        }
        ASSERT(false, "Impossible control flow");
    }

    bool UpdateFailReadAccessStatus(TxnRecord* txn_record, const ColumnSet& bs) {
        if (cc_level_ == TableCCLevel::RECORD_LEVEL) {
            return GetCoarseHandle()->UpdateFailReadAccessStatus(txn_record, bs);
        } else if (cc_level_ == TableCCLevel::CELL_LEVEL) {
            return GetFineHandle()->UpdateFailReadAccessStatus(txn_record, bs);
        }
        ASSERT(false, "Impossible control flow");
    }

    void UpdateLastWriter(TxnRecord* txn_record) {
        if (cc_level_ == TableCCLevel::RECORD_LEVEL) {
            return GetCoarseHandle()->UpdateLastWriter(txn_record);
        } else if (cc_level_ == TableCCLevel::CELL_LEVEL) {
            return GetFineHandle()->UpdateLastWriter(txn_record);
        }
        ASSERT(false, "Impossible control flow");
    }

    bool valid() { return handle_impl_ != nullptr; }

    void UpdateWriteTimestamp(timestamp_t ts_exec, int v_idx) {
        if (cc_level_ == TableCCLevel::RECORD_LEVEL) {
            return GetCoarseHandle()->UpdateWriteTimestamp(ts_exec, v_idx);
        } else if (cc_level_ == TableCCLevel::CELL_LEVEL) {
            return GetFineHandle()->UpdateWriteTimestamp(ts_exec, v_idx);
        }
    }

   private:
    TableCCLevel cc_level_;
    RecordHandleBase* handle_impl_;
};