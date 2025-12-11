#include <boost/core/allocator_access.hpp>
#include <cstdint>
#include <cstring>

#include "common/Config.h"
#include "common/Type.h"
#include "db/Format.h"
#include "db/Schema.h"
#include "db/Table.h"
#include "mempool/Coroutine.h"
#include "rdma/Doorbell.h"
#include "rdma/RdmaBatch.h"
#include "transaction/Enums.h"
#include "transaction/TimestampGen.h"
#include "transaction/Txn.h"
#include "transaction/TxnConfig.h"
#include "util/Logger.h"
#include "util/Macros.h"
#include "util/Status.h"
#include "util/Timer.h"

Status Txn::IOManager::AcquireWriteLock(TxnRecord* record) {
    INVARIANT(!record->is_remote_locked());

    PoolPtr lock_address = record->lock_addr();
    char* lock_buf = buf_manager_->AllocAlign(8, WORD_SIZE);
    ASSERT(lock_buf, "Memory allocation failed");

    uint64_t lock_mask = record->rw_bitmap();
    // We use the dg_lock_bitmap as the lock mask value if the local
    // execution is enabled. The only difference between enabling
    // local execution or not is that what lock mask value they use
    // for acquiring remote lock
    if (TxnConfig::ENABLE_LOCAL_EXECUTION) {
        lock_mask = record->dg_lock_bitmap();
    }

    uint64_t compare_val = 0x00ULL;
    uint64_t swap_val = lock_mask;

    Status s = pool_->PostMaskedCAS(GetCoroId(), lock_address, lock_buf, compare_val, swap_val,
                                    lock_mask, lock_mask);
    ASSERT(s.ok(), "Fail to post masked CAS");
    record->set_lock_data(lock_buf);
    txn_->ADD_RDMA_ATOMIC_STATS(8);
    return s;
}

Status Txn::IOManager::AcquireLockAndRead(TxnRecord* record) {
    INVARIANT(!record->is_remote_locked());

    PoolPtr lock_address = record->lock_addr();
    char* lock_buf = buf_manager_->AllocAlign(8, WORD_SIZE);
    ASSERT(lock_buf, "Memory allocation failed");

    uint64_t lock_mask = 0x00ULL;
    if (record->is_record_cc()) {
        // For record-level CC, simply set one bit of the Lock field is okay
        // lock_mask = (0x01ULL << 63);
        lock_mask = record->rw_bitmap();
    } else if (record->is_column_cc()) {
        lock_mask = record->rw_bitmap();
        if (TxnConfig::ENABLE_LOCAL_EXECUTION) {
            lock_mask = record->dg_lock_bitmap();
        }
    }

    uint64_t compare_val = 0x00ULL;
    uint64_t swap_val = lock_mask;
    INVARIANT(lock_mask != 0x00ULL);

    LockReadBatch batch;
    batch.LockColumns(lock_buf, compare_val, swap_val, lock_mask, record->lock_addr());
    record->set_lock_data(lock_buf);
    txn_->ADD_RDMA_ATOMIC_STATS(8);

    AllocateBuffer(record);
    batch.ReadRecord(record->get_data(), record->record_size(), record->pool_addr());
    txn_->ADD_RDMA_READ_STATS(1, record->record_size());
    record->set_access_status(RecordAccessStatus::READ_RECORD);

    Status s = pool_->PostReadLockBatch(GetCoroId(), &batch);
    ASSERT(s.ok(), "Fail to post ReadLockBatch");
    return Status::OK();
}

Status Txn::IOManager::AcquireUsedFlag(TxnRecord* record) {
    INVARIANT(record->is_insert());
    PoolPtr address = record->pool_addr();
    char* lock_buf = buf_manager_->AllocAlign(8, WORD_SIZE);
    char* data_buf = buf_manager_->AllocAlign(record->record_size(), CACHE_LINE_SIZE);
    if (unlikely(!lock_buf || !data_buf)) {
        return Status(kMemError, "Memory allocation failed");
    }
    Status s = pool_->PostCAS(GetCoroId(), record->used_flag_addr(), lock_buf, UNUSED, USED);
    ASSERT(s.ok(), "Transaction Post CAS failed");

    // Reuse the lock field for used flag acquire results
    record->set_lock_data(lock_buf);
    record->set_data(data_buf);
    txn_->ADD_RDMA_ATOMIC_STATS(8);
    return s;
}

Status Txn::IOManager::ReleaseUsedFlag(TxnRecord* record) {
    INVARIANT(record->is_insert() && record->is_remote_locked());
    *(uint64_t*)(record->get_lock_data()) = UNUSED;
    Status s = pool_->PostWrite(GetCoroId(), record->get_lock_data(), 8, record->used_flag_addr());
    ASSERT(s.ok(), "Transaction release used flag failed");
    txn_->ADD_RDMA_WRITE_STATS(1, 8);
    return s;
}

Status Txn::IOManager::AllocateBuffer(TxnRecord* record) {
    char* record_buf = buf_manager_->AllocAlign(record->record_size(), CACHE_LINE_SIZE);
    ASSERT(record_buf != nullptr, "Memory Allocation Failed");
    record_buf[record->record_size() - 1] = (char)0;
    record->set_data(record_buf);
    // char* mr_bound =
    // (char*)(pool_->GetQueuePair(0)->GetLocalMemoryRegionToken().get_region_addr());
    return Status::OK();
}

Status Txn::IOManager::ReadRecord(TxnRecord* record) {
    PoolPtr record_addr = record->pool_addr();
    AllocateBuffer(record);
    Status s = pool_->PostRead(GetCoroId(), record_addr, record->record_size(), record->get_data());
    ASSERT(s.ok(), "");
    txn_->ADD_RDMA_READ_STATS(1, record->record_size());

    record->set_access_status(RecordAccessStatus::READ_RECORD);
    return Status::OK();
}

Status Txn::IOManager::ReadRecordValidation(TxnRecord* record) {
    PoolPtr record_address = record->pool_addr();
    // Only read the record header for validation:
    char* record_buf = buf_manager_->AllocAlign(record->record_size(), RecordHeaderSize);
    ASSERT(record_buf != nullptr, "Memory Allocation Failed");

    uint64_t rd_size = RecordHeaderSize;
    Status s = pool_->PostRead(GetCoroId(), record_address, rd_size, record_buf);
    ASSERT(s.ok(), "");
    txn_->ADD_RDMA_READ_STATS(1, rd_size);

    record->set_validate_data(record_buf);
    return Status::OK();
}

Status Txn::IOManager::ReleaseWriteLock(TxnRecord* record) {
    INVARIANT(record->is_remote_locked());
    INVARIANT(record->get_lock_data() != nullptr);

    PoolPtr lock_address = record->lock_addr();
    char* lock_buf = record->get_lock_data();
    if (record->is_record_cc()) {
        // Directly clear the fields
        write_data<uint64_t>(lock_buf, 0);
        Status s = pool_->PostWrite(GetCoroId(), lock_buf, sizeof(uint64_t), lock_address);
        txn_->ADD_RDMA_WRITE_STATS(1, 8);
    } else if (record->is_column_cc()) {
        uint64_t lock_mask = record->rw_bitmap();
        if (TxnConfig::ENABLE_LOCAL_EXECUTION) {
            lock_mask = record->dg_commit_bitmap();
        }
        uint64_t compare_val = lock_mask;
        uint64_t swap_val = 0;

        // clang-format off
    Status s = pool_->PostMaskedCAS(GetCoroId(),
                  /* remote_addr  = */ lock_address,
                  /* local_addr   = */ record->get_lock_data(),
                  /* compare_val  = */ compare_val,
                  /* swap_val     =*/  swap_val,
                  /* compare_mask =*/  lock_mask,
                  /* swap_mask    =*/  lock_mask);
        // clang-format on
        ASSERT(s.ok(), "Transaction PostMaskedCAS failed");
        txn_->ADD_RDMA_ATOMIC_STATS(8);
        return s;
    }
    return Status::OK();
}

Status Txn::IOManager::ReleaseWriteLockDelegation(TxnRecord* record) {
    char* lock_buf = record->get_lock_data();
    PoolPtr lock_address = record->lock_addr();
    // For the delegation case, we have no guarantee that the record has
    // its own lock buffer as this transaction might be a delegator to
    // release the write lock
    if (lock_buf == nullptr) {
        lock_buf = buf_manager_->AllocAlign(8, WORD_SIZE);
        ASSERT(lock_buf, "Allocate lock buffer failed");
    }
    if (record->is_record_cc()) {
        write_data<uint64_t>(lock_buf, 0);
        Status s = pool_->PostWrite(GetCoroId(), lock_buf, sizeof(uint64_t), lock_address);
        txn_->ADD_RDMA_WRITE_STATS(1, 8);
    } else if (record->is_column_cc()) {
        uint64_t lock_mask = record->rw_bitmap();
        if (TxnConfig::ENABLE_LOCAL_EXECUTION) {
            lock_mask = record->dg_commit_bitmap();
        }
        uint64_t compare_val = lock_mask;
        uint64_t swap_val = 0;

        Status s = pool_->PostMaskedCAS(GetCoroId(),
                                        /* remote_addr  = */ lock_address,
                                        /* local_addr   = */ lock_buf,
                                        /* compare_val  = */ compare_val,
                                        /* swap_val     =*/swap_val,
                                        /* compare_mask =*/lock_mask,
                                        /* swap_mask    =*/lock_mask);
        ASSERT(s.ok(), "Transaction PostMaskedCAS failed");
        txn_->ADD_RDMA_ATOMIC_STATS(8);
        return s;
    }
    return Status::OK();
}

Status Txn::IOManager::WriteRecordDelegation(TxnRecord* record, PoolPtr record_address) {
    INVARIANT(record_address != POOL_PTR_NULL);

    // The transaction only writes columns that is in its dg_commit_bitmap()
    // which means the transaction only writes the columns that is modified by itself
    ColumnSet bs = (record->dg_commit_bitmap() & record->dg_commit_version_bitmap());

    ColumnSet final_write_bs = ColumnSet::EmptySet();
    ColumnId start_cid, end_cid;
    TxnSchema* txn_schema = record->txn_schema();
    WriteRecordBatch batch;
    char* commit_data = record->get_dg_commit_data();

    // Update the version of each column to write
    ColumnSet::SingleIterator s_iter = bs.Iter();
    while (s_iter.Valid()) {
        ColumnId cid = s_iter.Value();
        s_iter.Next();
        ColumnHeader* header = (ColumnHeader*)(commit_data + txn_schema->ColumnOffset(cid));
        version_t v = BASE_VERSION;
        if (record->is_record_cc()) {
            v = record->record_handle_.GetCoarseHandle()->commit_version();
        } else if (record->is_column_cc()) {
            v = record->record_handle_.GetFineHandle()->commit_version(cid);
        }
        if (v != BASE_VERSION) {
            header->version = v;
            final_write_bs.Add(cid);
        }
    }

    ColumnSet::RangeIterator range_iter = final_write_bs.RangeIter();
    while (range_iter.Valid()) {
        range_iter.Value(&start_cid, &end_cid);
        range_iter.Next();

        // 1. Write the column data itself
        size_t start_off = txn_schema->ColumnOffset(start_cid);
        uint64_t write_sz = txn_schema->MultiColumnSize(start_cid, end_cid);
        void* laddr = record->get_dg_commit_data() + start_off;
        PoolPtr raddr = record_address + start_off;
        batch.WriteColumns(laddr, write_sz, raddr);
        txn_->ADD_RDMA_WRITE_STATS(1, write_sz);

        // 2. Write the subversion data itself:
        size_t subversion_off = txn_schema->SubVersionOffset(start_cid);
        uint64_t subversion_write_sz = (end_cid - start_cid + 1) * sizeof(sub_version_t);
        laddr = record->get_dg_commit_data() + subversion_off;
        raddr = record_address + subversion_off;

        batch.WriteSubversions(laddr, subversion_write_sz, raddr);
        txn_->ADD_RDMA_WRITE_STATS(1, subversion_write_sz);

        pool_->PostWriteBatch(GetCoroId(), &batch);
    }
    return Status::OK();
}

Status Txn::IOManager::WriteRecord(TxnRecord* record, PoolPtr record_address) {
    // INVARIANT(record->pool_addr() != POOL_PTR_NULL);
    INVARIANT(record_address != POOL_PTR_NULL);

    ColumnSet bs = record->rw_bitmap();
    ColumnSet::RangeIterator range_iter = bs.RangeIter();
    ColumnId start_cid, end_cid;
    TxnSchema* txn_schema = record->txn_schema();
    WriteRecordBatch batch;
    while (range_iter.Valid()) {
        // Issue a write operation for consecutive columns: [start_cid, end_cid]
        range_iter.Value(&start_cid, &end_cid);
        range_iter.Next();

        // 1. Write the column data itself
        size_t start_off = txn_schema->ColumnOffset(start_cid);
        uint64_t write_sz = txn_schema->MultiColumnSize(start_cid, end_cid);
        void* laddr = record->get_data() + start_off;
        PoolPtr raddr = record_address + start_off;
        batch.WriteColumns(laddr, write_sz, raddr);
        txn_->ADD_RDMA_WRITE_STATS(1, write_sz);

        // 2. Write the subversion data itself:
        size_t subversion_off = txn_schema->SubVersionOffset(start_cid);
        uint64_t subversion_write_sz = (end_cid - start_cid + 1) * sizeof(sub_version_t);
        laddr = record->get_data() + subversion_off;
        raddr = record_address + subversion_off;

        batch.WriteSubversions(laddr, write_sz, raddr);
        txn_->ADD_RDMA_WRITE_STATS(1, subversion_write_sz);

        pool_->PostWriteBatch(GetCoroId(), &batch);
    }

    return Status::OK();
}

Status Txn::IOManager::WriteRecordDelegationColumnLevel(TxnRecord* record) {
    INVARIANT(record->pool_addr() != POOL_PTR_NULL);
    ColumnId start_cid = INVALID_COLUMN_ID;
    ColumnId end_cid = start_cid;
    TxnSchema* txn_schema = record->txn_schema();
    for (size_t cid = 0; cid < record->column_count(); ++cid) {
        if (record->is_dg_commit_column(cid) && start_cid == INVALID_COLUMN_ID) {
            start_cid = cid;
            end_cid = cid;
        } else if (record->is_dg_commit_column(cid) && start_cid != INVALID_COLUMN_ID) {
            end_cid = cid;
        } else if (!record->is_dg_commit_column(cid) && start_cid == INVALID_COLUMN_ID) {
            continue;
        } else if (!record->is_dg_commit_column(cid) && start_cid != INVALID_COLUMN_ID) {
            // Issue a write operation from start_cid to end_cid:
            size_t start_off = txn_schema->ColumnOffset(start_cid);
            uint64_t write_sz = txn_schema->ColumnOffset(end_cid) -
                                txn_schema->ColumnOffset(start_cid) +
                                txn_schema->ColumnMemSize(end_cid);
            Status s = pool_->PostWrite(GetCoroId(), record->get_dg_commit_data() + start_off,
                                        write_sz, record->pool_addr() + start_off);
            ASSERT(s.ok(), "");
            txn_->ADD_RDMA_WRITE_STATS(1, write_sz);
            start_cid = INVALID_COLUMN_ID;
            end_cid = INVALID_COLUMN_ID;
        }
    }

    if (start_cid != INVALID_COLUMN_ID && end_cid != INVALID_COLUMN_ID) {
        // Issue a write operation from start_cid to end_cid:
        size_t start_off = txn_schema->ColumnOffset(start_cid);
        uint64_t write_sz = txn_schema->ColumnOffset(end_cid) -
                            txn_schema->ColumnOffset(start_cid) +
                            txn_schema->ColumnMemSize(end_cid);
        Status s = pool_->PostWrite(GetCoroId(), record->get_dg_commit_data() + start_off, write_sz,
                                    record->pool_addr() + start_off);
        ASSERT(s.ok(), "");
        txn_->ADD_RDMA_WRITE_STATS(1, write_sz);
    }
    return Status::OK();
}

Status Txn::IOManager::WriteRecordDelegationRecordLevel(TxnRecord* record) {
    INVARIANT(record->pool_addr() != POOL_PTR_NULL);
    ColumnId last_cid = record->column_count() - 1;
    TxnSchema* schema = record->txn_schema();
    uint64_t start_off = schema->ColumnOffset(0);
    uint64_t write_sz =
        schema->ColumnOffset(last_cid) - start_off + schema->ColumnMemSize(last_cid);
    Status s = pool_->PostWrite(GetCoroId(), record->get_dg_commit_data() + start_off, write_sz,
                                record->pool_addr() + start_off);
    txn_->ADD_RDMA_WRITE_STATS(1, write_sz);
    ASSERT(s.ok(), "");
    return Status::OK();
}

Status Txn::IOManager::WriteNewRecord(TxnRecord* record, PoolPtr pool_address) {
    Status s =
        pool_->PostWrite(GetCoroId(), record->get_data(), record->record_size(), pool_address);
    ASSERT(s.ok(), "");
    txn_->ADD_RDMA_WRITE_STATS(1, record->record_size());
    return s;
}

Status Txn::IOManager::ReadHashBucket(TxnRecord* record, PoolPtr bkt_address) {
    char* bkt_buf = nullptr;
    if (!BucketAlreadyRead(bkt_address, &bkt_buf)) {
        size_t bucket_sz = db_->GetTable(record->table_id())->GetBucketSize();
        char* allocate_bkt_buf = buf_manager_->AllocAlign(bucket_sz, CACHE_LINE_SIZE);
        ASSERT(allocate_bkt_buf != nullptr, "Memory allocation failed");

        Status s = pool_->PostRead(GetCoroId(), bkt_address, bucket_sz, allocate_bkt_buf);
        ASSERT(s.ok(), "Read bucket failed");

        txn_->ADD_RDMA_READ_STATS(1, bucket_sz);

        bkt_read_hist_.emplace_back(
            BucketRead{.bkt_ptr = bkt_address, .bkt_buffer = allocate_bkt_buf});
        bkt_buf = allocate_bkt_buf;
    }

    record->set_access_status(RecordAccessStatus::READ_BUCKET);
    record->set_data(bkt_buf);
    record->set_pool_addr(bkt_address);  // Set the bucket address

    return Status::OK();
}

bool Txn::IOManager::BucketAlreadyRead(PoolPtr bkt_address, char** bkt_buf) {
    for (const auto& bkt_read : bkt_read_hist_) {
        if (bkt_read.bkt_ptr == bkt_address) {
            *bkt_buf = bkt_read.bkt_buffer;
            return true;
        }
    }
    return false;
}

Status Txn::IOManager::WriteRedoEntry(RedoEntry* entry) {
    PoolPtr primary_address = entry->GetPrimaryAddress();
    Status s = pool_->PostWrite(GetCoroId(), entry->data(), entry->entry_size(), primary_address);
    ASSERT(s.ok(), "Write log entry failed");
    txn_->ADD_RDMA_WRITE_STATS(1, entry->entry_size());

    for (const auto& backup : entry->log_manager->GetBackupAddress()) {
        PoolPtr backup_address = backup + entry->start_offset;
        s = pool_->PostWrite(GetCoroId(), entry->data(), entry->entry_size(), backup_address);
        ASSERT(s.ok(), "Write log entry failed");
        txn_->ADD_RDMA_WRITE_STATS(1, entry->entry_size());
    }

    return s;
}

Status Txn::IOManager::UpdateLogStatus(RedoEntry* entry) {
    uint64_t offset = entry->GetLogStatusOffset();
    ASSERT(offset >= 0 && offset < LogManager::PER_COODINATOR_LOG_SIZE, "Invalid offset");
    PoolPtr primary_address = entry->GetLogStatusOffset() + entry->log_manager->primary_address_;
    Status s = pool_->PostWrite(GetCoroId(), entry->data() + 8, sizeof(LogStatus), primary_address);
    ASSERT(s.ok(), "Write log status failed");
    txn_->ADD_RDMA_WRITE_STATS(1, 8);

    for (const auto& backup : entry->log_manager->GetBackupAddress()) {
        PoolPtr backup_address = backup + offset;
        s = pool_->PostWrite(GetCoroId(), entry->data() + 8, sizeof(LogStatus), backup_address);
        ASSERT(s.ok(), "Update log status failed");
        txn_->ADD_RDMA_WRITE_STATS(1, 8);
    }
    return s;
}
