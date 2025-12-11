#pragma once

#include <absl/container/flat_hash_map.h>
#include <bits/types/struct_FILE.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
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
#include "transaction/Enums.h"
#include "transaction/TxnRecord.h"
#include "util/Macros.h"
#include "util/Status.h"
#include "util/Timer.h"

template <typename T>
T Next(const char*& src) {
  T v = *reinterpret_cast<const T*>(src);
  src += sizeof(T);
  return v;
}

template <typename T>
T TryNext(const char* src) {
  T v = *reinterpret_cast<const T*>(src);
  return v;
}

template <typename T>
void WriteNext(char*& dst, T v) {
  *reinterpret_cast<T*>(dst) = v;
  dst += sizeof(T);
}

// LOG_END_MARKER is a random 8B value which is unlikely to conflict with
// the actual content of another log entry. So using this value would most
// probably be safe to indicate the end of a log entry
static constexpr uint64_t LOG_END_MARKER = 0xdeadbeefdeadbeef;

struct RedoRecordHeader {
  TableId table_id;
  RecordKey record_key;
};

class LogManager;
class RedoEntry;

// The TxnLogManager manages a coordinator's private log buffer. All
// redo log entry created by this coordinator must be allocated from
// this LogManager's internal buffer
class LogManager {
 public:
  friend class Txn;
  friend class RedoEntry;

  static constexpr size_t PER_COODINATOR_LOG_SIZE = 1ULL * 1024 * 1024;

 public:
  LogManager(char* buf) : buf_(buf), start_offset_(0), tail_offset_(0) {}

  void SetPrimaryAddress(PoolPtr addr) { primary_address_ = addr; }
  PoolPtr GetPrimaryAddress() const { return primary_address_; }

  void AddBackupAddress(PoolPtr addr) { backup_address_.push_back(addr); }
  const std::vector<PoolPtr>& GetBackupAddress() const { return backup_address_; }

 public:
  RedoEntry* AllocateNewLogEntry(TxnId txn_id);

  // Allocate the memory pool address for this redo log entry
  Status AllocatePoolAddress(RedoEntry* entry);

  // Size of memory remained
  size_t RemainingSize() const {
    if (start_offset_ <= tail_offset_) {
      return PER_COODINATOR_LOG_SIZE - tail_offset_;
    } else {
      return start_offset_ - tail_offset_;
    }
  }

 private:
  void GarbageCollection(size_t sz);

 private:
  char* buf_;

  PoolPtr primary_address_;
  std::vector<PoolPtr> backup_address_;

  std::deque<RedoEntry*> redo_entries_;  // Maintain all allocated redo log entries
  uint64_t start_offset_, tail_offset_;
};

// Per-transactions redo-log entry
struct RedoEntryHeader {
  TxnId txn_id;
  LogStatus status;
};
constexpr size_t RedoEntryHeaderSize = sizeof(RedoEntryHeader);

constexpr uint64_t INVALID_OFFSET = static_cast<uint64_t>(-1);

struct RedoEntry {
  TxnId txn_id;                       // Unique identifier of this redo_entry
  std::atomic<int> record_count;      // Number of records contained in this log
  uint64_t size;                      // The memory used by this log entry
  LogManager* log_manager;            // The log manager that manages this log entry
  char* start;                        // The start of this log entry in local buffer
  char* tail;                         // The tail of this log entry in local buffer
  std::vector<TxnId> dependent_txns;  // The dependent transactions
  uint64_t start_offset;              // The offset where the entry starts on memory side

  char* data() const { return start; }

  size_t entry_size() const { return size; }

  PoolPtr GetPrimaryAddress() const { return log_manager->primary_address_ + start_offset; }
  PoolPtr GetPrimaryStatusAddress() const {
    return log_manager->primary_address_ + start_offset + sizeof(TxnId);
  }

  uint64_t GetLogStatusOffset() const { return offsetof(RedoEntryHeader, status) + start_offset; }

  void WriteLocalEntryHeader(TxnId txn_id, LogStatus s) {
    RedoEntryHeader* header = reinterpret_cast<RedoEntryHeader*>(start);
    header->txn_id = txn_id;
    header->status = s;
  }

  void WriteRedoEntry(TxnRecord* record) {
    this->tail = record->WriteLog(tail);
    auto ret = record_count.fetch_add(1);
    // printf("Txn%lu add record count: %d\n", txn_id, ret);
    size = tail - start;
  }

  void WriteDependentTransactions() {
    WriteNext<size_t>(tail, dependent_txns.size());
    for (const auto& tx_id : dependent_txns) {
      WriteNext<TxnId>(tail, tx_id);
    }
    size = tail - start;
  }

  void WriteEndMarker() {
    WriteNext<uint64_t>(tail, LOG_END_MARKER);
    size = tail - start;
  }

  void AddDependentTxns(TxnId txn_id) { dependent_txns.push_back(txn_id); }

  void ReduceLogRef() {
    int ref = record_count.fetch_sub(1);
    // printf("Txn%lu record count: %d\n", txn_id, ref);
  }

  void ResetLogRef() { record_count.store(0); }
};

struct RecoverEntry {
  TxnId txn_id;
  LogStatus status;
  std::vector<TxnId> dependent_txns;
  std::vector<TxnRecord> redo_records;
  Db* db;

  RecoverEntry(Db* db) : db(db) {}
  // We do not allow to copy this entry as the redo_records contains
  // heap-allocated memory.
  RecoverEntry(const RecoverEntry&) = delete;
  RecoverEntry& operator=(const RecoverEntry&) = delete;

  ~RecoverEntry() {
    for (auto& record : redo_records) {
      delete[] record.get_data();
    }
  }

  // Construct this recover entry from a given source position
  const char* Construct(const char* src);

  // For debug and illustration
  std::string ToString();
};

class DbRestorer {
 public:
  // The DbRestoer only runs on memory nodes
  DbRestorer(Pool* pool, Db* db) : pool_(pool), db_(db) { INVARIANT(pool->IsMN()); }

  char* GetLogAreaBase() const {
    char* mr_addr = pool_->GetLocalDataRegion().first;
    return mr_addr + 4096;
  }

  char* GetLogArea(ThreadId tid, coro_id_t cid) const {
    return GetLogAreaBase() + LogManager::PER_COODINATOR_LOG_SIZE * (tid * 2 + cid);
  }

  // For Debug
  std::string LogEntryToString(ThreadId tid, coro_id_t cid) const {
    RecoverEntry r_ent(db_);
    r_ent.Construct(GetLogArea(tid, cid));
    return r_ent.ToString();
  }

 private:
  Pool* pool_;
  Db* db_;
};