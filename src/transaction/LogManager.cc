#include "transaction/LogManager.h"

#include <cstddef>
#include <cstdint>
#include <sstream>

#include "db/Format.h"
#include "db/Schema.h"
#include "transaction/Enums.h"
#include "util/Logger.h"
#include "util/Status.h"

const char* RecoverEntry::Construct(const char* src) {
  this->txn_id = Next<TxnId>(src);
  this->status = Next<LogStatus>(src);

  // Construct each record value:
  while (TryNext<uint64_t>(src) != LOG_END_MARKER) {
    RedoRecordHeader* record_header = (RedoRecordHeader*)src;
    TxnSchema* txn_schema = db->GetTable(record_header->table_id)->GetTxnSchema();
    RecordValueSchema* record_schema = db->GetTable(record_header->table_id)->GetRecordSchema();

    // Construct the TxnRecord:
    redo_records.emplace_back(
        /* table_id      = */ record_header->table_id,
        /* record_key    = */ record_header->record_key,
        /* db_txn        = */ nullptr,
        /* db_schema     = */ txn_schema,
        /* record_schema = */ record_schema,
        /* access_mode   = */ AccessMode::READ_ONLY);

    // Create dynamic data array
    char* data = new char[txn_schema->GetRecordMemSize()];
    redo_records.back().set_data(data);

    // Initialize the row header
    std::memcpy(data, src, RecordHeaderSize);
    src += sizeof(RecordHeader);

    // Initialize the modified columns
    src = redo_records.back().ReadFromLog(src);
  }
  return src + sizeof(uint64_t);
}

std::string RecoverEntry::ToString() {
  std::stringstream ss;
  ss << "[Log]"
     << "[TxnId : " << TxnIdToString(this->txn_id) << "]"
     << "[Status : " << LogStatusString[static_cast<uint64_t>(this->status)] << "]\n";
  ss << "Redo Parts:\n";
  return ss.str();
}

RedoEntry* LogManager::AllocateNewLogEntry(TxnId txn_id) {
  RedoEntry* ret = new RedoEntry();
  ret->log_manager = this;
  ret->txn_id = txn_id;
  // TxnLogManager will allocate the remote start-offset for this redo entry
  ret->start_offset = INVALID_OFFSET;
  ret->record_count.store(0);
  ret->start = buf_;
  ret->tail = ret->start + RedoEntryHeaderSize;
  ret->size = 0;
  return ret;
}

Status LogManager::AllocatePoolAddress(RedoEntry* entry) {
  if (RemainingSize() < entry->size) {
    GarbageCollection(entry->size);
  }
  if (RemainingSize() >= entry->size) {
    entry->start_offset = this->tail_offset_;
    this->tail_offset_ += entry->size;
    ASSERT(this->tail_offset_ <= PER_COODINATOR_LOG_SIZE,
           "No enough space to write redo log entry");
    // Add this entry
    redo_entries_.push_back(entry);
    return Status::OK();
  } else {
    return Status(kTxnError, "No enough space to write redo log entry");
  }
}

void LogManager::GarbageCollection(size_t sz) {
  // Try to erase all unreferenced redo entries
  RedoEntry* entry = redo_entries_.front();
  while (true) {
    if (entry->record_count <= 0) {
      redo_entries_.pop_front();
      // delete entry;
      // Reset start offset to the next log entry
      if (!redo_entries_.empty()) {
        entry = redo_entries_.front();
        start_offset_ = entry->start_offset;
      } else {
        // No entry any more
        start_offset_ = tail_offset_ = 0;
        break;
      }
    } else {
      break;
    }
  }

  if (start_offset_ <= tail_offset_ && RemainingSize() < sz) {
    tail_offset_ = 0;
  }

  LOG_DEBUG("Finish Garbage Collection");
}
