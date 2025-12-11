#include <bitset>
#include <boost/unordered/concurrent_flat_map.hpp>
#include <boost/unordered/concurrent_flat_map_fwd.hpp>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <vector>

#include "common/Type.h"
#include "db/Schema.h"
#include "transaction/RecordHandle.h"
#include "transaction/RecordHandleWrapper.h"
#include "util/Macros.h"

class RecordHandleIndex {
    using ConcurrentMap = boost::concurrent_flat_map<RecordKey, RecordHandleWrapper>;

   public:
    RecordHandleIndex(TableId table_id) : table_id_(table_id), txn_schema_(nullptr) {}

    RecordHandleWrapper SelectRecord(RecordKey record_key, AccessMode access_mode, bool& create_new,
                                     ThreadId thread_id = 0, coro_id_t coro_id = 0);

    void SetTxnSchema(TxnSchema* txn_schema) { txn_schema_ = txn_schema; }

    void SetTableCCLevel(TableCCLevel cc_level) { cc_level_ = cc_level; }

   private:
    ConcurrentMap index_;
    TableId table_id_;
    TxnSchema* txn_schema_;
    TableCCLevel cc_level_;
};

class RecordHandleDB {
   public:
    RecordHandleDB(size_t table_num) {
        for (size_t i = 0; i < table_num; ++i) {
            indexes_.emplace_back(new RecordHandleIndex(static_cast<TableId>(i)));
        }
    }

    RecordHandleIndex* get_index(TableId table_id) { return indexes_[table_id]; }

    void set_db(Db* db) {
        db_ = db;
        for (int i = 0; i < db_->GetTableNumber(); ++i) {
            TxnSchema* txn_schema = db->GetTable(i)->GetTxnSchema();
            get_index(i)->SetTxnSchema(txn_schema);
            get_index(i)->SetTableCCLevel(txn_schema->GetTableCCLevel());
        }
    }

   private:
    std::vector<RecordHandleIndex*> indexes_;
    Db* db_;
};

ALWAYS_INLINE
RecordHandleWrapper RecordHandleIndex::SelectRecord(RecordKey record_key, AccessMode access_mode,
                                                    bool& create_new, ThreadId thread_id,
                                                    coro_id_t coro_id) {
    bool found = false;
    (void)access_mode;
    RecordHandleWrapper ret;
    index_.cvisit(record_key, [&](const auto& v) {
        ret = v.second;
        found = true;
    });

    // Directly return if the object is found
    if (found) {
        return ret;
    }

    // Otherwise insert a new object
    create_new = true;
    RecordHandleBase* new_handle = nullptr;
    if (cc_level_ == TableCCLevel::RECORD_LEVEL) {
        new_handle = new CoarseRecordHandle(table_id_, record_key, txn_schema_);
    } else if (cc_level_ == TableCCLevel::CELL_LEVEL) {
        new_handle = new FineRecordHandle(table_id_, record_key, txn_schema_);
    }
    ret = RecordHandleWrapper(table_id_, record_key, new_handle, cc_level_);

    // Re-insert and check if another transaction inserted the record
    found = false;
    index_.insert_or_visit({record_key, ret}, [&](const auto& v) {
        ret = v.second;
        found = true;
    });

    // If insertion fails, delete this local record
    if (found) {
        delete new_handle;
    }
    return ret;
}