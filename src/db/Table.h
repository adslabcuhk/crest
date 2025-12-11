#pragma once

#include <cstdint>
#include <functional>

#include "common/Type.h"
#include "db/AddressCache.h"
#include "db/DbRecord.h"
#include "db/Format.h"
#include "db/PoolHashIndex.h"
#include "db/Schema.h"
#include "util/Hash.h"

enum class TableType {
  NONE = 0,
  PRIMARY,
  BACKUP,
};

struct TableCreateAttr {
  TableId table_id;                    // Unique identifier of this table
  std::string name;                    // Name of this table
  SchemaArg schema_arg;                // Schema arguments of this Table
  PoolHashIndex::Config index_config;  // Hash index configuration of this table
  TableCCLevel cc_level;               // Are transactions gonna to access this table
                                       // in record or cell level?
};

struct TableAccessAttr {
  char *local_addr = nullptr;         // Only if this table is created locally
  PoolPtr pool_addr = POOL_PTR_NULL;  // The table's start location in the pool
  TableType table_type = TableType::NONE;
};

class DbTable {
 public:
  DbTable(const TableCreateAttr &create_attr, const TableAccessAttr &access_attr)
      : create_attr_(create_attr),
        access_attr_(access_attr),
        primary_index_(create_attr.index_config),
        txn_schema_(create_attr.schema_arg),
        record_schema_(create_attr.schema_arg),
        address_cache_(),
        cc_level_(create_attr.cc_level)
  // slot_cache_(create_attr.index_config)
  {
    txn_schema_.SetTableCCLevel(cc_level_);
  }

  DbTable(const TableCreateAttr &create_attr)
      : create_attr_(create_attr),
        access_attr_(),
        primary_index_(create_attr.index_config),
        txn_schema_(create_attr.schema_arg),
        record_schema_(create_attr.schema_arg),
        address_cache_(),
        cc_level_(create_attr.cc_level)
  // slot_cache_(create_attr.index_config)
  {
    txn_schema_.SetTableCCLevel(cc_level_);
  }

  DbTable() = delete;
  DbTable(const DbTable &) = delete;
  DbTable &operator=(const DbTable &) = delete;

 public:
  void SetAsPrimary() { access_attr_.table_type = TableType::PRIMARY; }

  void SetAsBackup() { access_attr_.table_type = TableType::BACKUP; }

  TableId GetTableId() const { return create_attr_.table_id; }

  PoolPtr TablePoolPtr() const { return access_attr_.pool_addr; }
  void SetTablePoolPtr(PoolPtr ptr) { access_attr_.pool_addr = ptr; }

  PoolPtr PrimaryTablePoolPtr() const { return primary_addr_; }
  void SetPrimaryTablePoolPtr(PoolPtr ptr) { primary_addr_ = ptr; }

  std::vector<PoolPtr> BackupTablePoolPtr() const { return backup_addrs_; }
  void AddBackupTablePoolPtr(PoolPtr ptr) { backup_addrs_.push_back(ptr); }

  char *TableLocalPtr() const { return access_attr_.local_addr; }
  void SetTableLocalPtr(char *addr) { access_attr_.local_addr = addr; }

  uint64_t GetBucketSize() const {
    return txn_schema_.GetRecordMemSize() * primary_index_.config_.records_per_bucket_;
  }

  size_t GetRecordMemSize() const { return txn_schema_.GetRecordMemSize(); }

  size_t GetRecordNumPerBucket() const { return primary_index_.config_.records_per_bucket_; }

  size_t GetTableSize() const { return GetBucketSize() * primary_index_.config_.bucket_num_; }

  PoolPtr LocateBucketInPool(RecordKey key) {
    return TablePoolPtr() + primary_index_.LocateBucketId(key) * GetBucketSize();
  }

  char *LocateBucketLocal(RecordKey key) {
    return TableLocalPtr() + primary_index_.LocateBucketId(key) * GetBucketSize();
  }

  TxnSchema *GetTxnSchema() { return &txn_schema_; }

  RecordValueSchema *GetRecordSchema() { return &record_schema_; }

  // Locate the address of specied record key. The returned value is the
  // pool address of the record if the found flag is set to be true; Otherwise
  // it's the address of the bucket (may be) containing the record
  PoolPtr GetRecordAddress(RecordKey record_key, bool &found) {
    PoolPtr ret = POOL_PTR_NULL;
    if (ret = address_cache_.Search(record_key), ret != POOL_PTR_NULL) {
      found = true;
      return ret;
    }
    return LocateBucketInPool(record_key);
  }

  PoolPtr GetCachedAddress(RecordKey record_key) {
    PoolPtr ret = POOL_PTR_NULL;
    if (ret = address_cache_.Search(record_key), ret != POOL_PTR_NULL) {
      return ret;
    }
    return POOL_PTR_NULL;
  }

  // Always returns the address of the primary table
  PoolPtr GetBucketAddress(RecordKey record_key) {
    return LocateBucketInPool(record_key);
  }

  void UpdateRecordAddress(RecordKey record_key, PoolPtr new_addr) {
    address_cache_.Insert(record_key, new_addr);
  }

  void InvalidateAddressCache(RecordKey record_key) { address_cache_.Delete(record_key); }

 public:
  // Insert a DbRecord into the database locally, following the database
  // internal record format. Return false if insertion fails, return true
  // otherwise
  bool InsertRecordLocal(RecordKey rkey, const DbRecord &record, version_t v);

  // Update a database record using the DbRecord, following the database's internal
  // record format. Return false if the update fails, i.e., the target record does
  // not exist
  bool UpdateRecordLocal(RecordKey rkey, const DbRecord &record, version_t v);

  // Search a record with specified row key
  bool SearchRecordLocal(RecordKey rkey, DbRecord *record);

  // Delete a record with specified row key and return the value of this
  // record
  bool DeleteRecordLocal(RecordKey rkey, DbRecord *record);

  // RowHeader *GetRow(char *bkt, size_t r_id) {
  //   return (RowHeader *)(bkt + r_id * txn_schema_.GetRecordMemSize());
  // }

  RecordHeader *GetRecord(char *bkt, size_t r_id) {
    return (RecordHeader *)(bkt + r_id * txn_schema_.GetRecordMemSize());
  }

  char *SerializeTableInfo(char *dst) const;
  const char *DeserializeTableInfo(const char *src);

  std::string DataString(RecordKey record_key);

 private:
  // Helper function
  // ColumnHeader *GetColumn(RowHeader *row, ColumnId cid) {
  //   return (ColumnHeader *)((char *)row + txn_schema_.ColumnOffset(cid));
  // }

  ColumnHeader *GetColumn(RecordHeader *record, ColumnId cid) {
    return (ColumnHeader *)((char *)record + txn_schema_.ColumnOffset(cid));
  }

 private:
  TableCreateAttr create_attr_;
  TableAccessAttr access_attr_;

  TxnSchema txn_schema_;
  RecordValueSchema record_schema_;

  PoolHashIndex primary_index_;  // Always use hash index as the primary index
  AddressCache address_cache_;   // Address Cache
  TableCCLevel cc_level_;        // How transactions access this table
  // BucketSlotCache slot_cache_;   // The empty slot cache

  // CN Side access information:
  PoolPtr primary_addr_;
  std::vector<PoolPtr> backup_addrs_;
};

using DbTablePtr = DbTable *;