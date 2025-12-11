#pragma once

#include <string>
#include <vector>

#include "common/Type.h"
#include "db/ColumnInfo.h"
#include "db/Schema.h"
#include "db/Table.h"

// Db is the context of all tables and the transaction execution engine
class Db {
 public:
  Db() : tables_(), primary_table_num(0), backup_table_num(0) {}

  Db(const Db &) = delete;
  Db &operator=(const Db &) = delete;

  Db(const std::vector<DbTablePtr> &tables)
      : tables_(tables), primary_table_num(0), backup_table_num(0) {}

  Db(size_t sz) : tables_(sz, nullptr), primary_table_num(0), backup_table_num(0) {}

  DbTablePtr GetTable(TableId tbl_id) { return tables_[tbl_id]; }

  void AddTable(TableId table_id, DbTablePtr tbl) { tables_[table_id] = tbl; }

  void CreateTable(const TableCreateAttr &create_attr) {
    tables_[create_attr.table_id] = new DbTable(create_attr);
  }

  int GetTableNumber() const { return tables_.size(); }

  void AddPrimaryTableNum() { primary_table_num++; }

  void AddBackupTableNum() { backup_table_num++; }

  int GetPrimaryTableNum() const { return primary_table_num; }

  int GetBackupTableNum() const { return backup_table_num; }

 private:
  std::vector<DbTablePtr> tables_;
  int primary_table_num;
  int backup_table_num;
};

using DbPtr = Db *;