#include <cstring>
#include <string>

#include "Generator.h"
#include "YCSB/YCSBBenchmark.h"
#include "YCSB/YCSBConstant.h"
#include "YCSB/YCSBContext.h"
#include "YCSB/YCSBTableStructs.h"
#include "common/Type.h"
#include "db/ColumnInfo.h"
#include "db/Table.h"
#include "db/ValueType.h"
#include "mempool/BufferManager.h"
#include "util/Logger.h"
#include "util/Macros.h"
#include "util/Timer.h"

Db* YCSBBenchmark::CreateDB(const YCSBConfig& config) {
    Db* db = new Db(ycsb::YCSB_TABLE_NUM);
    {
        SchemaArg ycsb_schema;
        for (size_t i = 0; i < ycsb::YCSB_COLUMN_NUM; ++i) {
            ycsb_schema.emplace_back(
                ColumnArg{/* ColumnId  = */ (ColumnId)i,
                          /* name      = */ "column" + std::to_string(i),
                          /* data type = */ kVarChar,
                          /* varsize   = */ ycsb::YCSB_COLUMN_SIZE,
                          /* primary   = */ false,
                          /* offset    = */ (int)(ycsb::YCSB_COLUMN_SIZE * i)});
        }
        TableCreateAttr ycsb_table0_attr =
            TableCreateAttr{/* table_id     = */ ycsb::YCSB_TABLE0,
                            /* name         = */ "ycsb",
                            /* schema_arg   = */ ycsb_schema,
                            /* index_config = */ config.ycsb_index_conf,
                            /* cc_level     = */ TableCCLevel::CELL_LEVEL};
        db->CreateTable(ycsb_table0_attr);

        TableCreateAttr ycsb_table1_attr =
            TableCreateAttr{/* table_id     = */ ycsb::YCSB_TABLE1,
                            /* name         = */ "ycsb",
                            /* schema_arg   = */ ycsb_schema,
                            /* index_config = */ config.ycsb_index_conf,
                            /* cc_level     = */ TableCCLevel::CELL_LEVEL};
        db->CreateTable(ycsb_table1_attr);
    }
    return db;
}

bool YCSBBenchmark::PopulateDatabaseRecords(BufferManager* buf_manager, Db* db, bool d) {
    util::Timer timer;

    PopulateYCSBRecords(buf_manager, db, 0, d);
    PopulateYCSBRecords(buf_manager, db, 1, d);

    LOG_INFO("Populate database records: %.2lf ms", timer.ms_elapse());
    return true;
}

bool YCSBBenchmark::PopulateYCSBRecords(BufferManager* buf_manager, Db* db, int table_id, bool d) {
    FastRandom random_generator(0x19289190);
    DbTable* ycsb_table = nullptr;
    if (table_id == 0) {
        ycsb_table = db->GetTable(ycsb::YCSB_TABLE0);
    } else if (table_id == 1) {
        ycsb_table = db->GetTable(ycsb::YCSB_TABLE1);
    } else {
        LOG_FATAL("Invalid table_id: %d", table_id);
        return false;
    }
    char* local_addr = buf_manager->AllocAlign(ycsb_table->GetTableSize(), CACHE_LINE_SIZE);

    if (unlikely(!local_addr) || (uint64_t)local_addr % CACHE_LINE_SIZE) {
        LOG_FATAL("No enough memory for YCSB Table");
        return false;
    }
    node_id_t nid = config_.node_init_attr.nid;
    ycsb_table->SetTableLocalPtr(local_addr);
    ycsb_table->SetTablePoolPtr(MakePoolPtr(nid, (uint64_t)local_addr));

    ycsb::ycsb_value_t ycsb_val;
    DbRecord ycsb_record((char*)&ycsb_val, sizeof(ycsb::ycsb_value_t),
                         ycsb_table->GetRecordSchema());
    int insert_record = 0, fail_insert = 0;
    for (uint32_t id = 0; id < ycsb_config_.num_records; ++id) {
        RecordKey row_key = static_cast<RecordKey>(id);
        // The value is filled in with some data
        for (ColumnId cid = 0; cid < ycsb::YCSB_COLUMN_NUM; ++cid) {
            std::string val = "key: " + std::to_string(row_key) + "col: " + std::to_string(cid);
            size_t sz = std::min(val.size(), ycsb::YCSB_COLUMN_SIZE - 1);
            val = val.substr(0, sz);
            strcpy(ycsb_val.data[cid], val.c_str());
        }

        if (row_key % 2 == table_id) {
            bool s = ycsb_table->InsertRecordLocal(row_key, ycsb_record, INIT_INSERT_VERSION);
            insert_record++;
            fail_insert += !s;
        }
    }
    // Set the Property of this table:
    std::string flag = "";
    if (table_id % config_.node_init_attr.num_mns == nid) {
        ycsb_table->SetAsPrimary();
        db->AddPrimaryTableNum();
        flag = "Primary";
    } else {
        ycsb_table->SetAsBackup();
        db->AddBackupTableNum();
        flag = "Backup ";
    }
    std::string table_name = "YCSB" + std::to_string(table_id);
    LOG_INFO(
        "[%s] [%s Table] addr: %#llx, record_size: %4lu, insert: %8d, failed: %8d, consumes: %.2lf "
        "MiB",
        flag.c_str(), table_name.c_str(), ycsb_table->TableLocalPtr(),
        ycsb_table->GetRecordMemSize(), insert_record, fail_insert,
        ycsb_table->GetTableSize() / (double)(1 << 20));
    return true;
}