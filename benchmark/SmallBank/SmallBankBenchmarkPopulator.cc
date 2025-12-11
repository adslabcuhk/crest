#include <cstring>
#include <string>

#include "SmallBank/SmallBankBenchmark.h"
#include "SmallBank/SmallBankConstant.h"
#include "SmallBank/SmallBankContext.h"
#include "common/Type.h"
#include "db/ColumnInfo.h"
#include "db/ValueType.h"
#include "mempool/BufferManager.h"

Db* SmallBankBenchmark::CreateDB(const SmallBankConfig& config) {
    Db* db = new Db(smallbank::SMALLBANK_TABLE_NUM);

    {
        SchemaArg accounts_schema = {
            {smallbank::ACCOUNTS_CUST_ID, "cust_id", kInt64, 0, false,
             offsetof(smallbank::accounts_value_t, cust_id)},
            {smallbank::ACCOUNTS_NAME, "name", kVarChar, smallbank::ACCOUNTS_MAX_NAME_SIZE + 1,
             false, offsetof(smallbank::accounts_value_t, name)},
        };
        TableCreateAttr accounts_table_attr =
            TableCreateAttr{/* table_id     = */ smallbank::ACCOUNTS_TABLE,
                            /* name         = */ "accounts",
                            /* schema_arg   = */ accounts_schema,
                            /* index_config = */ config.accounts_index_conf,
                            /* cc_level     = */ TableCCLevel::RECORD_LEVEL};
        db->CreateTable(accounts_table_attr);
    }

    {
        SchemaArg savings_schema = {
            {smallbank::SAVINGS_CUST_ID, "cust_id", kInt64, 0, false,
             offsetof(smallbank::savings_value_t, cust_id)},
            {smallbank::SAVINGS_BALANCE, "bal", kDouble, 0, false,
             offsetof(smallbank::savings_value_t, bal)},
        };
        TableCreateAttr savings_table_attr =
            TableCreateAttr{/* table_id     = */ smallbank::SAVINGS_TABLE,
                            /* name         = */ "savings",
                            /* schema_arg   = */ savings_schema,
                            /* index_config = */ config.savings_index_conf,
                            /* cc_level     = */ TableCCLevel::RECORD_LEVEL};
        db->CreateTable(savings_table_attr);
    }

    {
        SchemaArg checkings_schema = {
            {smallbank::CHECKINGS_CUST_ID, "cust_id", kInt64, 0, false,
             offsetof(smallbank::checkings_value_t, cust_id)},
            {smallbank::CHECKINGS_BALANCE, "bal", kDouble, 0, false,
             offsetof(smallbank::checkings_value_t, bal)},
        };
        TableCreateAttr checkings_table_attr =
            TableCreateAttr{/* table_id     = */ smallbank::CHECKING_TABLE,
                            /* name         = */ "checkings",
                            /* schema_arg   = */ checkings_schema,
                            /* index_config = */ config.checkings_index_conf,
                            /* cc_level     = */ TableCCLevel::RECORD_LEVEL};
        db->CreateTable(checkings_table_attr);
    }
    return db;
}

bool SmallBankBenchmark::PopulateDatabaseRecords(BufferManager* buf_manager, Db* db, bool d) {
    util::Timer timer;

    PopulateAccountsTable(buf_manager, db, d);
    PopulateSavingsTable(buf_manager, db, d);
    PopulateCheckingsTable(buf_manager, db, d);

    LOG_INFO("Populate database records: %.2lf ms", timer.ms_elapse());
    return true;
}

bool SmallBankBenchmark::PopulateAccountsTable(BufferManager* bm, Db* db, bool d) {
    FastRandom random_generator(0x19289190);
    DbTable* accounts_table = db->GetTable(smallbank::ACCOUNTS_TABLE);
    char* local_addr = bm->AllocAlign(accounts_table->GetTableSize(), CACHE_LINE_SIZE);

    if (unlikely(!local_addr || (uint64_t)local_addr % CACHE_LINE_SIZE)) {
        LOG_FATAL("No enough memory for Accounts Table");
        return false;
    }
    node_id_t nid = config_.node_init_attr.nid;
    accounts_table->SetTableLocalPtr(local_addr);
    accounts_table->SetTablePoolPtr(MakePoolPtr(nid, (uint64_t)local_addr));

    smallbank::accounts_value_t a_val;
    DbRecord a_record((char*)&a_val, sizeof(smallbank::accounts_value_t),
                      accounts_table->GetRecordSchema());
    int insert_record = 0, fail_insert = 0;
    for (uint32_t id = 0; id < smallbank_config_.num_accounts; ++id) {
        RecordKey row_key = static_cast<RecordKey>(id);
        a_val.cust_id = id;
        std::string name = "ACCOUNTS_NAME" + std::to_string(id);
        strcpy(a_val.name, name.c_str());

        bool s = accounts_table->InsertRecordLocal(row_key, a_record, INIT_INSERT_VERSION);
        insert_record++;
        fail_insert += !s;
    }
    // Set the Property of this table:
    std::string flag = "";
    if (smallbank::ACCOUNTS_TABLE % config_.node_init_attr.num_mns == nid) {
        accounts_table->SetAsPrimary();
        db->AddPrimaryTableNum();
        flag = "Primary";
    } else {
        accounts_table->SetAsBackup();
        db->AddBackupTableNum();
        flag = "Backup ";
    }
    LOG_INFO(
        "[%s] [%s Table] addr: %#llx, record_size: %4lu, insert: %8d, failed: %8d, consumes: %.2lf "
        "MiB",
        flag.c_str(), smallbank::accounts_table_name, accounts_table->TableLocalPtr(),
        accounts_table->GetRecordMemSize(), insert_record, fail_insert,
        accounts_table->GetTableSize() / (double)(1 << 20));
    return true;
}

bool SmallBankBenchmark::PopulateSavingsTable(BufferManager* bm, Db* db, bool d) {
    FastRandom random_generator(0x19289190);
    DbTable* savings_table = db->GetTable(smallbank::SAVINGS_TABLE);
    char* local_addr = bm->AllocAlign(savings_table->GetTableSize(), CACHE_LINE_SIZE);

    if (unlikely(!local_addr || (uint64_t)local_addr % CACHE_LINE_SIZE)) {
        LOG_FATAL("No enough memory for Savings Table");
        return false;
    }
    node_id_t nid = config_.node_init_attr.nid;
    savings_table->SetTableLocalPtr(local_addr);
    savings_table->SetTablePoolPtr(MakePoolPtr(nid, (uint64_t)local_addr));

    smallbank::savings_value_t s_val;
    DbRecord s_record((char*)&s_val, sizeof(smallbank::savings_value_t),
                      savings_table->GetRecordSchema());
    int insert_record = 0, fail_insert = 0;
    for (uint32_t id = 0; id < smallbank_config_.num_accounts; ++id) {
        RecordKey row_key = static_cast<RecordKey>(id);
        s_val.cust_id = id;
        s_val.bal = 1000000000.00;

        bool s = savings_table->InsertRecordLocal(row_key, s_record, INIT_INSERT_VERSION);
        insert_record++;
        fail_insert += !s;
    }
    // Set the property of this table
    std::string flag = "";
    if (smallbank::SAVINGS_TABLE % config_.node_init_attr.num_mns == nid) {
        savings_table->SetAsPrimary();
        db->AddPrimaryTableNum();
        flag = "Primary";
    } else {
        savings_table->SetAsBackup();
        db->AddBackupTableNum();
        flag = "Backup ";
    }
    LOG_INFO(
        "[%s] [%s Table] addr: %#llx, record_size: %4lu, insert: %8d, failed: %8d, consumes: %.2lf "
        "MiB",
        flag.c_str(), smallbank::savings_table_name, savings_table->TableLocalPtr(),
        savings_table->GetRecordMemSize(), insert_record, fail_insert,
        savings_table->GetTableSize() / (double)(1 << 20));
    return true;
}

bool SmallBankBenchmark::PopulateCheckingsTable(BufferManager* bm, Db* db, bool d) {
    FastRandom random_generator(0x19289190);
    DbTable* checkings_table = db->GetTable(smallbank::CHECKING_TABLE);
    char* local_addr = bm->AllocAlign(checkings_table->GetTableSize(), CACHE_LINE_SIZE);

    if (unlikely(!local_addr || (uint64_t)local_addr % CACHE_LINE_SIZE)) {
        LOG_FATAL("No enough memory for Checkings Table");
        return false;
    }
    node_id_t nid = config_.node_init_attr.nid;
    checkings_table->SetTableLocalPtr(local_addr);
    checkings_table->SetTablePoolPtr(MakePoolPtr(nid, (uint64_t)local_addr));

    smallbank::checkings_value_t c_val;
    DbRecord c_record((char*)&c_val, sizeof(smallbank::checkings_value_t),
                      checkings_table->GetRecordSchema());
    int insert_record = 0, fail_insert = 0;
    for (uint32_t id = 0; id < smallbank_config_.num_accounts; ++id) {
        RecordKey row_key = static_cast<RecordKey>(id);
        c_val.cust_id = id;
        c_val.bal = 1000000000.00;

        bool s = checkings_table->InsertRecordLocal(row_key, c_record, INIT_INSERT_VERSION);
        insert_record++;
        fail_insert += !s;
    }
    // Set the property of this table
    std::string flag = "";
    if (smallbank::CHECKING_TABLE % config_.node_init_attr.num_mns == nid) {
        checkings_table->SetAsPrimary();
        db->AddPrimaryTableNum();
        flag = "Primary";
    } else {
        checkings_table->SetAsBackup();
        db->AddBackupTableNum();
        flag = "Backup ";
    }
    LOG_INFO(
        "[%s] [%s Table] addr: %#llx, record_size: %4lu, insert: %8d, failed: %8d, consumes: %.2lf "
        "MiB",
        flag.c_str(), smallbank::checkings_table_name, checkings_table->TableLocalPtr(),
        checkings_table->GetRecordMemSize(), insert_record, fail_insert,
        checkings_table->GetTableSize() / (double)(1 << 20));
    return true;
}
