#include <infiniband/verbs_exp.h>

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <string>

#include "Generator.h"
#include "TATP/TATPBenchmark.h"
#include "TATP/TATPConstant.h"
#include "common/Config.h"
#include "common/Type.h"
#include "db/ColumnInfo.h"
#include "db/Db.h"
#include "db/DbRecord.h"
#include "db/Table.h"
#include "db/ValueType.h"
#include "mempool/BufferManager.h"
#include "util/Logger.h"
#include "util/Macros.h"
#include "util/Timer.h"

using namespace tatp;

Db* TATPBenchmark::CreateDB(const TATPConfig& config) {
    Db* db = new Db(tatp::TATP_TABLE_NUM);

    // Create Subscriber Table
    {
        SchemaArg subscriber_schema = {
            {S_SUBNUM, "s_sub_number", kVarChar, S_SUBNUM_SIZE, false,
             offsetof(subscriber_value_t, sub_number)},
            {S_HEX, "s_hex", kVarChar, S_HEX_NUM, false, offsetof(subscriber_value_t, hex)},
            {S_BYTES, "s_bytes", kVarChar, S_BYTES_NUM, false, offsetof(subscriber_value_t, bytes)},
            {S_BITS, "s_bits", kUInt32, 0, false, offsetof(subscriber_value_t, bits)},
            {S_MSC_LOCATION, "s_msc_location", kUInt32, 0, false,
             offsetof(subscriber_value_t, msc_location)},
            {S_VLR_LOCATION, "s_vlr_location", kUInt32, 0, false,
             offsetof(subscriber_value_t, vlr_location)},
        };

        TableCreateAttr subscriber_table_attr =
            TableCreateAttr{/* table_id     = */ tatp::SUBSCRIBER_TABLE,
                            /* name         = */ "subscriber",
                            /* schema_arg   = */ subscriber_schema,
                            /* index_config = */ config.subscriber_index_conf,
                            /* cc_level     = */ TableCCLevel::CELL_LEVEL};
        db->CreateTable(subscriber_table_attr);
    }

    // Create Secondary Subscriber Table
    {
        SchemaArg secondary_subscriber_schema = {
            {SEC_SUB_SID, "sec_sub_sid", kUInt32, 0, false,
             offsetof(secondary_subscriber_value_t, sid)},
        };
        TableCreateAttr secondary_subscriber_table_attr =
            TableCreateAttr{/* table_id     = */ tatp::SECONDARY_SUBSCRIBER_TABLE,
                            /* name         = */ "secondary_subscriber",
                            /* schema_arg   = */ secondary_subscriber_schema,
                            /* index_config = */ config.secondary_subscriber_index_conf,
                            /* cc_level     = */ TableCCLevel::CELL_LEVEL};
        db->CreateTable(secondary_subscriber_table_attr);
    }

    {
        SchemaArg accessinfo_schema = {
            {AI_DATA1, "ai_data1", kVarChar, 1, false, offsetof(accessinf_val_t, data1)},
            {AI_DATA2, "ai_data2", kVarChar, 1, false, offsetof(accessinf_val_t, data2)},
            {AI_DATA3, "ai_data3", kVarChar, 3, false, offsetof(accessinf_val_t, data3)},
            {AI_DATA4, "ai_data4", kVarChar, 5, false, offsetof(accessinf_val_t, data4)},
        };
        TableCreateAttr accessinfo_table_attr =
            TableCreateAttr{/* table_id     = */ tatp::ACCESSINFO_TABLE,
                            /* name         = */ "access_info",
                            /* schema_arg   = */ accessinfo_schema,
                            /* index_config = */ config.access_info_index_conf,
                            /* cc_level     = */ TableCCLevel::RECORD_LEVEL};
        db->CreateTable(accessinfo_table_attr);
    }

    {
        SchemaArg special_facility_schema = {
            {SF_IS_ACTIVE, "sf_is_active", kVarChar, 1, false, offsetof(specfac_val_t, is_active)},
            {SF_ERROR_CNTL, "sf_error_cntl", kVarChar, 1, false,
             offsetof(specfac_val_t, error_cntl)},
            {SF_DATA_A, "sf_data_a", kVarChar, 1, false, offsetof(specfac_val_t, data_a)},
            {SF_DATA_B, "sf_data_b", kVarChar, 5, false, offsetof(specfac_val_t, data_b)},
        };
        TableCreateAttr special_facility_table_attr =
            TableCreateAttr{/* table_id     = */ tatp::SPECIAL_FACILITY_TABLE,
                            /* name         = */ "special_facility",
                            /* schema_arg   = */ special_facility_schema,
                            /* index_config = */ config.special_facility_index_conf,
                            /* cc_level     = */ TableCCLevel::RECORD_LEVEL};
        db->CreateTable(special_facility_table_attr);
    }

    {
        SchemaArg call_forwarding_schema = {
            {CF_END_TIME, "cf_end_time", kUInt32, 0, false, offsetof(callfwd_val_t, end_time)},
            {CF_NUMBERX, "cf_numberx", kVarChar, 15, false, offsetof(callfwd_val_t, numberx)},
        };
        TableCreateAttr call_forwarding_table_attr =
            TableCreateAttr{/* table_id     = */ tatp::CALL_FORWARDING_TABLE,
                            /* name         = */ "call_forwarding",
                            /* schema_arg   = */ call_forwarding_schema,
                            /* index_config = */ config.call_forwarding_index_conf,
                            /* cc_level     = */ TableCCLevel::RECORD_LEVEL};
        db->CreateTable(call_forwarding_table_attr);
    }
    return db;
}

bool TATPBenchmark::PopulateDatabaseRecords(BufferManager* bm, Db* db, bool d) {
    util::Timer timer;

    PopulateSubscriberRecords(bm, db, d);
    PopulateSecondarySubscriberRecords(bm, db, d);
    PopulateAccessInfoRecords(bm, db, d);
    Populate_SpecialFacility_And_CallForwardingRecords(bm, db, d);

    LOG_INFO("Populate database records: %.2lf ms", timer.ms_elapse());
    LOG_INFO("Primary Table Num: %d", db->GetPrimaryTableNum());
    LOG_INFO("Backup Table Num: %d", db->GetBackupTableNum());
    return true;
}

bool TATPBenchmark::PopulateSubscriberRecords(BufferManager* bm, Db* db, bool deterministic) {
    uint64_t tmp_seed = 0xdeadbeef; /* Temporary seed for this function only */
    FastRandom random_generator(tmp_seed);
    DbTable* subscriber_table = db->GetTable(tatp::SUBSCRIBER_TABLE);
    char* local_addr = bm->AllocAlign(subscriber_table->GetTableSize(), CACHE_LINE_SIZE);
    if (unlikely(!local_addr || (uint64_t)local_addr % CACHE_LINE_SIZE)) {
        LOG_FATAL("No enought memory for SubscriberTable");
        return false;
    }
    node_id_t nid = config_.node_init_attr.nid;
    subscriber_table->SetTableLocalPtr(local_addr);
    subscriber_table->SetTablePoolPtr(MakePoolPtr(nid, (uint64_t)local_addr));

    tatp::subscriber_value_t s_val;
    DbRecord s_record((char*)&s_val, sizeof(tatp::subscriber_value_t),
                      subscriber_table->GetRecordSchema());

    int insert_record = 0, fail_insert = 0;
    for (uint32_t s_id = 0; s_id < tatp_config_.num_subscriber; s_id++) {
        RecordKey s_key = static_cast<RecordKey>(s_id);
        subscriber_value_t sub_val;
        sub_val.sub_number = SimpleGetSubscribeNumFromSubscribeID(s_id);

        if (deterministic) {
            // Create the record value in a deterministic way
            for (int i = 0; i < S_HEX_NUM; ++i) {
                sub_val.hex[i] = (char)i;
            }
            for (int i = 0; i < S_BYTES_NUM; ++i) {
                sub_val.bytes[i] = (char)i;
            }
            sub_val.bits = S_BITS_VALUE_DEFAULT;
            sub_val.vlr_location = VLR_LOCATION_DEFAULT_VALUE;
        } else {
            for (int i = 0; i < 5; i++) {
                sub_val.hex[i] = random_generator.NextChar();
            }
            for (int i = 0; i < 10; i++) {
                sub_val.bytes[i] = random_generator.NextChar();
            }
            sub_val.bits = random_generator.NextU16();
            sub_val.vlr_location = random_generator.NextU32();
        }

        sub_val.msc_location = tatp::MSC_LOCATION_DEFAULT_VALUE;

        bool s = subscriber_table->InsertRecordLocal(s_key, s_record, INIT_INSERT_VERSION);
        fail_insert += !s;
        insert_record++;
    }
    std::string flag = "";
    if (tatp::SUBSCRIBER_TABLE % config_.node_init_attr.num_mns == nid) {
        subscriber_table->SetAsPrimary();
        db->AddPrimaryTableNum();
        flag = "Primary";
    } else {
        subscriber_table->SetAsBackup();
        db->AddBackupTableNum();
        flag = "Backup ";
    }
    LOG_INFO(
        "[%s] [%s Table] addr: %#llx, record_size: %4lu, insert: %8d, failed: %8d, consumes: %.2lf "
        "MiB",
        flag.c_str(), tatp::subscriber_table_name, subscriber_table->TableLocalPtr(),
        subscriber_table->GetRecordMemSize(), insert_record, fail_insert,
        subscriber_table->GetTableSize() / (double)(1 << 20));
    return true;
}

bool TATPBenchmark::PopulateSecondarySubscriberRecords(BufferManager* bm, Db* db,
                                                       bool deterministic) {
    uint64_t tmp_seed = 0xdeadbeef; /* Temporary seed for this function only */
    FastRandom random_generator(tmp_seed);
    DbTable* sec_subscriber_table = db->GetTable(tatp::SECONDARY_SUBSCRIBER_TABLE);
    char* local_addr = bm->AllocAlign(sec_subscriber_table->GetTableSize(), CACHE_LINE_SIZE);
    if (unlikely(!local_addr || (uint64_t)local_addr % CACHE_LINE_SIZE)) {
        LOG_FATAL("No enought memory for Secondary SubscriberTable");
        return false;
    }
    node_id_t nid = config_.node_init_attr.nid;
    sec_subscriber_table->SetTableLocalPtr(local_addr);
    sec_subscriber_table->SetTablePoolPtr(MakePoolPtr(nid, (uint64_t)local_addr));

    tatp::secondary_subscriber_value_t sec_s_val;
    DbRecord sec_s_record((char*)&sec_s_val, sizeof(tatp::secondary_subscriber_value_t),
                          sec_subscriber_table->GetRecordSchema());

    int insert_record = 0, fail_insert = 0;
    for (uint32_t s_id = 0; s_id < tatp_config_.num_subscriber; s_id++) {
        tatp_sub_number sub_number = SimpleGetSubscribeNumFromSubscribeID(s_id);
        RecordKey record_key = sub_number.key;
        sec_s_val.sid = s_id;
        bool s =
            sec_subscriber_table->InsertRecordLocal(record_key, sec_s_record, INIT_INSERT_VERSION);
        fail_insert += !s;
        insert_record++;
    }
    // Set the Property of this table:
    std::string flag = "";
    if (tatp::SECONDARY_SUBSCRIBER_TABLE % config_.node_init_attr.num_mns == nid) {
        sec_subscriber_table->SetAsPrimary();
        db->AddPrimaryTableNum();
        flag = "Primary";
    } else {
        sec_subscriber_table->SetAsBackup();
        db->AddBackupTableNum();
        flag = "Backup ";
    }
    LOG_INFO(
        "[%s] [%s Table] addr: %#llx, record_size: %4lu, insert: %8d, failed: %8d, consumes: %.2lf "
        "MiB",
        flag.c_str(), tatp::sec_subscriber_table_name, sec_subscriber_table->TableLocalPtr(),
        sec_subscriber_table->GetRecordMemSize(), insert_record, fail_insert,
        sec_subscriber_table->GetTableSize() / (double)(1 << 20));

    return true;
}

bool TATPBenchmark::PopulateAccessInfoRecords(BufferManager* buf_manager, Db* db, bool d) {
    std::vector<uint8_t> ai_type_values = {0, 1, 2, 3};
    DbTable* accessinfo_table = db->GetTable(tatp::ACCESSINFO_TABLE);
    char* local_addr = buf_manager->AllocAlign(accessinfo_table->GetTableSize(), CACHE_LINE_SIZE);
    if (unlikely(!local_addr || (uint64_t)local_addr % CACHE_LINE_SIZE)) {
        LOG_FATAL("No enough memory for AccessInfoTable");
        return false;
    }
    node_id_t nid = config_.node_init_attr.nid;
    accessinfo_table->SetTableLocalPtr(local_addr);
    accessinfo_table->SetTablePoolPtr(MakePoolPtr(nid, (uint64_t)local_addr));

    tatp::accessinf_val_t ai_val;
    DbRecord ai_record((char*)&ai_val, sizeof(tatp::accessinf_val_t),
                       accessinfo_table->GetRecordSchema());
    int insert_record = 0, fail_insert = 0;
    /* Populate the table */
    for (uint32_t s_id = 0; s_id < tatp_config_.num_subscriber; s_id++) {
        // std::vector<uint8_t> ai_type_vec = SelectUniqueItem(&tmp_seed, ai_type_values, 1, 4);
        std::vector<uint8_t> ai_type_vec = ai_type_values;
        for (uint8_t ai_type : ai_type_vec) {
            // Populate with all AI types
            RecordKey acc_key = tatp_ctx_->MakeAccessInfoKey(s_id, ai_type);
            // Randomly generate some value
            ai_val.data1 = '1';
            ai_val.data2 = '2';
            ai_val.data3[0] = '3';
            ai_val.data4[0] = '4';

            bool s = accessinfo_table->InsertRecordLocal(acc_key, ai_record, INIT_INSERT_VERSION);
            fail_insert += !s;
            insert_record++;
        }
    }
    // Set the Property of this table:
    std::string flag = "";
    if (tatp::ACCESSINFO_TABLE % config_.node_init_attr.num_mns == nid) {
        accessinfo_table->SetAsPrimary();
        db->AddPrimaryTableNum();
        flag = "Primary";
    } else {
        accessinfo_table->SetAsBackup();
        db->AddBackupTableNum();
        flag = "Backup ";
    }
    LOG_INFO(
        "[%s] [%s Table] addr: %#llx, record_size: %4lu, insert: %8d, failed: %8d, consumes: %.2lf "
        "MiB",
        flag.c_str(), tatp::accessinfo_table_name, accessinfo_table->TableLocalPtr(),
        accessinfo_table->GetRecordMemSize(), insert_record, fail_insert,
        accessinfo_table->GetTableSize() / (double)(1 << 20));

    return true;
}

bool TATPBenchmark::Populate_SpecialFacility_And_CallForwardingRecords(BufferManager* buf_manager,
                                                                       Db* db, bool d) {
    const std::vector<uint8_t> sf_type_values = {0, 1, 2, 3};
    const std::vector<uint8_t> start_time_values = {0, 8, 16};
    FastRandom random_generator(0xdeadbeef);
    FastRandom data_random(0x12345678);

    DbTable* specfac_table = db->GetTable(tatp::SPECIAL_FACILITY_TABLE);
    DbTable* callfwd_table = db->GetTable(tatp::CALL_FORWARDING_TABLE);

    // Allocate memory for SpecialFacilityTable
    char* local_addr = buf_manager->AllocAlign(specfac_table->GetTableSize(), CACHE_LINE_SIZE);
    if (unlikely(!local_addr || (uint64_t)local_addr % CACHE_LINE_SIZE)) {
        LOG_FATAL("No enough memory for SpecialFacilityTable");
        return false;
    }
    node_id_t nid = config_.node_init_attr.nid;
    specfac_table->SetTableLocalPtr(local_addr);
    specfac_table->SetTablePoolPtr(MakePoolPtr(nid, (uint64_t)local_addr));

    // Allocate memory for CallForwardingTable
    local_addr = buf_manager->AllocAlign(callfwd_table->GetTableSize(), CACHE_LINE_SIZE);
    if (unlikely(!local_addr || (uint64_t)local_addr % CACHE_LINE_SIZE)) {
        LOG_FATAL("No enough memory for CallForwardingTable");
        return false;
    }
    callfwd_table->SetTableLocalPtr(local_addr);
    callfwd_table->SetTablePoolPtr(MakePoolPtr(nid, (uint64_t)local_addr));

    tatp::specfac_val_t sf_val;
    DbRecord sf_record((char*)&sf_val, sizeof(tatp::specfac_val_t),
                       specfac_table->GetRecordSchema());

    tatp::callfwd_val_t cf_val;
    DbRecord cf_record((char*)&cf_val, sizeof(tatp::callfwd_val_t),
                       callfwd_table->GetRecordSchema());
    int sf_insert_record = 0, sf_fail_insert = 0;
    int callfwd_insert_record = 0, callfwd_fail_insert = 0;
    // For each Subscriber
    for (uint32_t s_id = 0; s_id < tatp_config_.num_subscriber; ++s_id) {
        // For each SpecialFacility
        for (uint8_t sf_type : sf_type_values) {
            RecordKey sf_key = tatp_ctx_->MakeSpecialFacilityKey(s_id, sf_type);
            sf_val.is_active = random_generator.Next() % 100 < 100 ? 1 : 0;
            // sf_val.error_cntl = random_generator.NextChar();
            sf_val.error_cntl = data_random.NextChar();
            sf_val.data_a = data_random.NextChar();
            sf_val.data_b[0] = data_random.NextChar();
            bool s = specfac_table->InsertRecordLocal(sf_key, sf_record, INIT_INSERT_VERSION);
            sf_fail_insert += !s;
            sf_insert_record++;

            // For each CallForwarding
            for (size_t start_time = 0; start_time <= 16; start_time += 8) {
                RecordKey cf_key = tatp_ctx_->MakeCallForwardingKey(s_id, sf_type, start_time);
                cf_val.numberx[0] = data_random.NextChar();
                cf_val.end_time = (random_generator.NextU32() % 24) + 1;
                s = callfwd_table->InsertRecordLocal(cf_key, cf_record, INIT_INSERT_VERSION);
                callfwd_fail_insert += !s;
                callfwd_insert_record++;
            }
        }
    }
    std::string sf_flag = "";
    std::string callfwd_flag = "";
    if (tatp::SPECIAL_FACILITY_TABLE % config_.node_init_attr.num_mns == nid) {
        specfac_table->SetAsPrimary();
        db->AddPrimaryTableNum();
        sf_flag = "Primary";
    } else {
        specfac_table->SetAsBackup();
        db->AddBackupTableNum();
        sf_flag = "Backup ";
    }
    if (tatp::CALL_FORWARDING_TABLE % config_.node_init_attr.num_mns == nid) {
        callfwd_table->SetAsPrimary();
        db->AddPrimaryTableNum();
        callfwd_flag = "Primary";
    } else {
        callfwd_table->SetAsBackup();
        db->AddBackupTableNum();
        callfwd_flag = "Backup ";
    }

    LOG_INFO(
        "[%s] [%s Table] addr: %#llx, record_size: %4lu, insert: %8d, failed: %8d, consumes: %.2lf "
        "MiB",
        sf_flag.c_str(), tatp::specialfacility_table_name, specfac_table->TableLocalPtr(),
        specfac_table->GetRecordMemSize(), sf_insert_record, sf_fail_insert,
        specfac_table->GetTableSize() / (double)(1 << 20));

    LOG_INFO(
        "[%s] [%s Table] addr: %#llx, record_size: %4lu, insert: %8d, failed: %8d, consumes: %.2lf "
        "MiB",
        callfwd_flag.c_str(), tatp::callforwarding_table_name, callfwd_table->TableLocalPtr(),
        callfwd_table->GetRecordMemSize(), callfwd_insert_record, callfwd_fail_insert,
        callfwd_table->GetTableSize() / (double)(1 << 20));
    return true;
}