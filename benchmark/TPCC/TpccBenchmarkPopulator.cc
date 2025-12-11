#include <cstddef>
#include <cstring>
#include <string>

#include "Generator.h"
#include "TPCC/TpccBenchmark.h"
#include "TPCC/TpccConstant.h"
#include "TPCC/TpccUtil.h"
#include "common/Config.h"
#include "common/Type.h"
#include "db/ColumnInfo.h"
#include "db/DbRecord.h"
#include "db/ValueType.h"
#include "mempool/BufferManager.h"
#include "util/Logger.h"
#include "util/Macros.h"
#include "util/Timer.h"

Db* TpccBenchmark::CreateDB(const TpccConfig& config) {
  // Telling DB that we are gonna to create #TPCC_TABLE_NUM tables
  Db* db = new Db(tpcc::TPCC_TABLE_NUM);

  // Create Warehouse Table
  {
    SchemaArg warehouse_schema = {
        {tpcc::W_TAX, "w_tax", kDouble, 0, false, offsetof(tpcc::warehouse_value_t, w_tax)},
        {tpcc::W_YTD, "w_ytd", kDouble, 0, false, offsetof(tpcc::warehouse_value_t, w_ytd)},
        {tpcc::W_NAME, "w_name", kVarChar, tpcc::WAREHOUSE_NAME_SIZE_MAX + 1, false,
         offsetof(tpcc::warehouse_value_t, w_name)},
        {tpcc::W_STREET_1, "w_street_1", kVarChar, tpcc::WAREHOUSE_STREET_SIZE_MAX + 1, false,
         offsetof(tpcc::warehouse_value_t, w_street_1)},
        {tpcc::W_STREET_2, "w_street_2", kVarChar, tpcc::WAREHOUSE_STREET_SIZE_MAX + 1, false,
         offsetof(tpcc::warehouse_value_t, w_street_2)},
        {tpcc::W_CITY, "w_city", kVarChar, tpcc::WAREHOUSE_CITY_SIZE_MAX + 1, false,
         offsetof(tpcc::warehouse_value_t, w_city)},
        {tpcc::W_STATE, "w_state", kVarChar, tpcc::WAREHOUSE_STATE_SIZE + 1, false,
         offsetof(tpcc::warehouse_value_t, w_state)},
        {tpcc::W_ZIP, "w_zip", kVarChar, tpcc::WAREHOUSE_ZIP_SIZE + 1, false,
         offsetof(tpcc::warehouse_value_t, w_zip)},
    };

    TableCreateAttr warehouse_table_attr =
        TableCreateAttr{/* table_id = */ tpcc::WAREHOUSE_TABLE,
                        /* name = */ "warehouse",
                        /* schema_arg = */ warehouse_schema,
                        /* index_config = */ config.warehouse_index_conf,
                        /* cc_level = */ TableCCLevel::CELL_LEVEL};
    db->CreateTable(warehouse_table_attr);
  }

  // Create District Table
  {
    SchemaArg district_schema = {
        {tpcc::D_TAX, "d_tax", kDouble, 0, false, offsetof(tpcc::district_value_t, d_tax)},
        {tpcc::D_YTD, "d_ytd", kDouble, 0, false, offsetof(tpcc::district_value_t, d_ytd)},
        {tpcc::D_NEXT_OID, "d_next_oid", kUInt64, 0, false,
         offsetof(tpcc::district_value_t, d_next_oid)},
        {tpcc::D_NAME, "d_name", kVarChar, tpcc::DISTRICT_NAME_SIZE_MAX + 1, false,
         offsetof(tpcc::district_value_t, d_name)},
        {tpcc::D_STREET_1, "d_street_1", kVarChar, tpcc::DISTRICT_STREET_SIZE_MAX + 1, false,
         offsetof(tpcc::district_value_t, d_street_1)},
        {tpcc::D_STREET_2, "d_street_2", kVarChar, tpcc::DISTRICT_STREET_SIZE_MAX + 1, false,
         offsetof(tpcc::district_value_t, d_street_2)},
        {tpcc::D_CITY, "d_city", kVarChar, tpcc::DISTRICT_CITY_SIZE_MAX + 1, false,
         offsetof(tpcc::district_value_t, d_city)},
        {tpcc::D_STATE, "d_state", kVarChar, tpcc::DISTRICT_STATE_SIZE + 1, false,
         offsetof(tpcc::district_value_t, d_state)},
        {tpcc::D_ZIP, "d_zip", kVarChar, tpcc::DISTRICT_ZIP_SIZE + 1, false,
         offsetof(tpcc::district_value_t, d_zip)},
    };

    TableCreateAttr district_table_attr =
        TableCreateAttr{/* table_id = */ tpcc::DISTRICT_TABLE,
                        /* name = */ "district",
                        /* schema_arg = */ district_schema,
                        /* index_config = */ config.district_index_conf,
                        /* cc_level = */ TableCCLevel::CELL_LEVEL};
    db->CreateTable(district_table_attr);
  }

  // Create Customer Table
  {
    SchemaArg customer_schema = {
        {tpcc::C_BALANCE, "c_balance", kDouble, 0, false,
         offsetof(tpcc::customer_value_t, c_balance)},
        {tpcc::C_YTD_PAYMENT, "c_ytd_payment", kDouble, 0, false,
         offsetof(tpcc::customer_value_t, c_ytd_payment)},
        {tpcc::C_PAYMENT_CNT, "c_payment_cnt", kUInt32, 0, false,
         offsetof(tpcc::customer_value_t, c_payment_cnt)},
        {tpcc::C_DATA, "c_data", kVarChar, tpcc::CUSTOMER_DATA_SIZE_MAX + 1, false,
         offsetof(tpcc::customer_value_t, c_data)},
        {tpcc::C_FIRST, "c_first", kVarChar, tpcc::CUSTOMER_FIRST_SIZE_MAX + 1, false,
         offsetof(tpcc::customer_value_t, c_first)},
        {tpcc::C_MIDDLE, "c_middle", kVarChar, tpcc::CUSTOMER_MIDDLE_SIZE + 1, false,
         offsetof(tpcc::customer_value_t, c_middle)},
        {tpcc::C_LAST, "c_last", kVarChar, tpcc::CUSTOMER_LAST_SIZE_MAX + 1, false,
         offsetof(tpcc::customer_value_t, c_last)},
        {tpcc::C_STREET_1, "c_street_1", kVarChar, tpcc::CUSTOMER_STREET_SIZE_MAX + 1, false,
         offsetof(tpcc::customer_value_t, c_street_1)},
        {tpcc::C_STREET_2, "c_street_2", kVarChar, tpcc::CUSTOMER_STREET_SIZE_MAX + 1, false,
         offsetof(tpcc::customer_value_t, c_street_2)},
        {tpcc::C_CITY, "c_city", kVarChar, tpcc::CUSTOMER_CITY_SIZE_MAX + 1, false,
         offsetof(tpcc::customer_value_t, c_city)},
        {tpcc::C_STATE, "c_state", kVarChar, tpcc::CUSTOMER_STATE_SIZE + 1, false,
         offsetof(tpcc::customer_value_t, c_state)},
        {tpcc::C_ZIP, "c_zip", kVarChar, tpcc::CUSTOMER_ZIP_SIZE + 1, false,
         offsetof(tpcc::customer_value_t, c_zip)},
        {tpcc::C_PHONE, "c_phone", kVarChar, tpcc::CUSTOMER_PHONE_SIZE + 1, false,
         offsetof(tpcc::customer_value_t, c_phone)},
        {tpcc::C_SINCE, "c_since", kUInt32, 0, false, offsetof(tpcc::customer_value_t, c_since)},
        {tpcc::C_CREDIT, "c_credit", kVarChar, tpcc::CUSTOMER_CREDIT_SIZE + 1, false,
         offsetof(tpcc::customer_value_t, c_credit)},
        {tpcc::C_CREDIT_LIMIT, "c_credit_limit", kDouble, 0, false,
         offsetof(tpcc::customer_value_t, c_credit_limit)},
        {tpcc::C_DISCOUNT, "c_discount", kDouble, 0, false,
         offsetof(tpcc::customer_value_t, c_discount)},
        {tpcc::C_DELIVERY_CNT, "c_delivery_cnt", kUInt32, 0, false,
         offsetof(tpcc::customer_value_t, c_delivery_cnt)},
    };
    TableCreateAttr customer_table_attr =
        TableCreateAttr{/* table_id = */ tpcc::CUSTOMER_TABLE,
                        /* name = */ "customer",
                        /* schema_arg = */ customer_schema,
                        /* index_config = */ config.customer_index_conf,
                        /* cc_level = */ TableCCLevel::CELL_LEVEL};
    db->CreateTable(customer_table_attr);
  }

  // Create the History table
  {
    SchemaArg history_schema = {
        {tpcc::H_AMOUNT, "h_amount", kDouble, 0, false, offsetof(tpcc::history_value_t, h_amount)},
        {tpcc::H_DATE, "h_date", kUInt32, 0, false, offsetof(tpcc::history_value_t, h_date)},
        {tpcc::H_DATA, "h_data", kVarChar, tpcc::HISTORY_DATA_SIZE_MAX + 1, false,
         offsetof(tpcc::history_value_t, h_data)},
    };
    TableCreateAttr history_table_attr =
        TableCreateAttr{/* table_id = */ tpcc::HISTORY_TABLE,
                        /* name = */ "history",
                        /* schema_arg = */ history_schema,
                        /* index_config = */ config.history_index_conf,
                        /* cc_level = */ TableCCLevel::RECORD_LEVEL};

    db->CreateTable(history_table_attr);
  }

  // Create the Order Table
  {
    SchemaArg order_schema = {
        {tpcc::O_C_ID, "o_c_id", kInt32, 0, false, offsetof(tpcc::order_value_t, o_c_id)},
        {tpcc::O_CARRIER_ID, "o_carrier_id", kInt32, 0, false,
         offsetof(tpcc::order_value_t, o_carrier_id)},
        {tpcc::O_OL_CNT, "o_ol_cnt", kInt32, 0, false, offsetof(tpcc::order_value_t, o_ol_cnt)},
        {tpcc::O_ALL_LOCAL, "o_all_local", kInt32, 0, false,
         offsetof(tpcc::order_value_t, o_all_local)},
        {tpcc::O_ENTRY_D, "o_entry_d", kInt64, 0, false, offsetof(tpcc::order_value_t, o_entry_d)},
    };
    TableCreateAttr order_table_attr = TableCreateAttr{/* table_id = */ tpcc::ORDER_TABLE,
                                                       /* name = */ "order",
                                                       /* schema_arg = */ order_schema,
                                                       /* index_config = */ config.order_index_conf,
                                                       /* cc_level = */ TableCCLevel::RECORD_LEVEL};
    db->CreateTable(order_table_attr);
  }

  // Create the New Order Table
  {
    SchemaArg new_order_schema = {
        {tpcc::NO_W_ID, "no_w_id", kInt32, 0, false, offsetof(tpcc::new_order_value_t, w_id)},
        {tpcc::NO_D_ID, "no_d_id", kInt32, 0, false, offsetof(tpcc::new_order_value_t, d_id)},
        {tpcc::NO_O_ID, "no_o_id", kInt32, 0, false, offsetof(tpcc::new_order_value_t, o_id)},
    };
    TableCreateAttr new_order_table_attr =
        TableCreateAttr{/* table_id = */ tpcc::NEW_ORDER_TABLE,
                        /* name = */ "new_order",
                        /* schema_arg = */ new_order_schema,
                        /* index_config = */ config.neworder_index_conf,
                        /* cc_level = */ TableCCLevel::RECORD_LEVEL};
    db->CreateTable(new_order_table_attr);
  }

  // Create the OrderLine Table
  {
    SchemaArg orderline_schema = {
        {tpcc::OL_I_ID, "ol_i_id", kInt32, 0, false, offsetof(tpcc::order_line_value_t, ol_i_id)},
        {tpcc::OL_SUPPLY_W_ID, "ol_supply_w_id", kInt32, 0, false,
         offsetof(tpcc::order_line_value_t, ol_supply_w_id)},
        {tpcc::OL_QUANTITY, "ol_quantity", kInt32, 0, false,
         offsetof(tpcc::order_line_value_t, ol_quantity)},
        {tpcc::OL_AMOUNT, "ol_amount", kDouble, 0, false,
         offsetof(tpcc::order_line_value_t, ol_amount)},
        {tpcc::OL_DELIVERY_D, "ol_delivery_d", kUInt32, 0, false,
         offsetof(tpcc::order_line_value_t, ol_delivery_d)},
        {tpcc::OL_DIST_INFO, "ol_dist_info", kVarChar, tpcc::ORDER_LINE_DIST_INFO_SIZE + 1, false,
         offsetof(tpcc::order_line_value_t, ol_dist_info)},
    };
    TableCreateAttr orderline_table_attr =
        TableCreateAttr{/* table_id = */ tpcc::ORDER_LINE_TABLE,
                        /* name = */ "order_line",
                        /* schema_arg = */ orderline_schema,
                        /* index_config = */ config.orderline_index_conf,
                        /* cc_level = */ TableCCLevel::RECORD_LEVEL};
    db->CreateTable(orderline_table_attr);
  }

  // Create the Item Table
  {
    SchemaArg item_schema = {
        {tpcc::I_IM_ID, "i_im_id", kInt32, 0, false, offsetof(tpcc::item_value_t, i_im_id)},
        {tpcc::I_PRICE, "i_price", kDouble, 0, false, offsetof(tpcc::item_value_t, i_price)},
        {tpcc::I_NAME, "i_name", kVarChar, tpcc::ITEM_NAME_SIZE_MAX + 1, false,
         offsetof(tpcc::item_value_t, i_name)},
        {tpcc::I_DATA, "i_data", kVarChar, tpcc::ITEM_DATA_SIZE_MAX + 1, false,
         offsetof(tpcc::item_value_t, i_data)},
    };
    TableCreateAttr item_table_attr = TableCreateAttr{/* table_id = */ tpcc::ITEM_TABLE,
                                                      /* name = */ "item",
                                                      /* schema_arg = */ item_schema,
                                                      /* index_config = */ config.item_index_conf,
                                                      /* cc_level = */ TableCCLevel::RECORD_LEVEL};
    db->CreateTable(item_table_attr);
  }

  // Create the Stock Table
  {
    SchemaArg stock_schema = {
        {tpcc::S_QUANTITY, "s_quantity", kInt32, 0, false,
         offsetof(tpcc::stock_value_t, s_quantity)},
        {tpcc::S_YTD, "s_ytd", kInt32, 0, false, offsetof(tpcc::stock_value_t, s_ytd)},
        {tpcc::S_ORDER_CNT, "s_order_cnt", kInt32, 0, false,
         offsetof(tpcc::stock_value_t, s_order_cnt)},
        {tpcc::S_REMOTE_CNT, "s_remote_cnt", kInt32, 0, false,
         offsetof(tpcc::stock_value_t, s_remote_cnt)},
        {tpcc::S_DIST, "s_dist", kVarChar,
         tpcc::DISTRICTS_PER_WAREHOUSE * (tpcc::STOCK_DIST_INFO_EACH + 1), false,
         offsetof(tpcc::stock_value_t, s_dist)},
        {tpcc::S_DATA, "s_data", kVarChar, tpcc::STOCK_DATA_SIZE + 1, false,
         offsetof(tpcc::stock_value_t, s_data)},
    };
    TableCreateAttr stock_table_attr = TableCreateAttr{/* table_id = */ tpcc::STOCK_TABLE,
                                                       /* name = */ "stock",
                                                       /* schema_arg = */ stock_schema,
                                                       /* index_config = */ config.stock_index_conf,
                                                       /* cc_level = */ TableCCLevel::RECORD_LEVEL};
    db->CreateTable(stock_table_attr);
  }

  return db;
}

bool TpccBenchmark::PopulateDatabaseRecords(BufferManager* bm, Db* db, bool d) {
  util::Timer timer;

  ResetCurrentTime(0);

  // Check some constants
  for (int i = 0; i < tpcc::CONST_STREET_NAME_COUNT; ++i) {
    ASSERT(strlen(tpcc::street_name[i]) <= tpcc::WAREHOUSE_STREET_SIZE_MAX,
           "Street name too long: %s", tpcc::street_name[i]);
  }

  for (int i = 0; i < tpcc::CONST_CITY_NAME_COUNT; ++i) {
    ASSERT(strlen(tpcc::city_name[i]) <= tpcc::WAREHOUSE_CITY_SIZE_MAX, "City name too long: %s",
           tpcc::city_name[i]);
  }

  for (int i = 0; i < tpcc::CONST_STATE_NAME_COUNT; ++i) {
    ASSERT(strlen(tpcc::state_name[i]) <= tpcc::WAREHOUSE_STATE_SIZE, "State name too long: %s",
           tpcc::state_name[i]);
  }

  PopulateWarehouseRecords(bm, db, d);
  PopulateDistrictRecords(bm, db, d);
  PopulateCustomerRecords(bm, db, d);
  PopulateHistoryRecords(bm, db, d);
  PopulateNewOrder_Order_OrderLineTable(bm, db, d);
  PopulateItemRecords(bm, db, d);
  PopulateStockRecords(bm, db, d);

  LOG_INFO("Populate database records: %.2lf ms", timer.ms_elapse());
  LOG_INFO("Primary Table Num: %d", db->GetPrimaryTableNum());
  LOG_INFO("Backup Table Num: %d", db->GetBackupTableNum());
  return true;
}

bool TpccBenchmark::PopulateWarehouseRecords(BufferManager* bm, Db* db, bool deterministic) {
  FastRandom random_generator(0x19289190);
  DbTable* warehouse_table = db->GetTable(tpcc::WAREHOUSE_TABLE);
  char* local_addr = bm->AllocAlign(warehouse_table->GetTableSize(), CACHE_LINE_SIZE);
  if (unlikely(!local_addr || (uint64_t)local_addr % CACHE_LINE_SIZE)) {
    LOG_FATAL("No enough memory for WarehousTable");
    return false;
  }
  node_id_t nid = config_.node_init_attr.nid;
  warehouse_table->SetTableLocalPtr(local_addr);
  warehouse_table->SetTablePoolPtr(MakePoolPtr(nid, (uint64_t)local_addr));

  tpcc::warehouse_value_t w_val;
  DbRecord w_record((char*)&w_val, sizeof(tpcc::warehouse_value_t),
                    warehouse_table->GetRecordSchema());
  int insert_record = 0, fail_insert = 0;
  for (uint32_t w_id = 0; w_id < tpcc_config_.num_warehouse; ++w_id) {
    RecordKey row_key = w_id;
    if (deterministic) {
      w_val.w_tax = DETERMINISTIC_TAX_VALUE;
    } else {
      w_val.w_tax = (double)RandomNumber(random_generator, 0, 2000) / 10000.0;
    }
    w_val.w_ytd = 300000 * 100;
    // Use some human-readable string for the name fields
    std::strcpy(w_val.w_name, ("ware" + std::to_string(w_id)).c_str());
    std::strcpy(w_val.w_street_1, tpcc::street_name[w_id % tpcc::CONST_STREET_NAME_COUNT]);
    std::strcpy(w_val.w_street_2, tpcc::street_name[(w_id + 1) % tpcc::CONST_STREET_NAME_COUNT]);
    std::strcpy(w_val.w_city, tpcc::city_name[w_id % tpcc::CONST_CITY_NAME_COUNT]);
    std::strcpy(w_val.w_state, tpcc::state_name[w_id % tpcc::CONST_STATE_NAME_COUNT]);
    std::strcpy(w_val.w_zip, "123456789");
    bool s = warehouse_table->InsertRecordLocal(row_key, w_record, INIT_INSERT_VERSION);
    fail_insert += !s;
    insert_record++;
  }
  // Set the Property of this table:
  std::string flag = "";
  if (tpcc::WAREHOUSE_TABLE % config_.node_init_attr.num_mns == nid) {
    warehouse_table->SetAsPrimary();
    db->AddPrimaryTableNum();
    flag = "Primary";
  } else {
    warehouse_table->SetAsBackup();
    db->AddBackupTableNum();
    flag = "Backup ";
  }
  LOG_INFO(
      "[%s] [%s Table] addr: %#llx, record_size: %4lu, insert: %8d, failed: %8d, consumes: %.2lf "
      "MiB",
      flag.c_str(), tpcc::warehouse_table_name, warehouse_table->TableLocalPtr(),
      warehouse_table->GetRecordMemSize(), insert_record, fail_insert,
      warehouse_table->GetTableSize() / (double)(1 << 20));

  return true;
}

bool TpccBenchmark::PopulateDistrictRecords(BufferManager* bm, Db* db, bool deterministic) {
  FastRandom random_generator(0x19289190);
  DbTable* district_table = db->GetTable(tpcc::DISTRICT_TABLE);
  char* local_addr = bm->AllocAlign(district_table->GetTableSize(), CACHE_LINE_SIZE);
  if (unlikely(!local_addr || (uint64_t)local_addr % CACHE_LINE_SIZE)) {
    LOG_FATAL("No enough memory for DistrictTable");
    return false;
  }
  node_id_t nid = config_.node_init_attr.nid;
  district_table->SetTableLocalPtr(local_addr);
  district_table->SetTablePoolPtr(MakePoolPtr(nid, (uint64_t)local_addr));

  int insert_record = 0, fail_insert = 0;
  tpcc::district_value_t d_val;
  DbRecord d_record((char*)&d_val, sizeof(tpcc::district_value_t),
                    district_table->GetRecordSchema());
  for (uint32_t w_id = 0; w_id < tpcc_config_.num_warehouse; ++w_id) {
    for (uint32_t d_id = 0; d_id < tpcc_config_.num_district_per_warehouse; ++d_id) {
      RecordKey row_key = tpcc_ctx_->MakeDistrictKey(w_id, d_id);
      if (deterministic) {
        d_val.d_tax = DETERMINISTIC_TAX_VALUE;
      } else {
        d_val.d_tax = ((double)RandomNumber(random_generator, 0, 2000) / 10000.0);
      }
      d_val.d_ytd = 30000 * 100;

      // In LoadNewOrderTable function, each client of each district makes an order.
      // So the next_o_id field of each district is set to be num_customer_per_district
      d_val.d_next_oid = tpcc_config_.num_customer_per_district;
      strcpy(d_val.d_name, ("dist" + std::to_string(row_key)).c_str());
      strcpy(d_val.d_street_1, tpcc::street_name[d_id % tpcc::CONST_STREET_NAME_COUNT]);
      strcpy(d_val.d_street_2, tpcc::street_name[(d_id + 1) % tpcc::CONST_STREET_NAME_COUNT]);
      strcpy(d_val.d_city, tpcc::city_name[d_id % tpcc::CONST_CITY_NAME_COUNT]);
      strcpy(d_val.d_state, tpcc::state_name[d_id % tpcc::CONST_STATE_NAME_COUNT]);
      strcpy(d_val.d_zip, "123456789");
      bool s = district_table->InsertRecordLocal(row_key, d_record, INIT_INSERT_VERSION);
      insert_record++;
      fail_insert += !s;
    }
  }
  std::string flag = "";
  if (tpcc::DISTRICT_TABLE % config_.node_init_attr.num_mns == nid) {
    district_table->SetAsPrimary();
    db->AddPrimaryTableNum();
    flag = "Primary";
  } else {
    district_table->SetAsBackup();
    db->AddBackupTableNum();
    flag = "Backup ";
  }
  LOG_INFO(
      "[%s] [%s Table] addr: %#llx, record_size: %4lu, insert: %8d, failed: %8d, consumes: %.2lf "
      "MiB",
      flag.c_str(), tpcc::district_table_name, district_table->TableLocalPtr(),
      district_table->GetRecordMemSize(), insert_record, fail_insert,
      district_table->GetTableSize() / (double)(1 << 20));
  return true;
}

bool TpccBenchmark::PopulateCustomerRecords(BufferManager* bm, Db* db, bool deterministic) {
  DbTable* customer_table = db->GetTable(tpcc::CUSTOMER_TABLE);
  FastRandom random_generator(0x19289190);
  char* local_addr = bm->AllocAlign(customer_table->GetTableSize(), CACHE_LINE_SIZE);
  if (unlikely(!local_addr || (uint64_t)local_addr % CACHE_LINE_SIZE)) {
    LOG_FATAL("No enough memory for CustomerTable");
    return false;
  }
  node_id_t nid = config_.node_init_attr.nid;
  customer_table->SetTableLocalPtr(local_addr);
  customer_table->SetTablePoolPtr(MakePoolPtr(nid, (uint64_t)local_addr));

  tpcc::customer_value_t c_val;
  DbRecord c_record((char*)&c_val, sizeof(tpcc::customer_value_t),
                    customer_table->GetRecordSchema());

  int insert_record = 0, fail_insert = 0;
  for (uint32_t w_id = 0; w_id < tpcc_config_.num_warehouse; ++w_id) {
    for (uint32_t d_id = 0; d_id < tpcc_config_.num_district_per_warehouse; ++d_id) {
      for (uint32_t c_id = 0; c_id < tpcc_config_.num_customer_per_district; ++c_id) {
        RecordKey row_key = tpcc_ctx_->MakeCustomerKey(w_id, d_id, c_id);
        c_val.c_balance = tpcc::INITIAL_BALANCE;
        c_val.c_ytd_payment = tpcc::INITIAL_YTD_PAYMENT;
        c_val.c_payment_cnt = tpcc::INITIAL_PAYMENT_CNT;

        if (deterministic) {
          strcpy(c_val.c_data, DETERMINISTIC_CUSTOMER_DATA.c_str());
          strcpy(c_val.c_first, DETERMINISTIC_CUSTOMER_FIRST.c_str());
          strcpy(c_val.c_middle, DETERMINISTIC_CUSTOMER_MIDDLE.c_str());
          strcpy(c_val.c_last, DETERMINISTIC_CUSTOMER_LAST.c_str());
        } else {
          uint64_t c_data_size = RandomNumber(random_generator, tpcc::CUSTOMER_DATA_SIZE_MIN,
                                              tpcc::CUSTOMER_DATA_SIZE_MAX);
          RandomStr(random_generator, c_data_size, c_val.c_data);
          uint64_t c_first_size = RandomNumber(random_generator, tpcc::CUSTOMER_FIRST_SIZE_MIN,
                                               tpcc::CUSTOMER_FIRST_SIZE_MAX);
          RandomStr(random_generator, c_first_size, c_val.c_first);
          RandomStr(random_generator, tpcc::CUSTOMER_MIDDLE_SIZE - 1, c_val.c_middle);
          uint64_t c_last_size = RandomNumber(random_generator, tpcc::CUSTOMER_LAST_SIZE_MIN,
                                              tpcc::CUSTOMER_LAST_SIZE_MAX);
          RandomStr(random_generator, c_last_size, c_val.c_last);
        }

        strcpy(c_val.c_street_1, tpcc::street_name[c_id % tpcc::CONST_STREET_NAME_COUNT]);
        strcpy(c_val.c_street_2, tpcc::street_name[(c_id + 1) % tpcc::CONST_STREET_NAME_COUNT]);
        strcpy(c_val.c_city, tpcc::city_name[c_id % tpcc::CONST_CITY_NAME_COUNT]);
        strcpy(c_val.c_state, tpcc::state_name[c_id % tpcc::CONST_STATE_NAME_COUNT]);
        strcpy(c_val.c_zip, "123456789");
        strcpy(c_val.c_phone, "123456789");
        c_val.c_since = 0;
        bool is_bc = deterministic ? c_id % 10 == 0 : RandomNumber(random_generator, 1, 100) <= 10;
        if (is_bc) {
          strcpy(c_val.c_credit, "BC");
        } else {
          strcpy(c_val.c_credit, "GC");
        }
        c_val.c_credit_limit = tpcc::INITIAL_CREDIT_LIM;
        c_val.c_discount = DETERMINISTIC_DISCOUNT_VALUE;
        c_val.c_delivery_cnt = 0;

        bool s = customer_table->InsertRecordLocal(row_key, c_record, INIT_INSERT_VERSION);
        insert_record++;
        fail_insert += !s;
      }
    }
  }
  std::string flag = "";
  if (tpcc::CUSTOMER_TABLE % config_.node_init_attr.num_mns == nid) {
    customer_table->SetAsPrimary();
    db->AddPrimaryTableNum();
    flag = "Primary";
  } else {
    customer_table->SetAsBackup();
    db->AddBackupTableNum();
    flag = "Backup ";
  }
  LOG_INFO(
      "[%s] [%s Table] addr: %#llx, record_size: %4lu, insert: %8d, failed: %8d, consumes: %.2lf "
      "MiB",
      flag.c_str(), tpcc::customer_table_name, customer_table->TableLocalPtr(),
      customer_table->GetRecordMemSize(), insert_record, fail_insert,
      customer_table->GetTableSize() / (double)(1 << 20));

  return true;
}

bool TpccBenchmark::PopulateHistoryRecords(BufferManager* bm, Db* db, bool deterministic) {
  DbTable* history_table = db->GetTable(tpcc::HISTORY_TABLE);
  char* local_addr = bm->AllocAlign(history_table->GetTableSize(), CACHE_LINE_SIZE);
  if (unlikely(!local_addr || (uint64_t)local_addr % CACHE_LINE_SIZE)) {
    LOG_FATAL("No enough memory for HistoryTable");
  }
  node_id_t nid = config_.node_init_attr.nid;
  history_table->SetTableLocalPtr(local_addr);
  history_table->SetTablePoolPtr(MakePoolPtr(nid, (uint64_t)local_addr));
  FastRandom random_generator(0x19289190);

  int insert_record = 0, fail_insert = 0;
  tpcc::history_value_t h_val;
  DbRecord h_record((char*)&h_val, sizeof(tpcc::history_value_t), history_table->GetRecordSchema());
  for (uint32_t w_id = 0; w_id < tpcc_config_.num_warehouse; ++w_id) {
    for (uint32_t d_id = 0; d_id < tpcc_config_.num_district_per_warehouse; ++d_id) {
      for (uint32_t c_id = 0; c_id < tpcc_config_.num_customer_per_district; ++c_id) {
        RecordKey rkey = tpcc_ctx_->MakeHistoryKey(w_id, d_id, c_id, GetCurrentTimeMillis());
        if (deterministic) {
          h_val.h_date = 0;
          h_val.h_amount = tpcc::HISTORY_INITIAL_AMOUNT;
          ASSERT(DETERMINISTIC_HISTORY_DATA.size() < tpcc::HISTORY_DATA_SIZE_MAX, "");
          strcpy(h_val.h_data, DETERMINISTIC_HISTORY_DATA.c_str());
        } else {
          h_val.h_date = GetCurrentTimeMillis();
          h_val.h_amount = tpcc::HISTORY_INITIAL_AMOUNT;
          RandomStr(random_generator, RandomNumber(random_generator, tpcc::HISTORY_DATA_SIZE_MIN,
                                                   tpcc::HISTORY_DATA_SIZE_MAX));
        }
        bool s = history_table->InsertRecordLocal(rkey, h_record, INIT_INSERT_VERSION);
        insert_record++;
        fail_insert += !s;
      }
    }
  }
  std::string flag = "";
  if (tpcc::HISTORY_TABLE % config_.node_init_attr.num_mns == nid) {
    history_table->SetAsPrimary();
    db->AddPrimaryTableNum();
    flag = "Primary";
  } else {
    history_table->SetAsBackup();
    db->AddBackupTableNum();
    flag = "Backup ";
  }
  LOG_INFO(
      "[%s] [%s Table] addr: %#llx, record_size: %4lu, insert: %8d, failed: %8d, consumes: %.2lf "
      "MiB",
      flag.c_str(), tpcc::history_table_name, history_table->TableLocalPtr(),
      history_table->GetRecordMemSize(), insert_record, fail_insert,
      history_table->GetTableSize() / (double)(1 << 20));
  return true;
}

bool TpccBenchmark::PopulateItemRecords(BufferManager* bm, Db* db, bool deterministic) {
  FastRandom random_generator(0x278569172);
  DbTable* item_table = db->GetTable(tpcc::ITEM_TABLE);
  char* local_addr = bm->AllocAlign(item_table->GetTableSize(), CACHE_LINE_SIZE);
  if (unlikely(!local_addr)) {
    LOG_FATAL("No enough memory for ItemTable");
  }
  node_id_t nid = config_.node_init_attr.nid;
  item_table->SetTableLocalPtr(local_addr);
  item_table->SetTablePoolPtr(MakePoolPtr(nid, (uint64_t)local_addr));

  int insert_record = 0, fail_insert = 0;
  tpcc::item_value_t i_val;
  DbRecord i_record((char*)&i_val, sizeof(tpcc::item_value_t), item_table->GetRecordSchema());
  for (uint32_t i_id = 0; i_id < tpcc_config_.num_item; ++i_id) {
    RecordKey row_key = i_id;
    i_val.i_im_id = i_id;
    if (deterministic) {
      i_val.i_price = DETERMINISTIC_ITEM_PRICE;
      strcpy(i_val.i_name, DETERMINISTIC_ITEM_NAME.c_str());
      strcpy(i_val.i_data, DETERMINISTIC_ITEM_DATA.c_str());
    } else {
      i_val.i_price = RandomNumber(random_generator, 100, 10000) / 100.0;
      RandomStr(random_generator,
                RandomNumber(random_generator, tpcc::ITEM_NAME_SIZE_MIN, tpcc::ITEM_NAME_SIZE_MAX),
                i_val.i_name);
      const int len =
          RandomNumber(random_generator, tpcc::ITEM_DATA_SIZE_MIN, tpcc::ITEM_DATA_SIZE_MAX);
      if (RandomNumber(random_generator, 1, 100) > 10) {
        RandomStr(random_generator, len, i_val.i_data);
      } else {
        const int startOriginal = RandomNumber(random_generator, 2, (len - 8));
        const std::string random_str = RandomStr(random_generator, startOriginal) + "ORIGINAL" +
                                       RandomStr(random_generator, len - startOriginal - 8);
        std::strcpy(i_val.i_data, random_str.c_str());
      }
    }

    bool s = item_table->InsertRecordLocal(row_key, i_record, INIT_INSERT_VERSION);
    insert_record++;
    fail_insert += !s;
  }
  std::string flag = "";
  if (tpcc::ITEM_TABLE % config_.node_init_attr.num_mns == nid) {
    item_table->SetAsPrimary();
    db->AddPrimaryTableNum();
    flag = "Primary";
  } else {
    item_table->SetAsBackup();
    db->AddBackupTableNum();
    flag = "Backup ";
  }
  LOG_INFO(
      "[%s] [%s Table] addr: %#llx, record_size: %4lu, insert: %8d, failed: %8d, consumes: %.2lf "
      "MiB",
      flag.c_str(), tpcc::item_table_name, item_table->TableLocalPtr(),
      item_table->GetRecordMemSize(), insert_record, fail_insert,
      item_table->GetTableSize() / (double)(1 << 20));
  return true;
}

bool TpccBenchmark::PopulateStockRecords(BufferManager* bm, Db* db, bool deterministic) {
  FastRandom random_generator(0x8129274191);
  DbTable* stock_table = db->GetTable(tpcc::STOCK_TABLE);
  char* local_addr = bm->AllocAlign(stock_table->GetTableSize(), CACHE_LINE_SIZE);
  if (unlikely(!local_addr)) {
    LOG_FATAL("No enough memory for StockTable");
  }
  node_id_t nid = config_.node_init_attr.nid;
  stock_table->SetTableLocalPtr(local_addr);
  stock_table->SetTablePoolPtr(MakePoolPtr(nid, (uint64_t)local_addr));

  int insert_record = 0, fail_insert = 0;
  tpcc::stock_value_t s_value;
  DbRecord stock_record((char*)&s_value, sizeof(tpcc::stock_value_t),
                        stock_table->GetRecordSchema());
  for (uint32_t w_id = 0; w_id < tpcc_config_.num_warehouse; ++w_id) {
    for (uint32_t i_id = 0; i_id < tpcc_config_.num_item; ++i_id) {
      RecordKey row_key = tpcc_ctx_->MakeStockKey(w_id, i_id);
      s_value.s_quantity = tpcc::INITIAL_QUANTITY;
      s_value.s_ytd = 0;
      s_value.s_order_cnt = 0;
      s_value.s_remote_cnt = 0;
      if (deterministic) {
        std::strcpy(s_value.s_data, DETERMINISTIC_STOCK_DATA.c_str());
      } else {
        const int len =
            RandomNumber(random_generator, tpcc::STOCK_DATA_SIZE / 2, tpcc::STOCK_DATA_SIZE);
        if (RandomNumber(random_generator, 1, 100) > 10) {
          RandomStr(random_generator, len, s_value.s_data);
        } else {
          const int startOriginal = RandomNumber(random_generator, 2, (len - 8));
          std::string s = RandomStr(random_generator, startOriginal) + "ORIGINAL" +
                          RandomStr(random_generator, len - startOriginal - 8);
          std::strcpy(s_value.s_data, s.c_str());
        }
      }
      bool s = stock_table->InsertRecordLocal(row_key, stock_record, INIT_INSERT_VERSION);
      insert_record++;
      fail_insert += !s;
    }
  }
  std::string flag = "";
  if (tpcc::STOCK_TABLE % config_.node_init_attr.num_mns == nid) {
    stock_table->SetAsPrimary();
    db->AddPrimaryTableNum();
    flag = "Primary";
  } else {
    stock_table->SetAsBackup();
    db->AddBackupTableNum();
    flag = "Backup ";
  }
  LOG_INFO(
      "[%s] [%s Table] addr: %#llx, record_size: %4lu, insert: %8d, failed: %8d, consumes: %.2lf "
      "MiB",
      flag.c_str(), tpcc::stock_table_name, stock_table->TableLocalPtr(),
      stock_table->GetRecordMemSize(), insert_record, fail_insert,
      stock_table->GetTableSize() / (double)(1 << 20));
  return true;
}

bool TpccBenchmark::PopulateNewOrder_Order_OrderLineTable(BufferManager* bm, Db* db,
                                                          bool deterministic) {
  FastRandom random_generator(0x18291924412);

  DbTable* order_table = db->GetTable(tpcc::ORDER_TABLE);
  DbTable* new_order_table = db->GetTable(tpcc::NEW_ORDER_TABLE);
  DbTable* order_line_table = db->GetTable(tpcc::ORDER_LINE_TABLE);

  node_id_t nid = config_.node_init_attr.nid;

  char* local_addr = bm->AllocAlign(order_table->GetTableSize(), CACHE_LINE_SIZE);
  if (unlikely(!local_addr || (uint64_t)local_addr % CACHE_LINE_SIZE)) {
    LOG_FATAL("No enough memory for OrderTable");
  }
  order_table->SetTableLocalPtr(local_addr);
  order_table->SetTablePoolPtr(MakePoolPtr(nid, (uint64_t)local_addr));

  local_addr = bm->AllocAlign(new_order_table->GetTableSize(), CACHE_LINE_SIZE);
  if (unlikely(!local_addr || (uint64_t)local_addr % CACHE_LINE_SIZE)) {
    LOG_FATAL("No enough memory for NewOrderTable");
  }
  new_order_table->SetTableLocalPtr(local_addr);
  new_order_table->SetTablePoolPtr(MakePoolPtr(nid, (uint64_t)local_addr));

  local_addr = bm->AllocAlign(order_line_table->GetTableSize(), CACHE_LINE_SIZE);
  if (unlikely(!local_addr || (uint64_t)local_addr % CACHE_LINE_SIZE)) {
    LOG_FATAL("No enough memory for OrderLineTable");
  }
  order_line_table->SetTableLocalPtr(local_addr);
  order_line_table->SetTablePoolPtr(MakePoolPtr(nid, (uint64_t)local_addr));

  int neworder_insert_record = 0, order_insert_record = 0, orderline_insert_record = 0;
  int neworder_fail_insert = 0, order_fail_insert = 0, orderline_fail_insert = 0;

  tpcc::order_value_t o_val;
  tpcc::new_order_value_t no_val;
  tpcc::order_line_value_t ol_val;

  DbRecord o_record((char*)&o_val, sizeof(tpcc::order_value_t), order_table->GetRecordSchema());
  DbRecord no_record((char*)&no_val, sizeof(tpcc::new_order_value_t),
                     new_order_table->GetRecordSchema());
  DbRecord ol_record((char*)&ol_val, sizeof(tpcc::order_line_value_t),
                     order_line_table->GetRecordSchema());

  for (uint32_t w_id = 0; w_id < tpcc_config_.num_warehouse; ++w_id) {
    for (uint32_t d_id = 0; d_id < tpcc_config_.num_district_per_warehouse; ++d_id) {
      std::set<uint32_t> c_ids_s;
      std::vector<uint32_t> c_ids;
      while (c_ids.size() != tpcc_config_.num_customer_per_district) {
        const auto x = (random_generator.Next() % tpcc_config_.num_customer_per_district) + 1;
        if (c_ids_s.count(x)) continue;
        c_ids_s.insert(x);
        c_ids.emplace_back(x);
      }

      for (uint32_t c = 0; c < tpcc_config_.num_customer_per_district; ++c) {
        RecordKey order_key = tpcc_ctx_->MakeOrderKey(w_id, d_id, c);
        o_val.o_c_id = c_ids[c];
        if (!deterministic && c <= tpcc_config_.num_customer_per_district * 0.7) {
          o_val.o_carrier_id =
              RandomNumber(random_generator, tpcc::MIN_CARRIER_ID, tpcc::MAX_CARRIER_ID);
        } else {
          o_val.o_carrier_id = 0;
        }

        if (deterministic) {
          o_val.o_ol_cnt = (tpcc::MIN_OL_CNT + tpcc::MAX_OL_CNT) / 2;
          o_val.o_entry_d = 0;
        } else {
          o_val.o_ol_cnt = RandomNumber(random_generator, tpcc::MIN_OL_CNT, tpcc::MAX_OL_CNT);
          o_val.o_entry_d = GetCurrentTimeMillis();
        }
        o_val.o_all_local = 1;

        bool s = order_table->InsertRecordLocal(order_key, o_record, INIT_INSERT_VERSION);
        order_insert_record++;
        order_fail_insert += !s;

        if (c > tpcc_config_.num_customer_per_district * 0.7) {
          RecordKey no_key = tpcc_ctx_->MakeNewOrderKey(w_id, d_id, c);
          no_val.w_id = w_id;
          no_val.d_id = d_id;
          no_val.o_id = c;
          bool s = new_order_table->InsertRecordLocal(no_key, no_record, INIT_INSERT_VERSION);
          neworder_insert_record++;
          neworder_fail_insert += !s;
        }

        for (uint32_t l = 0; l < o_val.o_ol_cnt; ++l) {
          RecordKey ol_key = tpcc_ctx_->MakeOrderLineKey(w_id, d_id, c, l);
          ol_val.ol_i_id = RandomNumber(random_generator, 0, tpcc_config_.num_item);
          if (c <= tpcc_config_.num_customer_per_district * 0.7) {
            ol_val.ol_delivery_d = o_val.o_entry_d;
            ol_val.ol_amount = 0;
          } else {
            ol_val.ol_delivery_d = 0;
            ol_val.ol_amount = DETERMINISTIC_TAX_VALUE;
          }

          ol_val.ol_supply_w_id = w_id;
          ol_val.ol_quantity = tpcc::INITIAL_QUANTITY;

          bool s = order_line_table->InsertRecordLocal(ol_key, ol_record, INIT_INSERT_VERSION);
          orderline_insert_record++;
          if (!s) {
            orderline_fail_insert += 1;
          }
        }
      }
    }
  }

  std::string flag = "";
  if (tpcc::ORDER_TABLE % config_.node_init_attr.num_mns == nid) {
    order_table->SetAsPrimary();
    db->AddPrimaryTableNum();
    flag = "Primary";
  } else {
    order_table->SetAsBackup();
    db->AddBackupTableNum();
    flag = "Backup ";
  }
  LOG_INFO(
      "[%s] [%s Table] addr: %#llx, record_size: %4lu, insert: %8d, failed: %8d, consumes: %.2lf "
      "MiB",
      flag.c_str(), tpcc::order_table_name, order_table->TableLocalPtr(),
      order_table->GetRecordMemSize(), order_insert_record, order_fail_insert,
      order_table->GetTableSize() / (double)(1 << 20));

  if (tpcc::NEW_ORDER_TABLE % config_.node_init_attr.num_mns == nid) {
    new_order_table->SetAsPrimary();
    db->AddPrimaryTableNum();
    flag = "Primary";
  } else {
    new_order_table->SetAsBackup();
    db->AddBackupTableNum();
    flag = "Backup ";
  }
  LOG_INFO(
      "[%s] [%s Table] addr: %#llx, record_size: %4lu, insert: %8d, failed: %8d, consumes: %.2lf "
      "MiB",
      flag.c_str(), tpcc::neworder_table_name, new_order_table->TableLocalPtr(),
      new_order_table->GetRecordMemSize(), neworder_insert_record, neworder_fail_insert,
      new_order_table->GetTableSize() / (double)(1 << 20));

  if (tpcc::ORDER_LINE_TABLE % config_.node_init_attr.num_mns == nid) {
    order_line_table->SetAsPrimary();
    db->AddPrimaryTableNum();
    flag = "Primary";
  } else {
    order_line_table->SetAsBackup();
    db->AddBackupTableNum();
    flag = "Backup ";
  }
  LOG_INFO(
      "[%s] [%s Table] addr: %#llx, record_size: %4lu, insert: %8d, failed: %8d, consumes: %.2lf "
      "MiB",
      flag.c_str(), tpcc::orderline_table_name, order_line_table->TableLocalPtr(),
      order_line_table->GetRecordMemSize(), orderline_insert_record, orderline_fail_insert,
      order_line_table->GetTableSize() / (double)(1 << 20));

  return true;
}