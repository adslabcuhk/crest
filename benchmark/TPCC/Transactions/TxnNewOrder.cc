#include <cstdint>
#include <cstring>

#include "TPCC/TpccConstant.h"
#include "TPCC/TpccTableStructs.h"
#include "TPCC/TpccTxnImpl.h"
#include "common/Type.h"
#include "db/DbRecord.h"
#include "mempool/Coroutine.h"
#include "transaction/Enums.h"
#include "transaction/Txn.h"
#include "util/Logger.h"

// This Macro is used for debug
#define NEWORDER_INSERT 1

namespace tpcc {
bool TxnNewOrder(Txn* txn, TxnId txn_id, NewOrderTxnParam* params, NewOrderTxnResult* result,
                 coro_yield_t& yield, bool return_results) {
  txn->Begin(txn_id, TxnType::READ_WRITE, 0, "TxnNewOrder");

  // "getWarehouseTaxRate":
  //   "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?"
  TxnRecordRef warehouse_record =
      txn->SelectRecord(WAREHOUSE_TABLE, params->WarehouseKey(), AccessMode::READ_ONLY);
  warehouse_record->add_ro_column(W_TAX);

  //
  // "getDistrict":
  //    SELECT D_TAX, D_NEXT_O_ID
  //    FROM DISTRICT
  //    WHERE D_ID = ? AND D_W_ID = ?"
  //
  // "incrementNextOrderId":
  //    UPDATE DISTRICT
  //    SET D_NEXT_O_ID = ?
  //    WHERE D_ID = ? AND D_W_ID = ?"
  //
  TxnRecordRef district_record =
      txn->SelectRecord(DISTRICT_TABLE, params->DistrictKey(), AccessMode::READ_WRITE);
  district_record->add_ro_column(D_TAX);
  district_record->add_rw_column(D_NEXT_OID);

  //
  // "getCustomer":
  //    SELECT C_DISCOUNT, C_LAST, C_CREDIT
  //    FROM CUSTOMER
  //    WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
  //
  TxnRecordRef customer_record =
      txn->SelectRecord(CUSTOMER_TABLE, params->CustomerKey(), AccessMode::READ_ONLY);
  customer_record->add_ro_columns({C_DISCOUNT, C_LAST, C_CREDIT});

  // "getStockInfo":
  //    SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d
  //    FROM STOCK
  //    WHERE S_I_ID = ? AND S_W_ID = ?"
  //
  // "updateStock":
  //    UPDATE STOCK
  //    SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ?
  //    WHERE S_I_ID = ? AND S_W_ID = ?"
  //
  // "getItemInfo":
  //    SELECT I_PRICE, I_NAME, I_DATA
  //    FROM ITEM
  //    WHERE I_ID = ?"

  std::vector<TxnRecordRef> item_records, stock_records, orderline_records;
  item_records.reserve(MAX_OL_CNT);
  stock_records.reserve(MAX_OL_CNT);
  orderline_records.reserve(MAX_OL_CNT);

  // clang-format off
  uint64_t item_ro_bitmap = (TxnRecord::BIT_ONE >> I_PRICE) |
                            (TxnRecord::BIT_ONE >> I_NAME)  |
                            (TxnRecord::BIT_ONE >> I_DATA);

  uint64_t stock_rw_bitmap = (TxnRecord::BIT_ONE >> S_QUANTITY)   | 
                             (TxnRecord::BIT_ONE >> S_YTD)        |
                             (TxnRecord::BIT_ONE >> S_ORDER_CNT)  |
                             (TxnRecord::BIT_ONE >> S_REMOTE_CNT);
  // clang-format on

  // Register the stock records and item records:
  for (int i = 0; i < params->ol_num_; ++i) {
    item_records.push_back(
        txn->SelectRecord(ITEM_TABLE, params->ItemKey(i), AccessMode::READ_ONLY));
    item_records.back()->add_ro_columns(item_ro_bitmap);
  }

  for (int i = 0; i < params->ol_num_; ++i) {
    stock_records.push_back(
        txn->SelectRecord(STOCK_TABLE, params->StockKey(i), AccessMode::READ_WRITE));
    stock_records.back()->add_rw_columns(stock_rw_bitmap);
  }

  // The first round of execution: get the next order id
  Status s = txn->Deref(yield);
  if (!s.ok()) {
    return false;
  }

  // Get the value of warehouse record and district record
  double w_tax = warehouse_record->Get<double>(W_TAX);
  warehouse_record->finish();

  uint64_t next_o_id = district_record->Get<uint64_t>(D_NEXT_OID);
  next_o_id += 1;
  double d_tax = district_record->Get<double>(D_TAX);
  district_record->Write<uint64_t>(D_NEXT_OID, next_o_id);
  district_record->finish();

  customer_record->finish();

#if NEWORDER_INSERT
  // Insert the new order record:
  TxnRecordRef new_order_record =
      txn->InsertRecord(NEW_ORDER_TABLE, params->NewOrderKey(next_o_id));

  // Insert the order table record:
  TxnRecordRef order_record = txn->InsertRecord(ORDER_TABLE, params->OrderKey(next_o_id));

  // Insert the Orderline records:
  for (int i = 0; i < params->ol_num_; ++i) {
    orderline_records.push_back(
        txn->InsertRecord(ORDER_LINE_TABLE, params->OrderLineKey(next_o_id, i)));
  }
#endif

  // Do the value updates:
  for (int i = 0; i < params->ol_num_; ++i) {
    // Update the stock record:
    TxnRecordRef stock_record = stock_records[i];
    TxnRecordRef item_record = item_records[i];

    // Update S_QUANTITY:
    int32_t old_quantity = stock_record->Get<int32_t>(S_QUANTITY);
    int32_t new_quantity = 0;
    if (old_quantity >= params->quantities_[i] + 10) {
      new_quantity = old_quantity - params->quantities_[i];
    } else {
      new_quantity = old_quantity + 91 - params->quantities_[i];
    }
    stock_record->Write<int32_t>(S_QUANTITY, new_quantity);
    // LOG_INFO("Stock record %lu, quantity: %d -> %d", stock_record->record_key(), old_quantity,
    //          new_quantity);

    // Update the S_YTD
    int32_t s_ytd = stock_record->Get<int32_t>(S_YTD);
    s_ytd += params->quantities_[i];
    stock_record->Write<int32_t>(S_YTD, s_ytd);

    // Update the S_ORDER_CNT:
    int32_t s_order_cnt = stock_record->Get<int32_t>(S_ORDER_CNT);
    s_order_cnt += 1;
    stock_record->Write<int32_t>(S_ORDER_CNT, s_order_cnt);

    // Update the S_REMOTE_CNT:
    if (params->supply_warehouse_ids_[i] != params->warehouse_id_) {
      int32_t s_remote_cnt = stock_record->Get<int32_t>(S_REMOTE_CNT);
      s_remote_cnt += 1;
      stock_record->Write<int32_t>(S_REMOTE_CNT, s_remote_cnt);
    }
    stock_record->finish();
    item_record->finish();
  }

#if NEWORDER_INSERT
  // Fetch the value
  s = txn->Deref(yield);
  if (!s.ok()) {
    return false;
  }

  // Update the new order value
  new_order_value_t no_val;
  no_val.w_id = params->warehouse_id_;
  no_val.d_id = params->district_id_;
  no_val.o_id = static_cast<int32_t>(next_o_id);
  DbRecord new_order_db_record((char*)&no_val, sizeof(new_order_value_t),
                               txn->GetRecordSchema(NEW_ORDER_TABLE));
  new_order_record->Write(new_order_db_record);

  // Update the order table value
  order_value_t o_val;
  o_val.o_c_id = params->customer_id_;
  o_val.o_carrier_id = 0;
  o_val.o_ol_cnt = params->ol_num_;
  o_val.o_all_local = 0;
  o_val.o_entry_d = 123456789;
  DbRecord order_db_record((char*)&o_val, sizeof(order_value_t), txn->GetRecordSchema(ORDER_TABLE));
  order_record->Write(order_db_record);

  order_line_value_t ol_val;
  DbRecord ol_db_record((char*)&ol_val, sizeof(order_line_value_t),
                        txn->GetRecordSchema(ORDER_LINE_TABLE));
  // Construct value for orderline records:
  for (int i = 0; i < params->ol_num_; ++i) {
    TxnRecordRef orderline_record = orderline_records[i];
    ol_val.ol_i_id = params->item_ids_[i];
    ol_val.ol_quantity = params->quantities_[i];
    ol_val.ol_supply_w_id = params->supply_warehouse_ids_[i];
    ol_val.ol_amount = params->ol_num_;
    orderline_record->Write(ol_db_record);
  }
#endif

  s = txn->Commit(yield);
  if (!s.ok()) {
    return false;
  }

  if (return_results) {
    result->SetCommitTimestamp(txn->GetCommitTimestamp());
    result->SetTxnId(txn->GetTxnId());
    result->w_tax = warehouse_record->Get<double>(W_TAX);
    result->d_tax = warehouse_record->Get<double>(D_TAX);
    result->next_o_id = next_o_id;

    result->c_discount = customer_record->Get<double>(C_DISCOUNT);
    std::strcpy(result->c_last, customer_record->Get<char*>(C_LAST));
    std::strcpy(result->c_credit, customer_record->Get<char*>(C_CREDIT));

    for (int i = 0; i < params->ol_num_; ++i) {
      stock_records[i]->Get((char*)&result->stock_vals[i]);
      item_records[i]->Get((char*)&result->item_vals[i]);
    }

#if DEBUG_LOCALIZATION_PERF
    result->yield_next_time = txn->GetTxnDetail().wait_batchread_data_dura;
    for (int i = 0; i < 16; ++i) {
      result->yield_next_count[i] = txn->get_yield_next_count(i);
    }
    const auto& dependent_table_ids = txn->GetDependentTxnTableIds();
    result->cnt = dependent_table_ids.size();
    for (size_t i = 0; i < std::min(dependent_table_ids.size(), (size_t)16); ++i) {
      result->dependent_table_ids_[i] = dependent_table_ids[i];
    }
    result->wait_dependent_txn_time_ = txn->GetWaitDependentTxnTime();
  }
#endif

  return true;
}
};  // namespace tpcc