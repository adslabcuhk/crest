#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <exception>

#include "TPCC/TpccConstant.h"
#include "TPCC/TpccTableStructs.h"
#include "TPCC/TpccTxnImpl.h"
#include "TPCC/TpccTxnStructs.h"
#include "TPCC/TpccUtil.h"
#include "transaction/Enums.h"
#include "transaction/Txn.h"
#include "util/Logger.h"

namespace tpcc {

bool TxnPayment(Txn* txn, TxnId txn_id, PaymentTxnParam* params, PaymentTxnResult* result,
                coro_yield_t& yield, bool return_results) {
  // "getWarehouse":
  //    SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP
  //    FROM WAREHOUSE
  //    WHERE W_ID = ?"
  //
  // "updateWarehouseBalance":
  //    UPDATE WAREHOUSE
  //    SET W_YTD = W_YTD + ?
  //    WHERE W_ID = ?"

  txn->Begin(txn_id, TxnType::READ_WRITE, 0, "TxnPayment");

  TxnRecordRef warehouse_record =
      txn->SelectRecord(WAREHOUSE_TABLE, params->WarehouseKey(), AccessMode::READ_WRITE);
  warehouse_record->add_rw_column(W_YTD);
  warehouse_record->add_ro_columns({W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP});

  // "getDistrict":
  //    SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP
  //    FROM DISTRICT
  //    WHERE D_W_ID = ? AND D_ID = ?"
  //
  // "updateDistrictBalance":
  //    UPDATE DISTRICT
  //    SET D_YTD = D_YTD + ?
  //    WHERE D_ID  = ? AND D_W_ID = ?"
  TxnRecordRef district_record =
      txn->SelectRecord(DISTRICT_TABLE, params->DistrictKey(), AccessMode::READ_WRITE);
  district_record->add_rw_column(D_YTD);
  district_record->add_ro_columns({D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP});

  // "getCustomerByCustomerId":
  //    SELECT  C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2,
  //            C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
  //            C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA
  //    FROM CUSTOMER
  //    WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
  //
  // "updateBCCustomer":
  //    UPDATE CUSTOMER
  //    SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ?
  //    WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
  //
  // "updateGCCustomer":
  //    UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?
  //    WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"

  TxnRecordRef customer_record =
      txn->SelectRecord(CUSTOMER_TABLE, params->CustomerKey(), AccessMode::READ_WRITE);

  customer_record->add_rw_column(C_BALANCE);
  customer_record->add_rw_column(C_YTD_PAYMENT);
  customer_record->add_rw_column(C_PAYMENT_CNT);
  customer_record->add_rw_column(C_DATA);
  customer_record->add_ro_columns({C_FIRST, C_CREDIT});

  params->h_date_ = GetCurrentTimeMillis();
  TxnRecordRef history_record = txn->InsertRecord(HISTORY_TABLE, params->HistoryKey());

  Status s = txn->Deref(yield);
  if (!s.ok()) {
    return false;
  }

  // Update w_ytd:
  double w_ytd = warehouse_record->Get<double>(W_YTD);
  double ov = w_ytd;
  w_ytd += params->h_amount_;
  warehouse_record->Write<double>(W_YTD, w_ytd);
  warehouse_record->finish();

  // LOG_INFO("Ts: %lu, Warehouse %d Address: %p, YTD: %f -> %f\n", txn->GetCommitTimestamp(),
  //          params->WarehouseKey(), warehouse_record->GetAddress(W_YTD), ov, w_ytd);

  // Update d_ytd:
  double d_ytd = district_record->Get<double>(D_YTD);
  double rd_d_ytd = d_ytd;
  d_ytd += params->h_amount_;
  district_record->Write<double>(D_YTD, d_ytd);
  district_record->finish();

  // Update customer value:
  // Update balance, ytd_payment, payment_cnt:
  double c_balance = customer_record->Get<double>(C_BALANCE);
  c_balance -= params->h_amount_;

  double c_ytd_payment = customer_record->Get<double>(C_YTD_PAYMENT);
  c_ytd_payment += params->h_amount_;

  uint32_t c_payment_cnt = customer_record->Get<uint32_t>(C_PAYMENT_CNT);
  c_payment_cnt += 1;

  customer_record->Write<double>(C_BALANCE, c_balance);
  customer_record->Write<double>(C_YTD_PAYMENT, c_ytd_payment);
  customer_record->Write<uint32_t>(C_PAYMENT_CNT, c_payment_cnt);

  char* credit = customer_record->Get<char*>(C_CREDIT);
  char* c_data = customer_record->Get<char*>(C_DATA);
  if (strcmp(credit, "BC") == 0) {
    // Only add a history record to the C_DATA field when current customer has bad
    // creit. This can help saves the space
    static const int HISTORY_SIZE = tpcc::CUSTOMER_DATA_SIZE_MAX;
    char history[HISTORY_SIZE];
    int characters = snprintf(history, HISTORY_SIZE, "(%d, %d, %d, %d, %d, %.2f)\n",
                              params->customer_id_, params->c_d_id_, params->c_w_id_,
                              params->district_id_, params->warehouse_id_, params->h_amount_);
    int current_keep = strlen(c_data);
    if (current_keep + characters > tpcc::CUSTOMER_DATA_SIZE_MAX - 1) {
      current_keep = tpcc::CUSTOMER_DATA_SIZE_MAX - 1 - characters;
    }
    // Move the history payment history forward
    memmove(c_data + characters, c_data, current_keep);
    memcpy(c_data, history, characters);
    c_data[characters + current_keep] = '\0';

    customer_record->add_commit_column(C_DATA);
  }
  customer_record->finish();

  // Construct the history value:
  tpcc::history_value_t h_value;
  h_value.h_amount = params->h_amount_;
  h_value.h_date = params->h_date_;
  strcpy(h_value.h_data, warehouse_record->Get<char*>(W_NAME));
  strcat(h_value.h_data, "    ");
  strcat(h_value.h_data, district_record->Get<char*>(D_NAME));

  DbRecord h_record((char*)&h_value, sizeof(h_value), txn->GetRecordSchema(HISTORY_TABLE));
  history_record->Write(h_record);
  history_record->finish();

  s = txn->Commit(yield);
  if (!s.ok()) {
    return false;
  }

  // Return the transaction execution result to the client
  if (return_results) {
    result->SetCommitTimestamp(txn->GetCommitTimestamp());
    result->SetTxnId(txn->GetTxnId());
    DbRecord w_record((char*)&result->w_val, sizeof(result->w_val),
                      txn->GetRecordSchema(WAREHOUSE_TABLE));
    DbRecord d_record((char*)&result->d_val, sizeof(result->d_val),
                      txn->GetRecordSchema(DISTRICT_TABLE));
    DbRecord c_record((char*)&result->c_val, sizeof(result->c_val),
                      txn->GetRecordSchema(CUSTOMER_TABLE));
    warehouse_record->Get(&w_record);
    double w_ytd = warehouse_record->Get<double>(W_YTD);

#if DEBUG_LOCALIZATION
    result->txn_count = district_record->txn_count();
    result->rw_txn_count = district_record->rw_txn_count();
    result->unref_txn_count = district_record->unref_txn_count();
    result->unref_rw_txn_count = district_record->unref_rw_txn_count();
    result->pv_access_bm = district_record->pv_access_bitmap();
    result->pv_rw_bm = district_record->pv_rw_bitmap();
#endif

#if DEBUG_LOCALIZATION_PERF
    result->yield_next_time = txn->GetTxnDetail().wait_batchread_data_dura;
    for (int i = 0; i < 16; ++i) {
      result->yield_next_count[i] = txn->get_yield_next_count(i);
    }
#endif
    const auto& dependent_table_ids = txn->GetDependentTxnTableIds();
    result->cnt = dependent_table_ids.size();
    for (size_t i = 0; i < std::min(dependent_table_ids.size(), (size_t)16); ++i) {
      result->dependent_table_ids_[i] = dependent_table_ids[i];
    }
    result->wait_dependent_txn_time_ = txn->GetWaitDependentTxnTime();
#if DEBUG_PAYMENT
    // result->rd_d_ytd = district_record->get_rd_d_ytd();
    result->rd_d_ytd = rd_d_ytd;
    // result->wr_d_ytd = district_record->get_wr_d_ytd();
    result->wr_d_ytd = d_ytd;
    result->rd_w_ytd = district_record->get_rd_w_ytd();
    result->wr_w_ytd = district_record->get_wr_w_ytd();
    result->dg_lock_bm = district_record->dg_lock_bitmap();
    result->dg_commit_bm = district_record->dg_commit_bitmap();
#endif
    district_record->Get(&d_record);
    customer_record->Get(&c_record);
  }

  return true;
}
}  // namespace tpcc
