#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <limits>

#include "TPCC/TpccBenchmark.h"
#include "TPCC/TpccConstant.h"
#include "TPCC/TpccTableStructs.h"
#include "TPCC/TpccTxnStructs.h"
#include "TPCC/TpccUtil.h"
#include "db/Db.h"
#include "db/DbRecord.h"
#include "db/Table.h"
#include "mempool/BufferManager.h"
#include "util/Logger.h"
#include "util/Macros.h"
#include "util/Timer.h"

bool TpccBenchmark::TxnNewOrderReplay(Db* db, tpcc::NewOrderTxnParam* param,
                                      tpcc::NewOrderTxnResult* result) {
    bool s = true;
    // "getWarehouseTaxRate":
    //   "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?"
    DbTable* warehouse_table = db->GetTable(tpcc::WAREHOUSE_TABLE);
    DbTable* district_table = db->GetTable(tpcc::DISTRICT_TABLE);
    DbTable* customer_table = db->GetTable(tpcc::CUSTOMER_TABLE);

    // "getWarehouseTaxRate":
    //   "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?"
    tpcc::warehouse_value_t w_val;
    DbRecord warehouse_record((char*)&w_val, sizeof(tpcc::warehouse_value_t),
                              warehouse_table->GetRecordSchema());
    warehouse_table->SearchRecordLocal(param->WarehouseKey(), &warehouse_record);
    result->w_tax = w_val.w_tax;

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
    tpcc::district_value_t d_val;
    DbRecord district_record((char*)&d_val, sizeof(tpcc::district_value_t),
                             district_table->GetRecordSchema());
    district_table->SearchRecordLocal(param->DistrictKey(), &district_record);

    // Update the record:
    d_val.d_next_oid += 1;
    s = district_table->UpdateRecordLocal(param->DistrictKey(), district_record, 0);

    result->next_o_id = d_val.d_next_oid;
    result->d_tax = d_val.d_tax;
    uint64_t next_oid = result->next_o_id;

    //
    // "getCustomer":
    //    SELECT C_DISCOUNT, C_LAST, C_CREDIT
    //    FROM CUSTOMER
    //    WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
    tpcc::customer_value_t c_val;
    DbRecord customer_record((char*)&c_val, sizeof(tpcc::customer_value_t),
                             customer_table->GetRecordSchema());
    customer_table->SearchRecordLocal(param->CustomerKey(), &customer_record);
    result->c_discount = c_val.c_discount;
    strcpy(result->c_last, c_val.c_last);
    strcpy(result->c_credit, c_val.c_credit);

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
    DbTable* stock_table = db->GetTable(tpcc::STOCK_TABLE);
    DbTable* item_table = db->GetTable(tpcc::ITEM_TABLE);

    std::vector<DbRecord> stock_records;
    for (int i = 0; i < param->ol_num_; ++i) {
        // Get Stock Records
        stock_records.emplace_back((char*)&result->stock_vals[i], sizeof(tpcc::stock_value_t),
                                   stock_table->GetRecordSchema());
        DbRecord item_record((char*)&result->item_vals[i], sizeof(tpcc::item_value_t),
                             item_table->GetRecordSchema());
        bool s = stock_table->SearchRecordLocal(param->StockKey(i), &stock_records[i]);
        ASSERT(s, "TxnNewOrderReplay: Failed to locate Stock Record");

        // Update Stock records:
        if (result->stock_vals[i].s_quantity >= param->quantities_[i] + 10) {
            result->stock_vals[i].s_quantity -= param->quantities_[i];
        } else {
            auto res = result->stock_vals[i].s_quantity + 91 - param->quantities_[i];
            result->stock_vals[i].s_quantity = res;
        }

        result->stock_vals[i].s_order_cnt += 1;
        result->stock_vals[i].s_remote_cnt +=
            (param->supply_warehouse_ids_[i] != param->warehouse_id_);
        result->stock_vals[i].s_ytd += param->quantities_[i];
        s = stock_table->UpdateRecordLocal(param->StockKey(i), stock_records[i], 0);
        ASSERT(s, "TxnNewOrderReplay: Failed to Update Stock Record");

        // Get the Item records
        s = item_table->SearchRecordLocal(param->ItemKey(i), &item_record);
        ASSERT(s, "TxnNewOrderReplay: Failed to locate Item Record %lu", param->ItemKey(i));
    }

    // Insert Order, NewOrder, Orderline Records
    DbTable* orderline_table = db->GetTable(tpcc::ORDER_LINE_TABLE);
    DbTable* neworder_table = db->GetTable(tpcc::NEW_ORDER_TABLE);
    DbTable* order_table = db->GetTable(tpcc::ORDER_TABLE);

    tpcc::order_value_t o_val;
    DbRecord order_record((char*)&o_val, sizeof(tpcc::order_value_t),
                          order_table->GetRecordSchema());
    o_val.o_c_id = param->customer_id_;
    o_val.o_carrier_id = 0;
    o_val.o_ol_cnt = param->ol_num_;
    o_val.o_all_local = 0;
    o_val.o_entry_d = GetCurrentTimeMillis();
    s = order_table->InsertRecordLocal(param->OrderKey(next_oid), order_record,
                                       INIT_INSERT_VERSION);

    tpcc::new_order_value_t no_val;
    DbRecord neworder_record((char*)&no_val, sizeof(tpcc::new_order_value_t),
                             neworder_table->GetRecordSchema());
    no_val.w_id = param->warehouse_id_;
    no_val.d_id = param->district_id_;
    no_val.o_id = next_oid;
    s = neworder_table->InsertRecordLocal(param->NewOrderKey(next_oid), neworder_record,
                                          INIT_INSERT_VERSION);

    // Multiple orderline records
    for (int i = 0; i < param->ol_num_; ++i) {
        tpcc::order_line_value_t ol_val;
        DbRecord ol_record((char*)&ol_val, sizeof(tpcc::order_line_value_t),
                           orderline_table->GetRecordSchema());
        ol_val.ol_i_id = param->item_ids_[i];
        ol_val.ol_delivery_d = 0;
        ol_val.ol_amount = param->quantities_[i] * (result->item_vals[i].i_price);
        ol_val.ol_supply_w_id = param->supply_warehouse_ids_[i];
        ol_val.ol_quantity = param->quantities_[i];
        s = orderline_table->InsertRecordLocal(param->OrderLineKey(next_oid, i), ol_record,
                                               INIT_INSERT_VERSION);
        ASSERT(s, "Insert Orderline Table finished");
    }

    return true;
}

bool TpccBenchmark::TxnPaymentReplay(Db* db, tpcc::PaymentTxnParam* param,
                                     tpcc::PaymentTxnResult* result) {
    //
    // "getWarehouse":
    //    SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP
    //    FROM WAREHOUSE
    //    WHERE W_ID = ?"
    //
    // "updateWarehouseBalance":
    //    UPDATE WAREHOUSE
    //    SET W_YTD = W_YTD + ?
    //    WHERE W_ID = ?"
    //
    DbTable* warehouse_table = db->GetTable(tpcc::WAREHOUSE_TABLE);
    DbRecord warehouse_record((char*)&result->w_val, sizeof(tpcc::warehouse_value_t),
                              warehouse_table->GetRecordSchema());
    bool s = warehouse_table->SearchRecordLocal(param->WarehouseKey(), &warehouse_record);
    ASSERT(s, "TxnPaymentReplay Failed: Target record not located");
    result->w_val.w_ytd += param->h_amount_;

    // "getDistrict":
    //    SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP
    //    FROM DISTRICT
    //    WHERE D_W_ID = ? AND D_ID = ?"
    //
    // "updateDistrictBalance":
    //    UPDATE DISTRICT
    //    SET D_YTD = D_YTD + ?
    //    WHERE D_W_ID  = ? AND D_ID = ?"
    DbTable* district_table = db->GetTable(tpcc::DISTRICT_TABLE);
    DbRecord district_record((char*)&result->d_val, sizeof(tpcc::district_value_t),
                             district_table->GetRecordSchema());
    s = district_table->SearchRecordLocal(param->DistrictKey(), &district_record);
    ASSERT(s, "TxnPaymentReplay Failed: Target record not located");
    result->d_val.d_ytd += param->h_amount_;

    // "getCustomerByCustomerId":
    //    SELECT  C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2,
    //            C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
    //            C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA
    //    FROM CUSTOMER
    //    WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"

    // "updateBCCustomer":
    //    UPDATE CUSTOMER
    //    SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ?
    //    WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
    //
    // "updateGCCustomer":
    //    UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?
    //    WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
    DbTable* customer_table = db->GetTable(tpcc::CUSTOMER_TABLE);
    DbRecord customer_record((char*)&result->c_val, sizeof(tpcc::customer_value_t),
                             customer_table->GetRecordSchema());
    customer_table->SearchRecordLocal(param->CustomerKey(), &customer_record);
    ASSERT(s, "TxnPaymentReplay Failed: Target record not located");
    result->c_val.c_ytd_payment += param->h_amount_;
    result->c_val.c_balance -= param->h_amount_;
    result->c_val.c_payment_cnt += 1;

    // Bad credit
    char* old_customer_data = result->c_val.c_data;
    if (strcmp(result->c_val.c_credit, "BC") == 0) {
        static const int HISTORY_SIZE = tpcc::CUSTOMER_DATA_SIZE_MAX;
        char history[HISTORY_SIZE];
        int characters = snprintf(history, HISTORY_SIZE, "(%d, %d, %d, %d, %d, %.2f)\n",
                                  param->customer_id_, param->c_d_id_, param->c_w_id_,
                                  param->district_id_, param->warehouse_id_, param->h_amount_);
        int current_keep = strlen(old_customer_data);
        if (current_keep + characters > tpcc::CUSTOMER_DATA_SIZE_MAX - 1) {
            current_keep = tpcc::CUSTOMER_DATA_SIZE_MAX - 1 - characters;
        }
        // Move the history payment history forward
        memmove(old_customer_data + characters, old_customer_data, current_keep);
        memcpy(old_customer_data, history, characters);
        old_customer_data[characters + current_keep] = '\0';
    }

    // "insertHistory":
    //    INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    DbTable* history_table = db->GetTable(tpcc::HISTORY_TABLE);
    DbRecord history_record((char*)&result->h_val, sizeof(tpcc::history_value_t),
                            history_table->GetRecordSchema());
    result->h_val.h_amount = param->h_amount_;
    result->h_val.h_date = param->h_date_;
    strcpy(result->h_val.h_data, result->w_val.w_name);
    strcat(result->h_val.h_data, "    ");
    strcat(result->h_val.h_data, result->d_val.d_name);

    // Commit this transaction:
    s = warehouse_table->UpdateRecordLocal(param->WarehouseKey(), warehouse_record, 0);
    ASSERT(s, "TxnPaymentReplay Failed: Update: target record not located");
    s = district_table->UpdateRecordLocal(param->DistrictKey(), district_record, 0);
    ASSERT(s, "TxnPaymentReplay Failed: Update: target record not located");
    s = customer_table->UpdateRecordLocal(param->CustomerKey(), customer_record, 0);
    ASSERT(s, "TxnPaymentReplay Failed: Update: target record not located");
    // s = history_table->InsertRecordLocal(param->HistoryKey(), history_record, 0);
    // ASSERT(s, "TxnPaymentReplay Failed: Insert target record not located");
    if (!s) {
        return false;
    }

    return true;
}

bool TpccBenchmark::TxnOrderStatusReplay(Db* db, tpcc::OrderStatusTxnParam* param,
                                         tpcc::OrderStatusTxnResult* result) {
    DbTable* customer_table = db->GetTable(tpcc::CUSTOMER_TABLE);
    DbTable* order_table = db->GetTable(tpcc::ORDER_TABLE);
    DbTable* orderline_table = db->GetTable(tpcc::ORDER_LINE_TABLE);

    tpcc::customer_value_t c_val;
    DbRecord customer_record((char*)&c_val, sizeof(tpcc::customer_value_t),
                             customer_table->GetRecordSchema());
    customer_table->SearchRecordLocal(param->CustomerKey(), &customer_record);
    result->c_balance = c_val.c_balance;

    tpcc::order_value_t o_val;
    DbRecord order_record((char*)&o_val, sizeof(tpcc::order_value_t),
                          order_table->GetRecordSchema());
    order_table->SearchRecordLocal(param->OrderKey(), &order_record);
    result->o_val = o_val;

    int ol_cnt = o_val.o_ol_cnt;
    for (int i = 0; i < ol_cnt; ++i) {
        DbRecord ol_record((char*)(&(result->ol_val[i])), sizeof(tpcc::order_line_value_t),
                           orderline_table->GetRecordSchema());
        orderline_table->SearchRecordLocal(param->OrderLineKey(i), &ol_record);
    }
    return true;
}

// This function replay the committed transactions in single-threaded
// mode, to check if the concurrent transaction execution results are
// correct
bool TpccBenchmark::Replay() {
    Db* replay_db = CreateDB(tpcc_config_);

    // Allocate memory to populate Tables:
    util::Timer timer;
    const size_t malloc_sz = 50ULL * 1024 * 1024 * 1024;
    char* d = (char*)malloc(malloc_sz);
    BufferManager bm(d, malloc_sz);

    // Populate the local records
    PopulateDatabaseRecords(&bm, replay_db, true);

    LOG_INFO("Replay: Populate Local Database Records Done: takes: %.2lf us\n", timer.us_elapse());

    // Step 1. Sort the committed transactions based on its timestamp
    timer.reset();
    std::vector<TpccTxnExecHistory> exec_hist;
    for (int i = 0; i < config_.thread_num; ++i) {
        exec_hist.insert(exec_hist.end(), exec_hist_[i].begin(), exec_hist_[i].end());
    }
    std::sort(exec_hist.begin(), exec_hist.end(), [&](auto a, auto b) {
        // We only consider committed transactions
        auto a_ts =
            a.committed ? a.results->GetCommitTimestamp() : std::numeric_limits<uint64_t>::max();
        auto b_ts =
            b.committed ? b.results->GetCommitTimestamp() : std::numeric_limits<uint64_t>::max();
        return a_ts < b_ts;
    });
    LOG_INFO("Replay: Sort execution results done, takes: %.2lf us\n", timer.us_elapse());

    std::ofstream of("replay_results.txt");
    uint64_t abort_ts = 0;
    int cmp_res, final_cmp_res;

    // Step2: Re-execute these transactions
    for (size_t i = 0; i < exec_hist.size(); ++i) {
        const auto& iter = exec_hist[i];
        // All committed transactions have been checked
        if (!iter.committed) {
            break;
        }
        switch (iter.type) {
            case tpcc::kNewOrder: {
                tpcc::NewOrderTxnResult replay_txn_result;
                TxnNewOrderReplay(replay_db, (tpcc::NewOrderTxnParam*)(iter.params),
                                  &replay_txn_result);
                int fail_idx = 0;
                cmp_res = replay_txn_result.Compare(iter.params, iter.results, &fail_idx);
                if (BenchConfig::DUMP_REPLAY_EXECUTION) {
                    DumpExecutionHist(of, &iter, &replay_txn_result);
                }
                if (cmp_res != 0) {
                    // of.close();
                    LOG_ERROR(
                        "Replay NewOrder transaction results failed, i = %d, cmp_res=%d, "
                        "fail_idx=%d",
                        i, cmp_res, fail_idx);
                    if (abort_ts == 0) {
                        abort_ts = iter.results->GetCommitTimestamp();
                    }
                    if (final_cmp_res == 0) {
                        final_cmp_res = cmp_res;
                    }
                    // LOG_FATAL("Replay NewOrder transaction results failed");
                }
                break;
            }
            case tpcc::kPayment: {
                tpcc::PaymentTxnResult replay_txn_result;
                bool s = TxnPaymentReplay(replay_db, (tpcc::PaymentTxnParam*)(iter.params),
                                          &replay_txn_result);
                // Compare the results
                cmp_res = replay_txn_result.Compare(iter.params, iter.results);
                if (BenchConfig::DUMP_REPLAY_EXECUTION) {
                    DumpExecutionHist(of, &iter, &replay_txn_result);
                }
                if (cmp_res != 0 || !s) {
                    of.close();
                    LOG_FATAL("Replay Payment transaction results failed, i = %d, cmp_res=%d", i,
                              cmp_res);
                    if (abort_ts == 0) {
                        abort_ts = iter.results->GetCommitTimestamp();
                    }
                    if (final_cmp_res == 0) {
                        final_cmp_res = cmp_res;
                    }
                    // LOG_FATAL("Replay Payment transaction results failed, cmp_res = %d",
                    // cmp_res);
                }
                break;
            }
            default:
                break;
        }
    }
    if (abort_ts != 0) {
        of << "Abort TS: " << abort_ts << "\n";
        of.close();
        LOG_FATAL("Replay transaction results failed, cmp_res = %d", final_cmp_res);
    }
    LOG_INFO("Successfully Pass the Replay Check");
    return true;
}

void TpccBenchmark::DumpExecutionHist(std::ofstream& of, const TpccTxnExecHistory* exec_record,
                                      TxnResult* replay_result) {
    switch (exec_record->type) {
        case tpcc::kPayment: {
            tpcc::PaymentTxnParam* param = (tpcc::PaymentTxnParam*)exec_record->params;
            tpcc::PaymentTxnResult* exec_result = (tpcc::PaymentTxnResult*)exec_record->results;
            tpcc::PaymentTxnResult* ref_exec_result = (tpcc::PaymentTxnResult*)replay_result;
            of << "\nTxnPaymentReplay [Meta]:\n"
               << "commit_ts: " << exec_record->results->GetCommitTimestamp() << "\n"
               << "txn_id: " << exec_record->results->GetTxnId() << "\n";
            // Dump the WarehouseKey() parameter into of:
            of << "[Parameters]:\n"
               << "district_key: " << param->DistrictKey() << ","
               << "ytd: " << std::fixed << std::setprecision(6) << param->h_amount_ << "\n";
#if DEBUG_PAYMENT
            // Dump the execution result, save 6 precision of w_ytd when dumping it into file
            of << "[Result]:\n"
               << "w_ytd: " << std::fixed << std::setprecision(6) << exec_result->w_val.w_ytd
               << "\n"
               << "rd_w_ytd: " << std::setprecision(6) << exec_result->rd_w_ytd << "\n"
               << "wr_w_ytd: " << std::setprecision(6) << exec_result->wr_w_ytd << "\n"
               << "d_ytd: " << std::fixed << std::setprecision(6) << exec_result->d_val.d_ytd
               << "\n"
               << "rd_d_ytd: " << std::fixed << std::setprecision(6) << exec_result->rd_d_ytd
               << "\n"
               << "wr_d_ytd: " << std::fixed << std::setprecision(6) << exec_result->wr_d_ytd
               << "\n";
            if (replay_result) {
                of << "[Ref Result]:\n"
                   << "d_ytd: " << std::fixed << std::setprecision(6)
                   << ref_exec_result->d_val.d_ytd << "\n";
            }
#endif

#if DEBUG_LOCALIZATION
            of << "district_key: " << param->DistrictKey() << "\n"
               << "txn_count: " << exec_result->txn_count << "\n"
               << "rw_txn_count: " << exec_result->rw_txn_count << "\n"
               << "unref_txn_count: " << exec_result->unref_txn_count << "\n"
               << "unref_rw_txn_count: " << exec_result->unref_rw_txn_count << "\n"
               << "pv_access_bitmap:" << std::bitset<64>(exec_result->pv_access_bm).to_string()
               << "\n"
               << "pv_rw_bitmap:" << std::bitset<64>(exec_result->pv_rw_bm).to_string() << "\n"
               << "dg_lock_bitmap:" << std::bitset<64>(exec_result->dg_lock_bm).to_string() << "\n"
               << "dg_commit_bitmap:" << std::bitset<64>(exec_result->dg_commit_bm).to_string()
               << "\n";
#endif

#if DEBUG_LOCALIZATION_PERF
            // for (int i = 0; i < 9; ++i) {
            //   of << "yield_next_cnt[" << i << "]: " << exec_result->yield_next_count[i] << "\n";
            // }
            // of << "yield_next_time: " << exec_result->yield_next_time << "\n";
#endif
            // Dump the table ids of dependent transactions:
            if (exec_result->cnt > 0) {
                of << "Dependent Table IDs: ";
                for (size_t i = 0; i < exec_result->cnt; ++i) {
                    of << exec_result->dependent_table_ids_[i] << ", ";
                }
                of << "\n";
                if (exec_result->wait_dependent_txn_time_ > 1000) {
                    of << "Wait Dependent Txn Time: " << exec_result->wait_dependent_txn_time_
                       << "\n";
                }
            }
            break;
        }
        case tpcc::kNewOrder: {
            tpcc::NewOrderTxnParam* param = (tpcc::NewOrderTxnParam*)exec_record->params;
            tpcc::NewOrderTxnResult* exec_result = (tpcc::NewOrderTxnResult*)exec_record->results;
            tpcc::NewOrderTxnResult* ref_exec_result = (tpcc::NewOrderTxnResult*)replay_result;
            of << "\nTxnNewOrderReplay [Meta]:\n"
               << "commit_ts: " << exec_record->results->GetCommitTimestamp() << "\n"
               << "txn_id: " << exec_record->results->GetTxnId() << "\n";
            of << "[Parameters]:\n"
               << "warehouse_key: " << param->WarehouseKey() << ","
               << "w_tax: " << std::fixed << std::setprecision(6) << exec_result->w_tax << "\n"
               << "district_key: " << param->DistrictKey() << ","
               << "d_next_o_id: " << std::fixed << std::setprecision(6) << exec_result->next_o_id
               << "\n"
               << "wr_d_ytd: " << std::fixed << std::setprecision(6) << exec_result->wr_d_ytd
               << "\n";

#if DEBUG_LOCALIZATION
            of << "txn_count: " << exec_result->txn_count << "\n"
               << "rw_txn_count: " << exec_result->rw_txn_count << "\n"
               << "unref_txn_count: " << exec_result->unref_txn_count << "\n"
               << "unref_rw_txn_count: " << exec_result->unref_rw_txn_count << "\n"
               << "pv_access_bitmap:" << std::bitset<64>(exec_result->pv_access_bm).to_string()
               << "\n"
               << "pv_rw_bitmap:" << std::bitset<64>(exec_result->pv_rw_bm).to_string() << "\n";
#endif

#if DEBUG_NEWORDER
            // Dump each stock value:
            for (int i = 0; i < param->ol_num_; ++i) {
                of << "idx = " << i << " , stock_key = " << param->StockKey(i) << "\n";
                of << "param quantity: " << param->quantities_[i] << "\n";
                of << "txn_count: " << exec_result->txn_counts[i]
                   << ",   unref_txn_count: " << exec_result->unref_txn_counts[i] << "\n";
                of << "exec quantity: " << exec_result->stock_vals[i].s_quantity << "\n";
                of << "ref quantity: " << ref_exec_result->stock_vals[i].s_quantity << "\n";
                of << "wr_quantity: " << exec_result->wr_quantity_vals[i] << "\n";
                of << "rd_quantity:" << exec_result->rd_quantity_vals[i] << "\n";
                of << "commit_bm: " << std::bitset<64>(exec_result->commit_bm[i]) << "\n";
            }
#endif

#if DEBUG_LOCALIZATION_PERF
            // for (int i = 0; i < 9; ++i) {
            //   of << "yield_next_cnt[" << i << "]: " << exec_result->yield_next_count[i] << "\n";
            // }
            // of << "yield_next_time: " << exec_result->yield_next_time << "\n";
#endif
            if (exec_result->cnt > 0) {
                of << "Dependent Table IDs: ";
                for (size_t i = 0; i < exec_result->cnt; ++i) {
                    of << exec_result->dependent_table_ids_[i] << ", ";
                }
                of << "\n";
                if (exec_result->wait_dependent_txn_time_ > 1000) {
                    of << "Wait Dependent Txn Time: " << exec_result->wait_dependent_txn_time_
                       << "\n";
                }
            }
            if (replay_result) {
                of << "[Ref Result]:\n"
                   << "d_next_oid: " << std::fixed << std::setprecision(6)
                   << ref_exec_result->next_o_id << "\n";
            }
            break;
        }
    }
    return;
}