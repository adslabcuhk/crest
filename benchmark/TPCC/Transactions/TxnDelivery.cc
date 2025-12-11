#include <cstdint>

#include "TPCC/TpccConstant.h"
#include "TPCC/TpccTxnImpl.h"
#include "common/Type.h"
#include "mempool/Coroutine.h"
#include "transaction/Enums.h"

namespace tpcc {
bool TxnDelivery(Txn* txn, TxnId txn_id, DeliveryTxnParam* params, DeliveryTxnResult* result,
                 coro_yield_t& yield, bool return_results) {
    txn->Begin(txn_id, TxnType::READ_WRITE, 0, "TxnDelivery");
    std::vector<TxnRecordRef> new_order_records;
    std::vector<TxnRecordRef> order_records;
    std::vector<TxnRecordRef> orderline_records;
    for (int d_id = 0; d_id < params->ctx_->config.num_district_per_warehouse; ++d_id) {
        RecordKey order_key =
            params->ctx_->MakeOrderKey(params->warehouse_id_, d_id, params->o_id_);
        RecordKey neworder_key =
            params->ctx_->MakeNewOrderKey(params->warehouse_id_, d_id, params->o_id_);
        TxnRecordRef new_order_record =
            txn->SelectRecord(NEW_ORDER_TABLE, neworder_key, AccessMode::READ_WRITE);
        TxnRecordRef order_record =
            txn->SelectRecord(ORDER_TABLE, order_key, AccessMode::READ_WRITE);
        new_order_records.push_back(new_order_record);
        order_records.push_back(order_record);
        for (int ol = 0; ol < tpcc::MAX_OL_CNT; ++ol) {
            RecordKey orderline_key =
                params->ctx_->MakeOrderLineKey(params->warehouse_id_, d_id, params->o_id_, ol);
            TxnRecordRef orderline_record =
                txn->SelectRecord(ORDER_LINE_TABLE, orderline_key, AccessMode::READ_WRITE);
            orderline_record->add_rw_column(OL_DELIVERY_D);
            orderline_records.push_back(orderline_record);
        }
    }
    Status s = txn->Deref(yield);
    if (!s.ok()) {
        return false;
    }

    int32_t sum_ol_amount = 0;
    for (size_t i = 0; i < orderline_records.size(); ++i) {
        TxnRecordRef ol_record = orderline_records[i];
        ol_record->Write<int32_t>(OL_DELIVERY_D, params->ol_delivery_d_);
        sum_ol_amount += ol_record->Get<int32_t>(OL_AMOUNT);
        ol_record->finish();
    }

    for (size_t i = 0; i < new_order_records.size(); ++i) {
        TxnRecordRef new_order_record = new_order_records[i];
        new_order_record->finish();
    }

    for (size_t i = 0; i < order_records.size(); ++i) {
        TxnRecordRef order_record = order_records[i];
        order_record->finish();
    }

    std::vector<TxnRecordRef> customer_records;
    for (int d_id = 0; d_id < params->ctx_->config.num_district_per_warehouse; ++d_id) {
        RecordKey cust_key =
            params->ctx_->MakeCustomerKey(params->warehouse_id_, d_id, params->o_id_);
        TxnRecordRef customer_record =
            txn->SelectRecord(CUSTOMER_TABLE, cust_key, AccessMode::READ_WRITE);
        customer_record->add_rw_column(C_BALANCE);
        customer_record->add_rw_column(C_DELIVERY_CNT);
        customer_records.push_back(customer_record);
    }

    s = txn->Deref(yield);
    if (!s.ok()) {
        return false;
    }
    for (size_t i = 0; i < customer_records.size(); ++i) {
        TxnRecordRef customer_record = customer_records[i];
        int32_t balance = customer_record->Get<int32_t>(C_BALANCE);
        balance += sum_ol_amount;
        customer_record->Write<int32_t>(C_BALANCE, balance);
        int32_t delivery_cnt = customer_record->Get<int32_t>(C_DELIVERY_CNT);
        delivery_cnt += 1;
        customer_record->Write<int32_t>(C_DELIVERY_CNT, delivery_cnt);
        customer_record->finish();
    }

    s = txn->Commit(yield);
    if (!s.ok()) {
        return false;
    }

    return true;
}
};  // namespace tpcc