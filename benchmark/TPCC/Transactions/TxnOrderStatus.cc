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
#include "util/Timer.h"

namespace tpcc {

bool TxnOrderStatus(Txn* txn, TxnId txn_id, OrderStatusTxnParam* params,
                    OrderStatusTxnResult* result, coro_yield_t& yield, bool return_results) {
    // OrderStatus is a read-only transaction
    txn->Begin(txn_id, TxnType::READ_ONLY, 0, "TxnOrderStatus");

    TxnRecordRef customer_record =
        txn->SelectRecord(CUSTOMER_TABLE, params->CustomerKey(), AccessMode::READ_ONLY);
    customer_record->add_ro_columns({C_BALANCE, C_FIRST, C_MIDDLE, C_LAST});

    TxnRecordRef order_record =
        txn->SelectRecord(ORDER_TABLE, params->OrderKey(), AccessMode::READ_ONLY);
    order_record->add_ro_columns({O_ENTRY_D, O_CARRIER_ID, O_OL_CNT});

    Status s = txn->Deref(yield);
    if (!s.ok()) {
        return false;
    }
    double c_balance = customer_record->Get<double>(C_BALANCE);
    customer_record->finish();

    int ol_cnt = order_record->Get<int32_t>(O_OL_CNT);
    ASSERT(ol_cnt < MAX_OL_CNT, "Invalid orderline count value");
    order_record->finish();

    std::vector<TxnRecordRef> orderline_records;
    for (int i = 0; i < ol_cnt; ++i) {
        TxnRecordRef orderline_record =
            txn->SelectRecord(ORDER_LINE_TABLE, params->OrderLineKey(i), AccessMode::READ_ONLY);
        orderline_record->add_ro_columns(
            {OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_AMOUNT, OL_QUANTITY});
        orderline_records.push_back(orderline_record);
    }

    s = txn->Deref(yield);
    if (!s.ok()) {
        return false;
    }

    for (int i = 0; i < ol_cnt; ++i) {
        orderline_records[i]->finish();
    }

    s = txn->Commit(yield);
    if (!s.ok()) {
        return false;
    }

    if (return_results) {
        // Return the transaction execution results to client
        result->SetCommitTimestamp(txn->GetCommitTimestamp());
        result->c_balance = customer_record->Get<double>(C_BALANCE);
        order_record->Get((char*)(&result->o_val));
        for (int i = 0; i < ol_cnt; ++i) {
            orderline_records[i]->Get((char*)(&result->ol_val[i]));
        }
    }

    return true;
}
}  // namespace tpcc