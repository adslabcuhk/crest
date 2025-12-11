#pragma once
#include "TpccTxnStructs.h"
#include "common/Type.h"
#include "mempool/Coroutine.h"
#include "transaction/Txn.h"

namespace tpcc {

bool TxnDelivery(Txn* txn, TxnId txn_id, DeliveryTxnParam* params, DeliveryTxnResult* result,
                 coro_yield_t& yield, bool return_results = false);

bool TxnNewOrder(Txn* txn, TxnId txn_id, NewOrderTxnParam* params, NewOrderTxnResult* result,
                 coro_yield_t& yield, bool return_results = false);

bool TxnPayment(Txn* txn, TxnId txn_id, PaymentTxnParam* params, PaymentTxnResult* result,
                coro_yield_t& yield, bool return_results = false);

bool TxnOrderStatus(Txn* txn, TxnId txn_id, OrderStatusTxnParam* params,
                    OrderStatusTxnResult* results, coro_yield_t& yield,
                    bool return_results = false);

};  // namespace tpcc