#pragma once

#include "TATPTxnStructs.h"
#include "common/Type.h"
#include "mempool/Coroutine.h"
#include "transaction/Txn.h"

namespace tatp {
bool TxnGetSubscriberData(Txn* txn, TxnId txn_id, GetSubscriberTxnParam* params,
                          GetSubscriberTxnResult* result, coro_yield_t& yield,
                          bool return_results = false);

bool TxnGetNewDestination(Txn* txn, TxnId txn_id, GetNewDestinationTxnParam* params,
                          GetNewDestinationTxnResult* result, coro_yield_t& yield,
                          bool return_results = false);

bool TxnGetAccessData(Txn* txn, TxnId txn_id, GetAccessDataTxnParam* params,
                      GetAccessDataTxnResult* result, coro_yield_t& yield,
                      bool return_results = false);

bool TxnUpdateSubscriberData(Txn* txn, TxnId txn_id, UpdateSubscriberDataTxnParam* params,
                             UpdateSubscriberDataTxnResult* result, coro_yield_t& yield,
                             bool return_results = false);

bool TxnUpdateLocation(Txn* txn, TxnId txn_id, UpdateLocationTxnParam* params,
                       UpdateLocationTxnResult* result, coro_yield_t& yield,
                       bool return_results = false);

bool TxnInsertCallForwarding(Txn* txn, TxnId txn_id, InsertCallForwardingTxnParam* params,
                             InsertCallForwardingTxnResult* result, coro_yield_t& yield,
                             bool return_results = false);
};  // namespace tatp