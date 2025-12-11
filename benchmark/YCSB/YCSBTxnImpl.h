#pragma once

#include "YCSBTxnStructs.h"
#include "common/Type.h"
#include "mempool/Coroutine.h"
#include "transaction/Txn.h"

namespace ycsb {
bool TxnUpdate(Txn* txn, TxnId txn_id, UpdateTxnParam* params, UpdateTxnResult* result,
               coro_yield_t& yield, bool return_results = false);

bool TxnRead(Txn* txn, TxnId txn_id, ReadTxnParam* params, ReadTxnResult* result,
             coro_yield_t& yield, bool return_results = false);

bool TxnInsert(Txn* txn, TxnId txn_id, InsertTxnParam* params, InsertTxnResult* result,
               coro_yield_t& yield, bool return_results = false);
};  // namespace ycsb