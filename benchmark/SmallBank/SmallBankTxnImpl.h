#pragma once

#include "SmallBankTxnStructs.h"
#include "common/Type.h"
#include "mempool/Coroutine.h"
#include "transaction/Txn.h"

namespace smallbank {

bool TxnAmalgamate(Txn* txn, TxnId txn_id, AmalgamateParam* params, AmalgamateResult* result,
                   coro_yield_t& yield, bool return_results = false);

bool TxnWriteCheck(Txn* txn, TxnId txn_id, WriteCheckParam* params, WriteCheckResult* result,
                   coro_yield_t& yield, bool return_results = false);

bool TxnTransactSaving(Txn* txn, TxnId txn_id, TransactSavingParam* params,
                       TransactSavingResult* result, coro_yield_t& yield,
                       bool return_results = false);

bool TxnSendPayment(Txn* txn, TxnId txn_id, SendPaymentParam* params, SendPaymentResult* result,
                    coro_yield_t& yield, bool return_results = false);

bool TxnDepositChecking(Txn* txn, TxnId txn_id, DepositCheckingParam* params,
                        DepositCheckingResult* result, coro_yield_t& yield,
                        bool return_results = false);

bool TxnBalance(Txn* txn, TxnId txn_id, BalanceParam* params, BalanceResult* result,
                coro_yield_t& yield, bool return_results = false);

};  // namespace smallbank