#include <infiniband/verbs.h>

#include <locale>

#include "SmallBank/SmallBankConstant.h"
#include "SmallBank/SmallBankTxnImpl.h"
#include "common/Type.h"
#include "transaction/Enums.h"
#include "transaction/Txn.h"

namespace smallbank {

bool TxnBalance(Txn* txn, TxnId txn_id, BalanceParam* params, BalanceResult* result,
                coro_yield_t& yield, bool return_results) {
    txn->Begin(txn_id, TxnType::READ_ONLY, 0, "TxnBalance");
    TxnRecordRef account_record =
        txn->SelectRecord(ACCOUNTS_TABLE, params->custid_, AccessMode::READ_ONLY);
    TxnRecordRef saving_record =
        txn->SelectRecord(SAVINGS_TABLE, params->custid_, AccessMode::READ_ONLY);
    TxnRecordRef checking_record =
        txn->SelectRecord(CHECKING_TABLE, params->custid_, AccessMode::READ_ONLY);

    account_record->add_ro_column(ACCOUNTS_NAME);
    saving_record->add_ro_column(SAVINGS_BALANCE);
    checking_record->add_ro_column(CHECKINGS_BALANCE);

    Status s = txn->Deref(yield);
    if (!s.ok()) {
        return false;
    }

    double saving_balance = saving_record->Get<double>(SAVINGS_BALANCE);
    double checking_balace = checking_record->Get<double>(CHECKINGS_BALANCE);
    account_record->finish();
    saving_record->finish();
    checking_record->finish();

    s = txn->Commit(yield);
    if (!s.ok()) {
        return false;
    }

    if (return_results) {
    }
    return true;
}

};  // namespace smallbank