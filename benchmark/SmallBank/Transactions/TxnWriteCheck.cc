#include <cstring>

#include "SmallBank/SmallBankConstant.h"
#include "SmallBank/SmallBankTxnImpl.h"
#include "SmallBank/SmallBankTxnStructs.h"
#include "common/Type.h"
#include "transaction/Enums.h"
#include "transaction/Txn.h"

namespace smallbank {
bool TxnWriteCheck(Txn* txn, TxnId txn_id, WriteCheckParam* params, WriteCheckResult* result,
                   coro_yield_t& yield, bool return_results) {
    txn->Begin(txn_id, TxnType::READ_WRITE, 0, "TxnWriteCheck");

    TxnRecordRef cust_account_record =
        txn->SelectRecord(ACCOUNTS_TABLE, params->custid_, AccessMode::READ_ONLY);
    cust_account_record->add_ro_column(ACCOUNTS_NAME);

    TxnRecordRef cust_checking_record =
        txn->SelectRecord(CHECKING_TABLE, params->custid_, AccessMode::READ_WRITE);
    cust_checking_record->add_rw_column(CHECKINGS_BALANCE);

    Status s = txn->Deref(yield);
    if (!s.ok()) {
        return false;
    }

    char name[ACCOUNTS_MAX_NAME_SIZE + 1];
    strcpy(name, cust_account_record->Get<char*>(ACCOUNTS_NAME));
    cust_account_record->finish();

    double old_checking_balance = cust_checking_record->Get<double>(CHECKINGS_BALANCE);
    cust_checking_record->finish();

    double old_saving_balance = 0;
    double balance = old_saving_balance + old_checking_balance;
    if (balance < params->amount_) {
        balance -= (params->amount_ + 1);
    } else {
        balance -= params->amount_;
    }

    s = txn->Commit(yield);
    if (!s.ok()) {
        return false;
    }

    if (return_results) {
        result->saving_balance = old_saving_balance;
        result->checking_balace = balance;
    }

    return true;
}
};  // namespace smallbank