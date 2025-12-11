#include "SmallBank/SmallBankConstant.h"
#include "SmallBank/SmallBankTxnImpl.h"
#include "SmallBank/SmallBankTxnStructs.h"
#include "common/Type.h"
#include "transaction/Enums.h"
#include "transaction/Txn.h"

namespace smallbank {

bool TxnTransactSaving(Txn* txn, TxnId txn_id, TransactSavingParam* params,
                       TransactSavingResult* result, coro_yield_t& yield, bool return_results) {
    txn->Begin(txn_id, TxnType::READ_WRITE, 0, "TxnTransactSaving");

    char name[ACCOUNTS_MAX_NAME_SIZE + 1];

    TxnRecordRef cust_account_record =
        txn->SelectRecord(ACCOUNTS_TABLE, params->custid_, AccessMode::READ_ONLY);
    cust_account_record->add_ro_column(ACCOUNTS_NAME);

    TxnRecordRef saving_record =
        txn->SelectRecord(SAVINGS_TABLE, params->custid_, AccessMode::READ_WRITE);
    saving_record->add_rw_column(SAVINGS_BALANCE);
    Status s = txn->Deref(yield);
    if (!s.ok()) {
        return false;
    }

    strcpy(name, cust_account_record->Get<char*>(ACCOUNTS_NAME));
    cust_account_record->finish();

    double old_saving_balance = saving_record->Get<double>(SAVINGS_BALANCE);
    double new_saving_balance = old_saving_balance - params->amount_;
    saving_record->Write<double>(SAVINGS_BALANCE, new_saving_balance);
    saving_record->finish();

    s = txn->Commit(yield);
    if (!s.ok()) {
        return false;
    }

    if (return_results) {
        result->saving_balance = new_saving_balance;
    }

    return true;
}

}  // namespace smallbank