#include "SmallBank/SmallBankConstant.h"
#include "SmallBank/SmallBankTxnImpl.h"
#include "SmallBank/SmallBankTxnStructs.h"
#include "transaction/Enums.h"

namespace smallbank {
bool TxnDepositChecking(Txn* txn, TxnId txn_id, DepositCheckingParam* params,
                        DepositCheckingResult* result, coro_yield_t& yield, bool return_results) {
    txn->Begin(txn_id, TxnType::READ_WRITE, 0, "TxnDepositChecking");

    char name[ACCOUNTS_MAX_NAME_SIZE + 1];

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

    strcpy(name, cust_account_record->Get<char*>(ACCOUNTS_NAME));
    cust_account_record->finish();

    double old_checking_balance = cust_checking_record->Get<double>(CHECKINGS_BALANCE);
    double new_checking_balance = old_checking_balance + params->amount_;
    cust_checking_record->Write<double>(CHECKINGS_BALANCE, new_checking_balance);
    cust_checking_record->finish();

    s = txn->Commit(yield);
    if (!s.ok()) {
        return false;
    }

    return true;
}
};  // namespace smallbank