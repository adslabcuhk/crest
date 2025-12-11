#include "SmallBank/SmallBankConstant.h"
#include "SmallBank/SmallBankTxnImpl.h"
#include "SmallBank/SmallBankTxnStructs.h"
#include "transaction/Enums.h"
#include "transaction/Txn.h"

namespace smallbank {

bool TxnSendPayment(Txn* txn, TxnId txn_id, SendPaymentParam* params, SendPaymentResult* result,
                    coro_yield_t& yield, bool return_results) {
    txn->Begin(txn_id, TxnType::READ_WRITE, 0, "TxnSendPayment");

    char name0[ACCOUNTS_MAX_NAME_SIZE + 1], name1[ACCOUNTS_MAX_NAME_SIZE + 1];

    TxnRecordRef cust0_account_record =
        txn->SelectRecord(ACCOUNTS_TABLE, params->custid_0_, AccessMode::READ_ONLY);
    cust0_account_record->add_ro_column(ACCOUNTS_NAME);

    TxnRecordRef cust1_account_record =
        txn->SelectRecord(ACCOUNTS_TABLE, params->custid_1_, AccessMode::READ_ONLY);
    cust1_account_record->add_ro_column(ACCOUNTS_NAME);

    TxnRecordRef cust0_saving_record =
        txn->SelectRecord(SAVINGS_TABLE, params->custid_0_, AccessMode::READ_WRITE);
    cust0_saving_record->add_rw_column(SAVINGS_BALANCE);

    TxnRecordRef cust1_saving_record =
        txn->SelectRecord(SAVINGS_TABLE, params->custid_1_, AccessMode::READ_WRITE);
    cust1_saving_record->add_rw_column(SAVINGS_BALANCE);

    Status s = txn->Deref(yield);
    if (!s.ok()) {
        return false;
    }
    strcpy(name0, cust0_account_record->Get<char*>(ACCOUNTS_NAME));
    cust0_account_record->finish();

    strcpy(name1, cust1_account_record->Get<char*>(ACCOUNTS_NAME));
    cust1_account_record->finish();

    // Transfer the money from one to the other:
    double old_cust0_account_bal = cust0_saving_record->Get<double>(SAVINGS_BALANCE);
    double old_cust1_account_bal = cust1_saving_record->Get<double>(SAVINGS_BALANCE);
    double new_cust0_account_bal = old_cust0_account_bal + params->amount_;
    double new_cust1_account_bal = old_cust1_account_bal - params->amount_;

    cust0_saving_record->Write<double>(SAVINGS_BALANCE, new_cust0_account_bal);
    cust1_saving_record->Write<double>(SAVINGS_BALANCE, new_cust1_account_bal);

    cust0_saving_record->finish();
    cust1_saving_record->finish();

    s = txn->Commit(yield);
    if (!s.ok()) {
        return false;
    }

    return true;
}
};  // namespace smallbank