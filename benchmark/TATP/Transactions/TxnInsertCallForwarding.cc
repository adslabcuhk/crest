#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <exception>

#include "TATP/TATPConstant.h"
#include "TATP/TATPTableStructs.h"
#include "TATP/TATPTxnImpl.h"
#include "TATP/TATPTxnStructs.h"
#include "transaction/Enums.h"
#include "transaction/Txn.h"
#include "util/Logger.h"

namespace tatp {
bool TxnInsertCallForwarding(Txn *txn, TxnId txn_id, InsertCallForwardingTxnParam *params,
                             InsertCallForwardingTxnResult *result, coro_yield_t &yield,
                             bool return_results) {
    txn->Begin(txn_id, TxnType::READ_WRITE, 0, "TxnInsertCallForwarding");

    auto sec_sub_record = txn->SelectRecord(SECONDARY_SUBSCRIBER_TABLE, params->SecSubscriberKey(),
                                            AccessMode::READ_ONLY);
    sec_sub_record->add_ro_column(SEC_SUB_SID);

    auto sub_record =
        txn->SelectRecord(SUBSCRIBER_TABLE, params->SubscriberKey(), AccessMode::READ_ONLY);
    sub_record->add_ro_columns({S_SUBNUM, S_HEX, S_BYTES, S_BITS, S_MSC_LOCATION, S_VLR_LOCATION});

    auto callfwd_record = txn->InsertRecord(CALL_FORWARDING_TABLE, params->CallForwardingKey());

    Status s = txn->Deref(yield);
    if (!s.ok()) {
        return false;
    }

    callfwd_val_t cf_val;
    cf_val.end_time = params->end_time_;
    cf_val.numberx[0] = '0';
    DbRecord callfwd_record_val((char *)&cf_val, sizeof(callfwd_val_t),
                                txn->GetRecordSchema(CALL_FORWARDING_TABLE));
    callfwd_record->Write(callfwd_record_val);

    sec_sub_record->finish();
    sub_record->finish();

    Status commit_status = txn->Commit(yield);
    return commit_status.ok();
}
};  // namespace tatp