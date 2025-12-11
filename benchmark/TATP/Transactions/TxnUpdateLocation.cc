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
bool TxnUpdateLocation(Txn *txn, TxnId txn_id, UpdateLocationTxnParam *params,
                       UpdateLocationTxnResult *result, coro_yield_t &yield, bool return_results) {
    txn->Begin(txn_id, TxnType::READ_WRITE, 0, "TxnUpdateLocation");
    uint32_t s_id = params->s_id_;
    uint32_t vlr_location = params->vlr_location_;

    auto sec_sub_record = txn->SelectRecord(SECONDARY_SUBSCRIBER_TABLE, params->SecSubsciberKey(),
                                            AccessMode::READ_ONLY);
    sec_sub_record->add_ro_column(SEC_SUB_SID);

    Status s = txn->Deref(yield);
    if (!s.ok()) {
        return false;
    }

    auto read_sid = sec_sub_record->Get<uint32_t>(SEC_SUB_SID);
    if (read_sid != s_id) {
        abort();
    }
    sec_sub_record->finish();

    auto sub_record =
        txn->SelectRecord(SUBSCRIBER_TABLE, (RecordKey)read_sid, AccessMode::READ_WRITE);
    sub_record->add_rw_column(S_VLR_LOCATION);

    s = txn->Deref(yield);
    if (!s.ok()) {
        return false;
    }
    sub_record->Write<uint32_t>(S_VLR_LOCATION, vlr_location);
    sub_record->finish();

    Status commit_status = txn->Commit(yield);
    return commit_status.ok();
}
}  // namespace tatp