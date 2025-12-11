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
bool TxnUpdateSubscriberData(Txn *txn, TxnId txn_id, UpdateSubscriberDataTxnParam *params,
                             UpdateSubscriberDataTxnResult *result, coro_yield_t &yield,
                             bool return_results) {
    txn->Begin(txn_id, TxnType::READ_WRITE, 0, "TxnUpdateSubscriberData");

    uint32_t s_id = params->s_id_;
    uint8_t sf_type = params->sf_type_;

    TxnRecordRef subscriber_record =
        txn->SelectRecord(SUBSCRIBER_TABLE, params->SubscriberKey(), AccessMode::READ_WRITE);
    subscriber_record->add_rw_column(S_BITS);

    TxnRecordRef specfac_record = txn->SelectRecord(
        SPECIAL_FACILITY_TABLE, params->SpecialFacilityKey(), AccessMode::READ_WRITE);
    specfac_record->add_rw_column(SF_DATA_A);

    Status s = txn->Deref(yield);
    if (!s.ok()) {
        return false;
    }

    // Update the subscriber record
    subscriber_record->Write<uint32_t>(S_BITS, params->s_val.bits);
    // Update the special facility record
    specfac_record->Write<char>(SF_DATA_A, 'a');
    subscriber_record->finish();
    specfac_record->finish();

    Status commit_status = txn->Commit(yield);
    return commit_status.ok();
}
}  // namespace tatp