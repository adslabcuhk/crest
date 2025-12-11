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
#include "transaction/TxnRecord.h"
#include "util/Logger.h"

namespace tatp {
bool TxnGetNewDestination(Txn *txn, TxnId txn_id, GetNewDestinationTxnParam *params,
                          GetNewDestinationTxnResult *result, coro_yield_t &yield,
                          bool return_results) {
    txn->Begin(txn_id, TxnType::READ_ONLY, 0, "TxnGetNewDestination");

    TxnRecordRef specfac_record = txn->SelectRecord(
        SPECIAL_FACILITY_TABLE, params->SpecialFacilityKey(), AccessMode::READ_ONLY);
    specfac_record->add_ro_columns({SF_IS_ACTIVE, SF_ERROR_CNTL, SF_DATA_A, SF_DATA_B});

    Status s = txn->Deref(yield);
    if (!s.ok()) {
        return false;
    }

    // If not active, abort this transaction
    bool is_active = specfac_record->Get<bool>(SF_IS_ACTIVE);
    if (!is_active) {
        txn->Abort(yield);
        return false;
    }
    specfac_record->finish();

    std::vector<TxnRecordRef> cf_records;
    for (int i = 0; i < params->fetch_num_; ++i) {
        TxnRecordRef callfwd_record = txn->SelectRecord(
            CALL_FORWARDING_TABLE, params->CallForwardingKey(i * 8), AccessMode::READ_ONLY);
        callfwd_record->add_ro_columns({CF_END_TIME, CF_NUMBERX});
        cf_records.push_back(callfwd_record);
    }

    s = txn->Deref(yield);
    if (!s.ok()) {
        return false;
    }

    bool callfwd_success = false;
    for (unsigned i = 0; i < params->fetch_num_; i++) {
        TxnRecordRef callfwd_record = cf_records[i];
        uint32_t start_time = i * 8;
        uint32_t end_time = callfwd_record->Get<uint32_t>(CF_END_TIME);
        if (start_time <= params->start_time_ && params->end_time_ < end_time) {
            callfwd_success = true;
        }
        callfwd_record->finish();
    }

    if (callfwd_success) {
        s = txn->Commit(yield);
        if (!s.ok()) {
            return false;
        }
    } else {
        txn->Abort(yield);
        return false;
    }
    return true;
}
};  // namespace tatp