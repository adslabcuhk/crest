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
bool TxnGetSubscriberData(Txn *txn, TxnId txn_id, GetSubscriberTxnParam *params,
                          GetSubscriberTxnResult *result, coro_yield_t &yield,
                          bool return_results) {
    txn->Begin(txn_id, TxnType::READ_ONLY, 0, "TxnGetSubscriberData");
    TxnRecordRef subscriber_record =
        txn->SelectRecord(SUBSCRIBER_TABLE, params->SubscriberKey(), AccessMode::READ_ONLY);
    subscriber_record->add_ro_columns(
        {S_SUBNUM, S_HEX, S_BYTES, S_BITS, S_MSC_LOCATION, S_VLR_LOCATION});

    Status s = txn->Deref(yield);
    if (!s.ok()) {
        return false;
    }
    subscriber_record->finish();
    s = txn->Commit(yield);
    if (!s.ok()) {
        return false;
    }
    return true;
}
};  // namespace tatp