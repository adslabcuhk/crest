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
bool TxnGetAccessData(Txn *txn, TxnId txn_id, GetAccessDataTxnParam *params,
                      GetAccessDataTxnResult *result, coro_yield_t &yield, bool return_results) {
    txn->Begin(txn_id, TxnType::READ_ONLY, 0, "TxnGetAccessData");
    TxnRecordRef accessinfo_record =
        txn->SelectRecord(ACCESSINFO_TABLE, params->AccessInfoKey(), AccessMode::READ_ONLY);
    accessinfo_record->add_ro_columns({AI_DATA1, AI_DATA2, AI_DATA3, AI_DATA4});

    Status s = txn->Deref(yield);
    if (!s.ok()) {
        return false;
    }

    accessinfo_record->finish();

    s = txn->Commit(yield);
    if (!s.ok()) {
        return false;
    }
    return true;
}
}  // namespace tatp