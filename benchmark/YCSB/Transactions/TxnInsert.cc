#include <infiniband/verbs.h>

#include <cstring>
#include <string>

#include "YCSB/YCSBConstant.h"
#include "YCSB/YCSBTxnImpl.h"
#include "common/Type.h"
#include "transaction/Enums.h"
#include "transaction/Txn.h"
#include "transaction/TxnRecord.h"

namespace ycsb {
bool TxnInsert(Txn *txn, TxnId txn_id, InsertTxnParam *params, InsertTxnResult *result,
               coro_yield_t &yield, bool return_results) {
    txn->Begin(txn_id, TxnType::READ_WRITE, 0, "TxnInsert");
    return true;
}
};  // namespace ycsb