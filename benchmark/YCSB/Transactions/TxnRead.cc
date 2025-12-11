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
bool TxnRead(Txn *txn, TxnId txn_id, ReadTxnParam *params, ReadTxnResult *result,
             coro_yield_t &yield, bool return_results) {
    txn->Begin(txn_id, TxnType::READ_ONLY, 0, "TxnRead");

    std::vector<TxnRecordRef> records;
    uint64_t ro_bms = 0;
    for (size_t i = 0; i < YCSB_COLUMN_NUM; ++i) {
        ro_bms |= (1 << (63 - i));
    }

    for (size_t i = 0; i < params->record_num; ++i) {
        RecordKey select_key = (RecordKey)params->records_keys_[i];
        TableId table_id = select_key % 2;
        TxnRecordRef record = txn->SelectRecord(table_id, select_key, AccessMode::READ_ONLY);
        record->add_ro_columns(ro_bms);
        records.push_back(record);
    }

    Status s = txn->Deref(yield);
    if (!s.ok()) {
        return false;
    }

    // Do reading:
    for (size_t i = 0; i < params->record_num; ++i) {
        TxnRecordRef record = records[i];
        record->finish();
    }
    s = txn->Commit(yield);
    if (!s.ok()) {
        return false;
    }
    return true;
}
}  // namespace ycsb