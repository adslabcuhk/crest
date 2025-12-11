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
bool TxnUpdate(Txn *txn, TxnId txn_id, UpdateTxnParam *params, UpdateTxnResult *result,
               coro_yield_t &yield, bool return_results) {
    txn->Begin(txn_id, TxnType::READ_WRITE, 0, "TxnUpdate");

    std::vector<TxnRecordRef> records;
    for (size_t i = 0; i < params->record_num; ++i) {
        RecordKey select_key = (RecordKey)params->records_keys_[i];
        TableId table_id = select_key % 2;
        TxnRecordRef record = txn->SelectRecord(table_id, select_key, AccessMode::READ_WRITE);
        record->add_rw_column(params->update_column_[i]);
        records.push_back(record);
    }

    Status s = txn->Deref(yield);
    if (!s.ok()) {
        return false;
    }

    // Do updating:
    for (size_t i = 0; i < params->record_num; ++i) {
        TxnRecordRef record = records[i];
        // std::string new_value = "seq:" + std::to_string(params->seq);
        // size_t new_value_size = std::min(ycsb::YCSB_COLUMN_SIZE - 1, new_value.size());
        // new_value = new_value.substr(0, new_value_size);
        // record->Write<char *>(params->update_column_[i], (char *)(new_value.c_str()));
        record->finish();
    }
    s = txn->Commit(yield);
    if (!s.ok()) {
        return false;
    }
    return true;
}
};  // namespace ycsb