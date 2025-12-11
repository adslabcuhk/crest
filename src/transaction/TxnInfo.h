#pragma once
#include <infiniband/verbs.h>

#include <bitset>
#include <boost/unordered/concurrent_flat_map.hpp>
#include <boost/unordered/concurrent_flat_map_fwd.hpp>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <vector>

#include "common/Config.h"
#include "common/Type.h"
#include "db/Db.h"
#include "db/Format.h"
#include "db/Schema.h"
#include "mempool/Coroutine.h"
#include "rdma/QueuePair.h"
#include "transaction/Bitset.h"
#include "transaction/Enums.h"
#include "transaction/TxnConfig.h"  // Add this line to include the TxnConfig declaration
#include "util/Lock.h"
#include "util/Logger.h"
#include "util/Macros.h"

struct TxnInfo {
  TxnId txn_id;
  TxnStatus txn_status;
  timestamp_t commit_ts;  // The commit timestamp of this transaction
  std::string txn_name;   // For debug

  TxnInfo(TxnId txn_id, const std::string& name = "")
      : txn_id(txn_id), txn_status(TxnStatus::BEGIN), commit_ts(BASE_TIMESTAMP), txn_name(name) {}

  // The value assignment of the `txn_status' attribute is a synchronization
  // point
  TxnStatus get_txn_status() const { return ATOMIC_LOAD(&txn_status); }
  void set_txn_status(TxnStatus s) { ATOMIC_STORE(&txn_status, s); }

  // No need to use atomic instructions for getting and setting the commit
  // timestamp, as set_commit_ts() happens before set_txn_status().
  timestamp_t get_commit_ts() const { return ATOMIC_LOAD(&commit_ts); }
  void set_commit_ts(timestamp_t ts) { ATOMIC_STORE(&commit_ts, ts); }
};