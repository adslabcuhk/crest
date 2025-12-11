#pragma once

#include <bitset>
#include <cstdint>
#include <cstdlib>
#include <vector>

#include "common/Config.h"
#include "common/Type.h"
#include "db/Schema.h"
#include "db/ValueType.h"
#include "mempool/Coroutine.h"
#include "util/Macros.h"
#include "util/Status.h"

//
// This file defines the classes used for transaction interface
//
//  See txn_example.txt for more information
//

using namespace util;
using namespace coro;

enum RecordAccessStatus {
  INIT = 0,     // The init status
  READ_BUCKET,  // An RDMA operation fetching the bucket has been issued
  READ_RECORD,  // An RDMA operation fetching the record has been issued
  READ_DONE,    // This record is safely retrived from MemPool
  LOCK_FAIL,
};

enum TxnAccessStatus {
  BEGIN = 0,                 // Nothing is done
  ACCESS_BEFORE_VALIDATION,  // This record requires validation at commit time
  ACCESS_AT_VALIDATION,      // This record is read at validation time
  NOT_EXIST,                 // This record does exist at all
  COMMITABLE,                // The value of this handle can be committed
};

enum class TxnStatus : int {
  BEGIN = 0,
  READBUCKET,
  LOCALIZATION,
  VALIDATING,
  COMMITABLE,
  COMMITTED,
  ABORT,
};

enum class AccessMode {
  NONE,        // Default Value, meaning not access
  READ_ONLY,   // This record is only being read
  READ_WRITE,  // Update this record and returns its original value
  WRITE,       // Update this record without returning previous value
  INSERT,      // Insert this record
  DELETE,      // Delete this record
};

enum class CheckStatus {
  READY = 1,  // The data of all accessed cells are ready
  RETRY,      // Some data is not presented
  ABORT,      // The check is aborted, maybe due to failed remote lock
};

enum class LockStatus : uint64_t {
  NO_LOCK = 1,
  LOCKING,
  SUCC_LOCK,
  FAIL_LOCK,
  RELEASING,
};

static_assert(sizeof(LockStatus) >= WORD_SIZE, "LockStatus size not big enough");

enum class RdmaStatus : uint64_t {
  INIT = 1,
  EXECUTING,
  FINISHED,
};

enum class LogStatus : uint64_t {
  DOING = 1,
  COMMITTED,
  ABORTED,
};

static const char* LogStatusString[4] = {"", "DOING", "COMMITTED", "ABORTED"};

enum class LockMode {
  RO = 1,
  RW,
};

inline bool HasWrite(AccessMode mode) {
  // We have special processing logic for the INSERT operation
  return mode == AccessMode::READ_WRITE || mode == AccessMode::WRITE;
}

inline bool HasRead(AccessMode mode) {
  // We have special processing logic for the DELETE operation
  return mode == AccessMode::READ_WRITE || mode == AccessMode::READ_ONLY;
}

enum class TxnType {
  READ_ONLY = 0,
  READ_WRITE,
};

enum class CheckResult {
  SUCC = 0,
  LOCK_BY_LOCAL,
  LOCK_BY_REMOTE,
};
