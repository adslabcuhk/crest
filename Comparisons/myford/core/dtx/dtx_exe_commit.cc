// Author: Ming Zhang
// Copyright (c) 2022

#include "dtx/dtx.h"

bool DTX::TxExe(coro_yield_t &yield, bool fail_abort) {
  // Start executing transaction
  auto start = std::chrono::high_resolution_clock::now();
  tx_status = TXStatus::TX_EXE;
  if (read_write_set.empty() && read_only_set.empty()) {
    auto dura = std::chrono::high_resolution_clock::now() - start;
    // Accumulate the execution time
    exec_time +=
        std::chrono::duration_cast<std::chrono::microseconds>(dura).count();
    return true;
  }

  if (global_meta_man->txn_system == DTX_SYS::FORD) {
    // Run our system
    if (read_write_set.empty()) {
      if (ExeRO(yield)) {
        auto dura = std::chrono::high_resolution_clock::now() - start;
        exec_time +=
            std::chrono::duration_cast<std::chrono::microseconds>(dura).count();
        return true;
      } else {
        goto ABORT;
      }
    } else {
      if (ExeRW(yield)) {
        auto dura = std::chrono::high_resolution_clock::now() - start;
        exec_time +=
            std::chrono::duration_cast<std::chrono::microseconds>(dura).count();
        return true;
      } else {
        goto ABORT;
      }
    }
  } else if (global_meta_man->txn_system == DTX_SYS::FaRM ||
             global_meta_man->txn_system == DTX_SYS::DrTMH ||
             global_meta_man->txn_system == DTX_SYS::LOCAL) {
    if (read_write_set.empty()) {
      if (CompareExeRO(yield))
        return true;
      else
        goto ABORT;
    } else {
      if (CompareExeRW(yield))
        return true;
      else
        goto ABORT;
    }
  } else {
    RDMA_LOG(FATAL) << "NOT SUPPORT SYSTEM ID: " << global_meta_man->txn_system;
  }

  return true;

ABORT:
  if (fail_abort) {
    Abort();
    // coro_sched->Yield(yield, coro_id);
  }
  auto dura = std::chrono::high_resolution_clock::now() - start;
  exec_time +=
      std::chrono::duration_cast<std::chrono::microseconds>(dura).count();
  return false;
}

bool DTX::TxCommit(coro_yield_t &yield) {
  // Only read one item
  if (read_write_set.empty() && read_only_set.size() == 1) {
    return true;
  }

  bool commit_stat;

  /*!
    FORD's commit protocol
    */

  if (global_meta_man->txn_system == DTX_SYS::FORD) {
    auto start = std::chrono::high_resolution_clock::now();
    if (!Validate(yield)) {
      auto dura = std::chrono::high_resolution_clock::now() - start;
      validate_and_log_time +=
          std::chrono::duration_cast<std::chrono::microseconds>(dura).count();
      goto ABORT;
    }

    auto dura = std::chrono::high_resolution_clock::now() - start;
    validate_and_log_time +=
        std::chrono::duration_cast<std::chrono::microseconds>(dura).count();

    // Next step. If read-write txns, we need to commit the updates to remote
    // replicas
    if (!read_write_set.empty()) {
      // Write back for read-write tx
#if COMMIT_TOGETHER
      auto start = std::chrono::high_resolution_clock::now();
      commit_stat = CoalescentCommit(yield);
      commit_time += std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::high_resolution_clock::now() - start)
                         .count();
      if (commit_stat) {
        return true;
      } else {
        goto ABORT;
      }
#else
      commit_stat = CompareCommitBackup(yield);
      if (!commit_stat) {
        goto ABORT;
      }
      commit_stat = CompareCommitPrimary(yield);
      if (!commit_stat) {
        goto ABORT;
      }
      commit_stat = CompareTruncateAsync(yield);
      if (commit_stat) {
        return true;
      } else {
        goto ABORT;
      }
#endif
    }
  }

  if (global_meta_man->txn_system == DTX_SYS::LOCAL) {
    if (!read_write_set.empty()) {
      // For read-write txn
      if (!LocalLock())
        return false;
      if (!LocalValidate())
        return false;
      commit_stat = CoalescentCommit(yield);
      if (commit_stat) {
        return true;
      } else {
        abort();
      }
      LocalUnlock();
    } else {
      // For read-only txn
      if (!LocalValidate())
        return false;
    }
  }

  /*!
    DrTM+H's commit protocol
    */

  if (global_meta_man->txn_system == DTX_SYS::DrTMH) {
    // Lock and Validation are batched
    if (!CompareLockingValidation(yield)) {
      goto ABORT;
    }

    // Seperately commit backup and primary
    if (!read_write_set.empty()) {
      commit_stat = CompareCommitBackup(yield);
      if (!commit_stat) {
        goto ABORT;
      }
      commit_stat = CompareCommitPrimary(yield);
      if (!commit_stat) {
        goto ABORT;
      }
      commit_stat = CompareTruncateAsync(yield);
      if (commit_stat) {
        return true;
      } else {
        goto ABORT;
      }
    }
  }

  /*!
    FaRM's commit protocol
    */

  if (global_meta_man->txn_system == DTX_SYS::FaRM) {
    if (!CompareLocking(yield)) {
      goto ABORT;
    }
    if (!CompareValidation(yield)) {
      goto ABORT;
    }

    // Seperately commit backup and primary
    if (!read_write_set.empty()) {
      commit_stat = CompareCommitBackup(yield);
      if (!commit_stat) {
        goto ABORT;
      }
      commit_stat = CompareCommitPrimary(yield);
      if (!commit_stat) {
        goto ABORT;
      }
      commit_stat = CompareTruncateAsync(yield);
      if (commit_stat) {
        return true;
      } else {
        goto ABORT;
      }
    }
  }

  return true;
ABORT:
  Abort();
  return false;
}
