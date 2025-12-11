#pragma once

struct TxnConfig {
  static constexpr bool ENABLE_LOCAL_EXECUTION = true;

  static constexpr bool ENABLE_REDO_LOG = true;

  // 4 times retry should be enough for most cases as a transaction only takes
  // 3~4 RTTs to commit in our system
  static constexpr int MAX_EXECUTE_COUNT = 2;

  static constexpr bool UPDATE_ADDRESS_CACHE_WHEN_CHECK_BKT = false;

  static constexpr bool ENABLE_CN_CACHE = true;
};