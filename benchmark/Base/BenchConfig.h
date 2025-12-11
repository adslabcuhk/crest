#pragma once
#include <cstdlib>

class BenchConfig {
 public:
  static constexpr bool DUMP_REPLAY_EXECUTION = true;

  static constexpr bool ENABLE_DETAILED_PERF = true;

  static constexpr bool WORKLOADS_PARTITION = false;

  static constexpr size_t MAX_EXEC_COUNT = 3;

  static constexpr size_t TPCC_MAX_EXEC_COUNT = 3;

  static constexpr size_t SMALLBANK_MAX_EXEC_COUNT = 10;

  static constexpr size_t YCSB_MAX_EXEC_COUNT = 10;

  static constexpr size_t TATP_MAX_EXEC_COUNT = 10;

  static constexpr bool USE_RANDOM_SEED = false;
};