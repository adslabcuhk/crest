CMAKE_MINIMUM_REQUIRED(VERSION 3.8)

SET(BENCHMAKE_SOURCE_LIST

  # General
  "Base/Benchmark.cc"

  # TPCC
  "TPCC/TpccBenchmarkExecutor.cc"
  "TPCC/TpccBenchmarkPopulator.cc"
  "TPCC/TpccBenchmarkReplayer.cc"
  "TPCC/Transactions/TxnNewOrder.cc"
  "TPCC/Transactions/TxnPayment.cc"
  "TPCC/Transactions/TxnDelivery.cc"
  "TPCC/Transactions/TxnOrderStatus.cc"

  # SmallBank
  "SmallBank/SmallBankBenchmarkExecutor.cc"
  "SmallBank/SmallBankBenchmarkPopulator.cc"
  "SmallBank/SmallBankBenchmarkReplayer.cc"
  "SmallBank/Transactions/TxnAmalgamate.cc"
  "SmallBank/Transactions/TxnWriteCheck.cc"
  "SmallBank/Transactions/TxnTransactSaving.cc"
  "SmallBank/Transactions/TxnSendPayment.cc"
  "SmallBank/Transactions/TxnDepositChecking.cc"
  "SmallBank/Transactions/TxnBalance.cc"

  # YCSB
  "YCSB/YCSBBenchmarkExecutor.cc"
  "YCSB/YCSBBenchmarkPopulator.cc"
  "YCSB/YCSBBenchmarkReplayer.cc"
  "YCSB/Transactions/TxnUpdate.cc"
  "YCSB/Transactions/TxnRead.cc"
  "YCSB/Transactions/TxnInsert.cc"

  # TATP
  "TATP/TATPBenchmarkExecutor.cc"
  "TATP/TATPBenchmarkPopulator.cc"
  "TATP/TATPBenchmarkReplayer.cc"
  "TATP/Transactions/TxnGetSubscriber.cc"
  "TATP/Transactions/TxnGetNewDestination.cc"
  "TATP/Transactions/TxnGetAccessData.cc"
  "TATP/Transactions/TxnUpdateSubscriber.cc"
  "TATP/Transactions/TxnUpdateLocation.cc"
  "TATP/Transactions/TxnInsertCallForwarding.cc"
)

ADD_LIBRARY(benchmark ${BENCHMAKE_SOURCE_LIST})
TARGET_LINK_LIBRARIES(benchmark crest)

ADD_EXECUTABLE(bench_runner BenchRunner.cc)
TARGET_LINK_LIBRARIES(bench_runner benchmark crest gflags pthread memcached)
