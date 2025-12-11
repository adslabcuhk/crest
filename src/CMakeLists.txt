CMAKE_MINIMUM_REQUIRED(VERSION 3.8)

SET(SOURCE_LIST
  "rdma/Context.cc"
  "rdma/MemoryRegion.cc"
  "rdma/QueuePair.cc"
  "rdma/QueuePairFactory.cc"
  "rdma/RdmaBatch.cc"
  "mempool/Scheduler.cc"
  "mempool/Pool.cc"
  "mempool/BufferManager.cc"
  "db/AddressCache.cc"
  "db/DbRecord.cc"
  "db/Table.cc"
  "transaction/TxnRecord.cc"
  "transaction/LogManager.cc"
  "transaction/RecordHandle.cc"
  "transaction/TxnIO.cc"
  "transaction/TxnLocal.cc"
  "transaction/Txn.cc"
)

SET(CORO_DEP_LIBS boost_coroutine boost_context boost_system tbb)

SET(NETWORK_DEP_LIBS ibverbs)

FIND_PACKAGE(PkgConfig REQUIRED)
pkg_check_modules(ABSL_HASH_MAP REQUIRED absl_flat_hash_map)

ADD_LIBRARY(crest STATIC ${SOURCE_LIST})
TARGET_LINK_LIBRARIES(crest ${NETWORK_DEP_LIBS} ${CORO_DEP_LIBS} ${ABSL_HASH_MAP_LIBRARIES})
