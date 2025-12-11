#pragma once

#include <infiniband/verbs.h>

#include <utility>
#include <vector>

#include "common/Config.h"
#include "common/Type.h"
#include "mempool/Coroutine.h"
#include "mempool/PoolMeta.h"
#include "mempool/Scheduler.h"
#include "rdma/Context.h"
#include "rdma/Doorbell.h"
#include "rdma/MemoryRegion.h"
#include "rdma/QueuePair.h"
#include "rdma/RdmaBatch.h"
#include "util/Statistic.h"
#include "util/Status.h"

using namespace rdma;
using namespace util;
using namespace coro;

struct DatabaseMeta {
  static const uint64_t address_count = 4096 / sizeof(PoolPtr);
  PoolPtr table_address[address_count];
};
static_assert(sizeof(DatabaseMeta) == 4096, "DatabaseMeta size not match");

// A Pool manages all memory nodes and abstract them as a memory pool. We do not
// consider CN-to-CN connection currently (which might be used for cache
// coherence). A Pool provides the following functionalities:
//  * Manages the metadata information of each memory node, including memory
//  usage, etc.
//  * Manages the RDMA queue pair to enable two/one-sided access
//  * Allows CN threads to acquire memory on memory nodes.
// If the PoolManager is created on Memory nodes, it has to only complete memory
// initialization tasks and wait for incomming connection requests.
class Pool {
  static constexpr size_t kPoolManagerMetaBufferSize = 1ULL << 20;

  static constexpr size_t kAcquireChunkSize = 128 * (1ULL << 20);

  friend class CoroutineScheduler;

 public:
  Pool(NodeInitAttr init_attr, Statistics *statistic = nullptr)
      : node_info_(init_attr),
        ctx_(nullptr),
        mr_(nullptr),
        mr_owner_(false),
        sched_(nullptr),
        statistic_(statistic) {
    qps_.reserve(MAX_MEMORY_NODE_NUM);
  }

  ~Pool() {
    if (mr_owner_) {
      delete mr_;
    }
  }

  // Create the memory region on this node according to the input NodeInitAttr
  // struct.
  Status CreateLocalMemoryRegion();

  // Compute Nodes run the BuildConnection to connect all memory nodes
  // Memory Nodes run the BuildConnection to wait for CNs' incomming connection requests
  Status BuildConnection(const std::vector<RemoteNodeAttr> &contact, int qp_per_node);

  Status BuildConnection(rdma::Context *ctx, rdma::MemoryRegion *mr,
                         const std::vector<RemoteNodeAttr> &contact, int qp_per_node);

  void SetCoroSched(CoroutineScheduler *sched) {
    sched_ = sched;
    sched_->SetPool(this);
  }

  auto GetCoroSched() { return sched_; }

  Status AcquireMemory(size_t sz, node_id_t nid, PoolPtr *ptr);

  QueuePair *GetQueuePair(node_id_t nid) { return qps_[nid]; }

  // const std::vector<QueuePair *> &GetAllQueuePair() const { return qps_; }
  size_t GetQueueNum() const { return qps_.size(); }

  MemoryRegionToken GetLocalMemoryRegionToken() const { return mr_->GetMemoryRegionToken(); }

  std::pair<char *, size_t> GetLocalDataRegion() const {
    auto mr_token = mr_->GetMemoryRegionToken();
    return std::make_pair<char *, size_t>((char *)mr_token.get_region_addr(),
                                          mr_token.get_region_size());
  }

  node_id_t MyId() const { return node_info_.nid; }

  bool IsMN() const { return node_info_.node_type == kMN; }

  bool IsCN() const { return node_info_.node_type == kCN; }

  node_id_t GetNodeId() const { return node_info_.nid; }

  Status PostRead(coro_id_t coro_id, PoolPtr raddr, size_t sz, void *laddr,
                  int flags = IBVFlags::SIGNAL()) {
    return sched_->PostRead(coro_id, NodeId(raddr), GetQueuePair(NodeId(raddr)),
                            (void *)NodeAddress(raddr), sz, laddr, flags);
  }

  Status PostBatch(coro_id_t coro_id, RDMABatch *batch, node_id_t nid,
                   int flags = IBVFlags::SIGNAL()) {
    return sched_->PostBatch(coro_id, nid, batch, GetQueuePair(nid), flags);
  }

  Status PostWriteBatch(coro_id_t coro_id, WriteRecordBatch *write_batch) {
    node_id_t nid = write_batch->nid;
    QueuePair *qp = GetQueuePair(nid);
    return sched_->PostWriteBatch(coro_id, write_batch, qp);
  }

  Status PostReadLockBatch(coro_id_t coro_id, LockReadBatch *lock_read_batch) {
    node_id_t nid = lock_read_batch->nid;
    QueuePair *qp = GetQueuePair(nid);
    return sched_->PostLockReadBatch(coro_id, lock_read_batch, qp);
  }

  Status PostWrite(coro_id_t coro_id, void *laddr, size_t sz, PoolPtr raddr,
                   int flags = IBVFlags::SIGNAL()) {
    ASSERT(sz != 64, "Write size should not be 64");
    return sched_->PostWrite(coro_id, NodeId(raddr), GetQueuePair(NodeId(raddr)), laddr, sz,
                             (void *)NodeAddress(raddr), flags);
  }

  Status PostCAS(coro_id_t coro_id, PoolPtr raddr, void *laddr, uint64_t compare_val,
                 uint64_t swap_val, int flags = IBVFlags::SIGNAL()) {
    return sched_->PostCAS(coro_id, NodeId(raddr), GetQueuePair(NodeId(raddr)),
                           (void *)NodeAddress(raddr), laddr, compare_val, swap_val, flags);
  }

  Status PostMaskedCAS(coro_id_t coro_id, PoolPtr raddr, void *laddr, uint64_t compare,
                       uint64_t swap, uint64_t compare_mask, uint64_t swap_mask,
                       int flags = IBVFlags::SIGNAL()) {
    return sched_->PostMaskedCAS(coro_id, NodeId(raddr), GetQueuePair(NodeId(raddr)),
                                 (void *)NodeAddress(raddr), laddr, compare, swap, compare_mask,
                                 swap_mask, flags);
  }

  Status PostFAA(coro_id_t coro_id, PoolPtr raddr, void *laddr, uint64_t add,
                 int flags = IBVFlags::SIGNAL()) {
    return sched_->PostFAA(coro_id, NodeId(raddr), GetQueuePair(NodeId(raddr)),
                           (void *)NodeAddress(raddr), laddr, add, flags);
  }

  void YieldForPoll(coro_id_t coro_id, coro_yield_t &yield) {
    return sched_->YieldForPoll(coro_id, yield);
  }

  void YieldToNext(coro_id_t coro_id, coro_yield_t &yield) {
    return sched_->YieldToNext(coro_id, yield);
  }

  void YieldForFinish(coro_id_t coro_id, coro_yield_t &yield) {
    return sched_->YieldForFinish(coro_id, yield);
  }

 private:
  Status FetchMetaData(node_id_t nid);

  void recordPoolAllocationTime(uint64_t us) {
    // RecordHistory(statistic_, POOL_ALLOCATION_LAT, us);
    // RecordTicker(statistic_, POOL_ALLOCATION_COUNT, 1);
  }

 private:
  NodeInitAttr node_info_;  // to start this node
  // std::vector<QueuePair *> qps_;  // queue pairs indexed by node id
  std::unordered_map<node_id_t, QueuePair *> qps_;
  rdma::MemoryRegion *mr_;  // registered memory region on current node
  bool mr_owner_;           // If the object owns this region
  rdma::Context *ctx_;      // Context for all RDMA operations
  Statistics *statistic_;
  CoroutineScheduler *sched_;  // For coroutine
};
