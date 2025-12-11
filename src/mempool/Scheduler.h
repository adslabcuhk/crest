#pragma once

#include <infiniband/verbs.h>

#include <cstring>

#include "common/Type.h"
#include "mempool/Coroutine.h"
#include "rdma/Doorbell.h"
#include "rdma/QueuePair.h"
#include "rdma/QueuePairFactory.h"
#include "rdma/RdmaBatch.h"
#include "util/Logger.h"
#include "util/Macros.h"
#include "util/Statistic.h"
#include "util/Status.h"
#include "util/Timer.h"

using namespace rdma;

class Pool;
namespace coro {

// The Coroutine has req_num at the specific qp
struct RequestHandle {
  coro_id_t coro_id;
  node_id_t node_id;
  QueuePair *qp;
  int req_num;
};

// A CoroutineScheduler is a wrapper managing the RDMA requests of multiple
// concurrently running coroutines. It contains a poll coroutine that
// periodically poll the CQ of used QueuePair and notifies the coroutine
// that sent the requests.
// Each thread has a thread-local CoroutineScheduler manages all coroutines
// spawned from this thread. All coroutines are supposed to use the same
// `QueuePair' struct to send RDMA requests and poll the corresponding
// CQ to get the response
class CoroutineScheduler {
  friend class Txn;
  friend class TxnRecord;

  static constexpr size_t kMaxCorotineNum = 16;

  // The maximum number of remote qp each coroutine can have
  static constexpr size_t kMaxRemoteQPNum = 16;

  // The 0th coroutine is used for polling all the time
  static constexpr int POLL_CORO_ID = 0;

 public:
  CoroutineScheduler(ThreadId tid, int coro_num, Statistics *stat)
      : tid_(tid), coro_num_(coro_num), stat_(stat) {
    coros_ = new Coroutine[coro_num];
    head_ = &coros_[0];
    // Link all Coroutine in a double-linked manner:
    for (int i = 0; i < coro_num; ++i) {
      coros_[i].next_coro = (i == coro_num - 1 ? head_ : &coros_[i + 1]);
      coros_[i].prev_coro = (i == 0 ? &coros_[coro_num - 1] : &coros_[i - 1]);
      coros_[i].coro_id = i;
    }
    std::memset(pending_request_num_, 0, sizeof(pending_request_num_));
    std::memset(pending_request_num_per_coro_, 0, sizeof(pending_request_num_per_coro_));
  }
  ~CoroutineScheduler() { delete[] coros_; }

  // A CoroutineScheduler is not copyable.
  CoroutineScheduler(const CoroutineScheduler &) = delete;
  CoroutineScheduler &operator=(const CoroutineScheduler &) = delete;

  Coroutine *GetCoroutine(coro_id_t coro_id) { return &coros_[coro_id]; }

  // We embed the current time (as micro seconds) together with the coro_id
  // so that the polling coroutine can easily calculate the time duration
  // between posting and polling the request. We use the most significant 56bits
  // to store the current time as a 56bits number can represent a time interval
  // of more than two thousand years, which is feasible in our case. We use the
  // least significant 8bits to store the coro_id
  inline uint64_t GenerateWrId(coro_id_t coro_id) {
#if ENABLE_PERF
    uint64_t now_us = util::NowMicros();
    return now_us << 8 | (uint64_t)coro_id;
#else
    return static_cast<uint64_t>(coro_id);
#endif
  }

  inline coro_id_t GetCoroId(uint64_t wr_id) { return wr_id & 0xFFULL; }

  inline uint64_t GetEmbeddedTime(uint64_t wr_id) { return wr_id >> 8; }

  void SetPool(Pool *pool) { pool_ = pool; }

 public:
  // Interface for issuing RDMA requests
  util::Status PostWrite(coro_id_t coro_id, node_id_t nid, QueuePair *qp, void *laddr, size_t sz,
                         void *raddr, int flags);

  util::Status PostRead(coro_id_t coro_id, node_id_t nid, QueuePair *qp, void *raddr, size_t sz,
                        void *laddr, int flags);

  util::Status PostBatch(coro_id_t coro_id, node_id_t nid, RDMABatch *batch, QueuePair *qp,
                         int flags);

  util::Status PostWriteBatch(coro_id_t coro_id, WriteRecordBatch *write_batch, QueuePair *qp);

  util::Status PostLockReadBatch(coro_id_t coro_id, LockReadBatch *lock_read_batch, QueuePair *qp);

  util::Status PostCAS(coro_id_t coro_id, node_id_t nid, QueuePair *qp, void *raddr, void *laddr,
                       uint64_t compare, uint64_t swap, int flags);

  util::Status PostFAA(coro_id_t coro_id, node_id_t nid, QueuePair *qp, void *raddr, void *laddr,
                       uint64_t add, int flags);

  util::Status PostMaskedCAS(coro_id_t coro_id, node_id_t nid, QueuePair *qp, void *raddr,
                             void *laddr, uint64_t compare, uint64_t swap, uint64_t compare_mask,
                             uint64_t swap_mask, int flags);

  util::Status PostBatch(coro_id_t coro_id, node_id_t nid, QueuePair *qp, RDMABatch &batch);

  // Yield the Coroutine specified by coro_id until it is awakened.
  void YieldForPoll(coro_id_t coro_id, coro_yield_t &yield);

  // Simply yield to the next coroutine
  void YieldToNext(coro_id_t coro_id, coro_yield_t &yield);

  void YieldForFinish(coro_id_t coro_id, coro_yield_t &yield);

  // This coroutine will be sleeping at least for #dura us
  // Note that this does not mean the duration between the next wakeup time point and the
  // last sleep time point would be exactly the #dura time
  void YieldForSleep(coro_id_t coro_id, coro_yield_t &yield, uint64_t dura);

  // Continue the execution of a specific coroutine
  void RunCoroutine(Coroutine *coro, coro_yield_t &yield) {
    if (coro->coro_id != 0) {
      // printf("T%lu C%d is scheduled to run\n", tid_, coro->coro_id);
    }
    assert(!coro->is_wait_poll);
    ASSERT(pending_request_num_per_coro_[coro->coro_id] == 0, "Invalid request number");
    // ASSERT: If the scheduler is gonna to schedule a coroutine, the
    // coroutine must not be in a WaitPoll state, which means its requested
    // RDMA operation has not been completed yet
    ASSERT(coro->status != CoroutineStatus::kWaitPoll, "");
    coro->status = CoroutineStatus::kRun;
    yield(coro->func);
  }

  bool CheckAllCoroutineFinished() {
    for (int i = 1; i < coro_num_; ++i) {
      if (coros_[i].status != CoroutineStatus::kFinish) {
        return false;
      }
    }
    return true;
  }

  bool CheckLocalMemoryRegionBound(QueuePair *qp, void *laddr, size_t sz) {
    auto lmr = qp->GetLocalMemoryRegionToken();
    bool right_bound = (uint64_t)laddr + sz <= lmr.get_region_bound();
    bool left_bound = (uint64_t)laddr >= lmr.get_region_addr();
    return right_bound && left_bound;
  }

  bool CheckRemoteMemoryRegionBound(QueuePair *qp, void *raddr, size_t sz) {
    auto rmr = qp->GetRemoteMemoryRegionToken();
    bool right_bound = (uint64_t)raddr + sz <= rmr.get_region_bound();
    bool left_bound = (uint64_t)raddr >= rmr.get_region_addr();
    return left_bound && right_bound;
  }

  util::Status PollCompletionQueue(coro_id_t *wake_coro);

  void RecordRequestLatency(ibv_wc *wc) {
#if ENABLE_PERF
    // uint64_t dura = util::NowMicros() - GetEmbeddedTime(wc->wr_id);
    // HistTypes type = RDMA_READ_LAT;
    // switch (wc->opcode) {
    //   case IBV_WC_RDMA_READ:
    //     type = RDMA_READ_LAT;
    //     break;
    //   case IBV_WC_RDMA_WRITE:
    //     type = RDMA_WRITE_LAT;
    //     break;
    //   case IBV_WC_COMP_SWAP:
    //     type = RDMA_CAS_LAT;
    //   case IBV_WC_FETCH_ADD:
    //     type = RDMA_FAA_LAT;
    //   default:
    //     type = INVALID_TYPE;
    //     break;
    // }
    // if (stat_) [[likely]] {
    //   stat_->RecordHistory(type, dura);
    // }
#endif
  }

  Coroutine *Head() const { return head_; }

 private:
  // Remove this coroutine from the active list if it issues an RDMA request.
  void RemoveFromActiveLists(Coroutine *coro) {
    coro->prev_coro->next_coro = coro->next_coro;
    coro->next_coro->prev_coro = coro->prev_coro;
    if (head_ == coro) {
      head_ = coro->next_coro;
    }
    coro->next_coro = coro->prev_coro = nullptr;
    // LOG_DEBUG("Coro%d is removed from active lists", coro->coro_id);
  }

  // Add this coroutine to the tail of active lists
  void AddToTail(Coroutine *coro) {
    head_->prev_coro->next_coro = coro;
    coro->prev_coro = head_->prev_coro;
    head_->prev_coro = coro;
    coro->next_coro = head_;
    coro->status = CoroutineStatus::kPending;
    // LOG_DEBUG("Coro%d is added to the tail of active list", coro->coro_id);
  }

 public:
  ThreadId tid_;  // thread id

  Coroutine *coros_;  // An array of all coroutines

  Coroutine *head_;  // The head of active coroutines

  int coro_num_;

  // The number of pending rdma requests of each coroutine
  // The request number is incremented by 1 if the corresponding coroutine sends
  // an RDMA request. Minus 1 if the poll coroutine polls a completion of the
  // request.
  int pending_request_num_[kMaxCorotineNum][kMaxRemoteQPNum] = {0};
  int pending_request_num_per_coro_[kMaxCorotineNum] = {0};

  // The list of QueuePair used for sending RDMA requests.
  // std::vector<QueuePair *> pending_qp_;

  std::vector<RequestHandle> pending_requests_;

  Statistics *stat_;

  Pool *pool_;
};

};  // namespace coro
