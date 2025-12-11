#include "mempool/Scheduler.h"

#include <infiniband/verbs.h>

#include <cstdint>
#include <cstdlib>

#include "common/Config.h"
#include "mempool/Coroutine.h"
#include "mempool/Pool.h"
#include "rdma/QueuePair.h"
#include "rdma/QueuePairFactory.h"
#include "rdma/RdmaBatch.h"
#include "util/Logger.h"
#include "util/Macros.h"
#include "util/Statistic.h"
#include "util/Status.h"
#include "util/Timer.h"

using namespace rdma;

namespace coro {

bool IsSignaled(int flags) { return (flags & IBVFlags::SIGNAL()) != 0; }

util::Status CoroutineScheduler::PostWrite(coro_id_t coro_id, node_id_t nid, QueuePair *qp,
                                           void *laddr, size_t sz, void *raddr, int flags) {
    ASSERT(CheckLocalMemoryRegionBound(qp, laddr, sz), "");
    ASSERT(CheckRemoteMemoryRegionBound(qp, raddr, sz), "");
    auto s = qp->PostWrite(laddr, sz, raddr, (RequestToken *)GenerateWrId(coro_id), flags);
    if (s.ok() && IsSignaled(flags)) [[likely]] {
        // Add the pending request number only when it is a signaled request
        pending_request_num_[coro_id][nid]++;
        // pending_request_num_per_coro_[coro_id]++;
    } else if (!s.ok()) {
        abort();
    }

    // pending_qp_.emplace_back(qp);
    return s;
}

util::Status CoroutineScheduler::PostRead(coro_id_t coro_id, node_id_t nid, QueuePair *qp,
                                          void *raddr, size_t sz, void *laddr, int flags) {
    ASSERT(CheckLocalMemoryRegionBound(qp, laddr, sz), "");
    ASSERT(CheckRemoteMemoryRegionBound(qp, raddr, sz), "");
    auto s = qp->PostRead(raddr, sz, laddr, (RequestToken *)GenerateWrId(coro_id), flags);
    if (s.ok() && IsSignaled(flags)) [[likely]] {
        pending_request_num_[coro_id][nid]++;
        // pending_request_num_per_coro_[coro_id]++;
    } else if (!s.ok()) {
        abort();
    }

    // pending_qp_.emplace_back(qp);
    return s;
}

util::Status CoroutineScheduler::PostBatch(coro_id_t coro_id, node_id_t nid, RDMABatch *batch,
                                           QueuePair *qp, int flags) {
    auto s = batch->SendRequest(qp);
    if (s.ok() && IsSignaled(flags)) [[likely]] {
        pending_request_num_[coro_id][nid]++;
        // pending_request_num_per_coro_[coro_id]++;
    } else if (!s.ok()) {
        abort();
    }

    // pending_qp_.emplace_back(qp);
    return s;
}

util::Status CoroutineScheduler::PostWriteBatch(coro_id_t coro_id, WriteRecordBatch *write_batch,
                                                QueuePair *qp) {
    ASSERT(CheckLocalMemoryRegionBound(qp, (void *)write_batch->sges[0].addr,
                                       write_batch->sges[0].length),
           "");
    ASSERT(CheckLocalMemoryRegionBound(qp, (void *)write_batch->sges[1].addr,
                                       write_batch->sges[1].length),
           "");
    write_batch->sges[0].lkey = qp->GetLocalMemoryRegionToken().get_local_key();
    write_batch->wrs[0].wr.rdma.rkey = qp->GetRemoteMemoryRegionToken().get_remote_key();
    write_batch->wrs[0].next = &(write_batch->wrs[1]);

    write_batch->sges[1].lkey = qp->GetLocalMemoryRegionToken().get_local_key();
    write_batch->wrs[1].wr.rdma.rkey = qp->GetRemoteMemoryRegionToken().get_remote_key();
    write_batch->wrs[1].next = nullptr;
    write_batch->wrs[1].wr_id = coro_id;

    Status s = qp->PostIBVRequest(&(write_batch->wrs[0]), &(write_batch->bad_wr));
    if (s.ok()) {
        pending_request_num_[coro_id][write_batch->nid]++;
        // pending_request_num_per_coro_[coro_id]++;
    } else {
        abort();
    }
    return s;
}

util::Status CoroutineScheduler::PostLockReadBatch(coro_id_t coro_id, LockReadBatch *lrb,
                                                   QueuePair *qp) {
    lrb->sges[0].lkey = qp->GetLocalMemoryRegionToken().get_local_key();
    lrb->wrs[0].ext_op.masked_atomics.rkey = qp->GetRemoteMemoryRegionToken().get_remote_key();
    lrb->wrs[0].next = &(lrb->wrs[1]);

    lrb->sges[1].lkey = qp->GetLocalMemoryRegionToken().get_local_key();
    lrb->wrs[1].wr.rdma.rkey = qp->GetRemoteMemoryRegionToken().get_remote_key();
    lrb->wrs[1].next = nullptr;
    lrb->wrs[1].wr_id = coro_id;

    Status s = qp->PostExpIBVRequest(&(lrb->wrs[0]), &(lrb->bad_wr));
    if (s.ok()) {
        pending_request_num_[coro_id][lrb->nid]++;
        // pending_request_num_per_coro_[coro_id]++;
    } else {
        abort();
    }
    return s;
}

util::Status CoroutineScheduler::PostCAS(coro_id_t coro_id, node_id_t nid, QueuePair *qp,
                                         void *raddr, void *laddr, uint64_t compare, uint64_t swap,
                                         int flags) {
    INVARIANT(CheckLocalMemoryRegionBound(qp, laddr, sizeof(uint64_t)));
    INVARIANT(CheckRemoteMemoryRegionBound(qp, raddr, sizeof(uint64_t)));
    auto wr_id = GenerateWrId(coro_id);
    auto s = qp->PostCompareAndSwap(raddr, laddr, compare, swap, (RequestToken *)wr_id, flags);
    if (s.ok() && IsSignaled(flags)) [[likely]] {
        pending_request_num_[coro_id][nid]++;
        // pending_request_num_per_coro_[coro_id]++;
    } else if (!s.ok()) {
        abort();
    }

    // pending_qp_.emplace_back(qp);
    return s;
}

util::Status CoroutineScheduler::PostFAA(coro_id_t coro_id, node_id_t nid, QueuePair *qp,
                                         void *raddr, void *laddr, uint64_t add, int flags) {
    INVARIANT(CheckLocalMemoryRegionBound(qp, laddr, sizeof(uint64_t)));
    INVARIANT(CheckRemoteMemoryRegionBound(qp, raddr, sizeof(uint64_t)));
    auto wr_id = GenerateWrId(coro_id);
    auto s = qp->PostFetchAndAdd(raddr, laddr, add, (RequestToken *)wr_id, flags);
    if (s.ok() && IsSignaled(flags)) [[likely]] {
        pending_request_num_[coro_id][nid]++;
        // pending_request_num_per_coro_[coro_id]++;
    } else if (!s.ok()) {
        abort();
    }

    // pending_qp_.emplace_back(qp);
    return s;
}

util::Status CoroutineScheduler::PostMaskedCAS(coro_id_t coro_id, node_id_t nid, QueuePair *qp,
                                               void *raddr, void *laddr, uint64_t compare,
                                               uint64_t swap, uint64_t compare_mask,
                                               uint64_t swap_mask, int flags) {
    INVARIANT(CheckLocalMemoryRegionBound(qp, laddr, sizeof(uint64_t)));
    INVARIANT(CheckRemoteMemoryRegionBound(qp, raddr, sizeof(uint64_t)));
    auto wr_id = GenerateWrId(coro_id);
    auto s = qp->PostMaskedCompareAndSwap(raddr, laddr, compare, swap, compare_mask,
                                          (RequestToken *)wr_id, flags);
    if (s.ok() && IsSignaled(flags)) [[likely]] {
        pending_request_num_[coro_id][nid]++;
        // pending_request_num_per_coro_[coro_id]++;
    } else if (!s.ok()) {
        abort();
    }

    // pending_qp_.emplace_back(qp);
    return s;
}

util::Status CoroutineScheduler::PostBatch(coro_id_t coro_id, node_id_t nid, QueuePair *qp,
                                           RDMABatch &batch) {
    batch.SetWrId(GenerateWrId(coro_id));
    auto s = batch.SendRequest(qp);
    if (s.ok()) [[likely]] {
        pending_request_num_[coro_id][nid]++;
        // pending_request_num_per_coro_[coro_id]++;
    } else {
        abort();
    }

    // pending_qp_.emplace_back(qp);
    return s;
}

void CoroutineScheduler::YieldForPoll(coro_id_t coro_id, coro_yield_t &yield) {
    auto coro = &coros_[coro_id];
    assert(coro->status == CoroutineStatus::kRun);
    coro->status = CoroutineStatus::kWaitPoll;

    ASSERT(pending_request_num_per_coro_[coro_id] == 0, "Invalid pending request number");

    // Traverse all queue pairs to find create the request queue:
    // const auto &qps = pool_->GetAllQueuePair();
    uint64_t pending_request_num_total = 0;
    size_t sz = pool_->GetQueueNum();
    for (node_id_t nid = 0; nid < sz; ++nid) {
        // There is no request on this queue pair
        if (pending_request_num_[coro_id][nid] > 0) {
            pending_requests_.emplace_back(RequestHandle{
                coro_id,
                nid,
                pool_->GetQueuePair(nid),
                pending_request_num_[coro_id][nid],
            });
        }
        pending_request_num_total += pending_request_num_[coro_id][nid];
    }
    pending_request_num_per_coro_[coro_id] = pending_request_num_total;
    ASSERT(pending_request_num_per_coro_[coro_id] > 0, "No pending request");
    // printf("T%lu C%d yield for poll: request_num = %lu\n", tid_, coro_id,
    // pending_request_num_total);

    auto next = coro->next_coro;
    RemoveFromActiveLists(coro);
    // Continue running the next linked coroutine
    // However, the question is: should we do one polling to find the next
    // possible coroutine?
    RunCoroutine(next, yield);
}

void CoroutineScheduler::YieldToNext(coro_id_t coro_id, coro_yield_t &yield) {
    // printf("T%lu C%d yield to next\n", tid_, coro_id);
    auto coro = &coros_[coro_id];
    coro->status = CoroutineStatus::kPending;
    auto next = coro->next_coro;
    // Do not remove current coroutine from the active list
    RunCoroutine(next, yield);
}

void CoroutineScheduler::YieldForFinish(coro_id_t coro_id, coro_yield_t &yield) {
    // A finished coroutine must have no pending RDMA requests
    INVARIANT(pending_request_num_per_coro_[coro_id] == 0);
    auto coro = &coros_[coro_id];
    // this coroutine is finished and should never be scheduled
    coro->status = CoroutineStatus::kFinish;
    auto next = coro->next_coro;
    RemoveFromActiveLists(coro);
    RunCoroutine(next, yield);
}

void CoroutineScheduler::YieldForSleep(coro_id_t coro_id, coro_yield_t &yield, uint64_t dura) {
    auto coro = &coros_[coro_id];
    assert(coro->status == CoroutineStatus::kRun);
    coro->status = CoroutineStatus::kWaitSleep;
    coro->start_sleep_time = util::NowMicros();
    coro->sleep_dura = dura;
    auto next = coro->next_coro;
    RemoveFromActiveLists(coro);
    // Continue running the next linked coroutine
    // However, the question is: should we do one polling to find the next
    // possible coroutine?
    RunCoroutine(next, yield);
}

util::Status CoroutineScheduler::PollCompletionQueue(coro_id_t *wake_coro) {
    static constexpr int kMaxPollWCNum = 32;
    bool has_waken_coro = false;
    for (auto it = pending_requests_.begin(); it != pending_requests_.end();) {
        QueuePair *qp = it->qp;
        coro_id_t coro_id = it->coro_id;
        ibv_wc wc[kMaxPollWCNum];
        // We need to check if ``it->req_num" requests have been finished
        int polled = 0, to_poll = it->req_num;
        while (polled < to_poll) {
            int ret =
                ibv_poll_cq(qp->GetSendCQ(), std::min(to_poll - polled, kMaxPollWCNum), &wc[0]);
            if (ret < 0) {
                LOG_FATAL("Read CQ failed: status");
            }
            polled += ret;
            // Check each work completion element
            for (int i = 0; i < ret; ++i) {
                if (wc[i].status != IBV_WC_SUCCESS) {
                    LOG_FATAL("Error status: %d, %s", wc[i].status,
                              ibv_wc_status_str(wc[i].status));
                }
            }
        }
        node_id_t nid = it->node_id;
        pending_request_num_[coro_id][nid] -= polled;
        pending_request_num_per_coro_[coro_id] -= polled;
        ASSERT(pending_request_num_per_coro_[coro_id] >= 0,
               "Pending Request Number can not be negative");
        if (pending_request_num_per_coro_[coro_id] == 0) {
            ASSERT(coros_[coro_id].status == CoroutineStatus::kWaitPoll, "Invalid status");
            AddToTail(&coros_[coro_id]);
            *wake_coro = coro_id;
            has_waken_coro = true;
            // printf("T%lu C%d pending_request = 0, add to list\n", tid_, coro_id);
        }
        // Delete this handle as all requests have been done
        it = pending_requests_.erase(it);
    }
    return util::Status::OK();
}

}  // namespace coro
