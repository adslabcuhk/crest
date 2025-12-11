
#include "rdma/RdmaBatch.h"

#include <infiniband/verbs.h>

#include <cstdint>

#include "rdma/QueuePair.h"
#include "rdma/RdmaConfig.h"
#include "util/Status.h"

namespace rdma {

void RDMABatch::PostWrite(void *laddr, size_t sz, void *raddr, int flags) {
  size_t id = sz_++;

  ibv_sge &sge = sges_[id];
  sge.addr = (uint64_t)laddr;
  sge.length = (uint32_t)sz;

  ibv_send_wr &wr = wrs_[id];
  std::memset(&wr, 0, sizeof(wr));
  wr.wr.rdma.remote_addr = (uint64_t)raddr;
  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.send_flags |= flags;

  wr.sg_list = &sges_[id];
  wr.num_sge = 1;
}

void RDMABatch::PostRead(void *raddr, size_t sz, void *laddr, int flags) {
  size_t id = sz_++;

  ibv_sge &sge = sges_[id];
  sge.addr = (uint64_t)laddr;
  sge.length = (uint32_t)sz;

  ibv_send_wr &wr = wrs_[id];
  std::memset(&wr, 0, sizeof(wr));
  wr.wr.rdma.remote_addr = (uint64_t)raddr;
  wr.opcode = IBV_WR_RDMA_READ;
  wr.send_flags |= flags;

  wr.sg_list = &sges_[id];
  wr.num_sge = 1;
}

void RDMABatch::PostCAS(void *laddr, void *raddr, uint64_t compare, uint64_t swap, int flags) {
  size_t id = sz_++;

  ibv_sge &sge = sges_[id];
  sge.addr = (uint64_t)laddr;
  sge.length = sizeof(uint64_t);

  ibv_send_wr &wr = wrs_[id];
  std::memset(&wr, 0, sizeof(wr));
  wr.wr.atomic.remote_addr = (uint64_t)raddr;
  wr.wr.atomic.compare_add = compare;
  wr.send_flags |= flags;
  wr.send_flags |= IBVFlags::INLINE();
  wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;

  wr.sg_list = &sges_[id];
  wr.num_sge = 1;
}

void RDMABatch::PostFAA(void *laddr, void *raddr, uint64_t inc, int flags) {
  // sges_.emplace_back(
  //     ibv_sge{.addr = (uint64_t)laddr, .length = sizeof(uint64_t)});
  size_t id = sz_++;

  ibv_sge &sge = sges_[id];
  sge.addr = (uint64_t)laddr;
  sge.length = sizeof(uint64_t);

  ibv_send_wr &wr = wrs_[id];
  std::memset(&wr, 0, sizeof(wr));
  wr.wr.atomic.remote_addr = (uint64_t)raddr;
  wr.wr.atomic.compare_add = inc;
  wr.send_flags |= flags;
  wr.send_flags |= IBVFlags::INLINE();
  wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;

  wr.sg_list = &sges_[id];
  wr.num_sge = 1;
}

util::Status RDMABatch::SendRequest(QueuePair *qp) {
  for (size_t i = 0; i < sz_; ++i) {
    sges_[i].lkey = qp->GetLocalMemoryRegionToken().get_local_key();
    if (wrs_[i].opcode == IBV_WR_ATOMIC_CMP_AND_SWP ||
        wrs_[i].opcode == IBV_WR_ATOMIC_FETCH_AND_ADD) {
      wrs_[i].wr.atomic.rkey = qp->GetRemoteMemoryRegionToken().get_remote_key();
    } else {
      wrs_[i].wr.rdma.rkey = qp->GetRemoteMemoryRegionToken().get_remote_key();
    }
    // Link all requests
    if (i >= 1) {
      wrs_[i - 1].next = &wrs_[i];
    }
  }
  // The last work request must be marked as signal
  wrs_[sz_ - 1].send_flags |= IBVFlags::SIGNAL();
  // Set the last one's next pointer to be nullptr
  wrs_[sz_ - 1].next = nullptr;
  return qp->PostIBVRequest(&wrs_[0], &bad_wr);
}

};  // namespace rdma
