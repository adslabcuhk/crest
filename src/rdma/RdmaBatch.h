#pragma once

#include <cstddef>
#include <vector>

#include "infiniband/verbs.h"
#include "rdma/QueuePair.h"

namespace rdma {

struct RDMABatch {
  static constexpr size_t MAX_BATCH_WR_NUM = 64;
  ibv_send_wr wrs_[MAX_BATCH_WR_NUM];
  ibv_sge sges_[MAX_BATCH_WR_NUM];
  size_t sz_;
  // std::vector<ibv_send_wr> wrs_;
  // std::vector<ibv_sge> sges_;
  ibv_send_wr *bad_wr;
  // uint64_t read_sz_, write_sz_, cas_sz_, faa_sz_;

  RDMABatch() : wrs_(), sges_(), sz_(0), bad_wr(nullptr) {}

  RDMABatch(size_t sz) : wrs_(), sges_(), sz_(0), bad_wr(nullptr) {
    // wrs_.reserve(sz);
    // sges_.reserve(sz);
  }

  int batch_size() const { return sz_; }

  void clear() { sz_ = 0; }

  // User can post requests to the tail of this Batch object. The RequstBatch
  // would link all wrs in the order they are posted in this queue.
  void PostWrite(void *laddr, size_t sz, void *raddr, int flags = IBVFlags::NONE());

  void PostRead(void *raddr, size_t sz, void *laddr, int flags = IBVFlags::NONE());

  void PostCAS(void *laddr, void *raddr, uint64_t compare, uint64_t swap,
               int flags = IBVFlags::NONE());

  void PostFAA(void *laddr, void *raddr, uint64_t inc, int flags = IBVFlags::NONE());

  // Sending the batch requests requires the metadata of queue pair, i.e., the
  // local and remote key of the targeted memory region.
  util::Status SendRequest(QueuePair *qp);

  void SetWrId(uint64_t wr_id) { wrs_[sz_ - 1].wr_id = wr_id; }
};
};  // namespace rdma