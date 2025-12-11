#pragma once
#include <infiniband/verbs.h>
#include <infiniband/verbs_exp.h>

#include <atomic>
#include <memory>

#include "rdma/MemoryRegion.h"
#include "rdma/QueuePairFactory.h"
#include "util/Status.h"

namespace rdma {

enum RDMAOpType {
  kSend,
  kRecv,
  kRecvWithImm,
  kWrite,
  kRead,
  kFAA,
  kCAS,
  kNone,
};

class Context;
class MemoryRegion;
class QueuePairFactory;
class QueuePair;

// A request token is a returned token for an RDMA request. The
// caller of RDMA data verbs can poll this token to check if the
// corresponding request has been done.
struct RequestToken {
  friend class QueuePair;
  RequestToken() : done_(false), type_(kNone) {}

  bool IsDone() { return done_.load(); }
  void SetDone() { done_.store(true); }
  void SetUndone() { done_.store(false); }
  void Reset() {
    done_ = false;
    type_ = kNone;
  }

  bool ok() { return wc_status_ == IBV_WC_SUCCESS; }
  void SetStatus(ibv_wc_status status) { wc_status_ = status; }

  uint64_t RequestId() const { return (uint64_t)this; }

  void SetImmData(uint32_t imm) { imm_data_ = imm; }
  uint32_t GetImmData() const { return imm_data_; }

 private:
  std::atomic<bool> done_;
  ibv_wc_status wc_status_;

  uint32_t imm_data_;
  RDMAOpType type_;
};

struct IBVFlags {
  static int NONE() { return 0; }

  static int SIGNAL() { return IBV_SEND_SIGNALED; }

  // INLINE is used to avoid one extra PCIe read, only used for small payload
  // (<64B)
  static int INLINE() { return IBV_SEND_INLINE; }

  static int FENCE() { return IBV_SEND_FENCE; }
};

class QueuePair {
  friend class QueuePairFactory;

 public:
  QueuePair(Context *ctx, MemoryRegionToken mr_token);
  ~QueuePair() {
    // for (const auto& [t, hist] : metrics_) {
    //   delete hist;
    // }
  }

  // Change the states of queue pair to Init
  util::Status Init();

  // Change the states of queue pair to RTS and RTR using received remote
  // QueuePair information
  util::Status Activate(const SerializedQueuePair *qp_info);

 public:
  /// Two-sided send/recv operations
  // Send operations with or without immediate data, if this function works
  // well, use PollCompletionQueue to check if this send request has finished.
  util::Status PostSend(void *laddr, size_t length, RequestToken *token,
                        int flags = IBVFlags::SIGNAL(), bool with_imm = false, uint32_t imm = 0);

  // Post a receive request to the queue pair and wait for the data coming.
  util::Status PostRecv(void *laddr, size_t length, RequestToken *token);

  // To avoid create request token for each RDMA request, the caller can specify
  // their own token. The caller must ensure the token is not nullptr
  util::Status PostWrite(void *laddr, size_t length, void *raddr, RequestToken *token,
                         int flags = IBVFlags::SIGNAL());

  util::Status PostRead(void *raddr, size_t length, void *laddr, RequestToken *token,
                        int flags = IBVFlags::SIGNAL());

  util::Status PostCompareAndSwap(void *raddr, void *laddr, uint64_t compare, uint64_t swap,
                                  RequestToken *token, int flags = IBVFlags::SIGNAL());

  util::Status PostMaskedCompareAndSwap(void *raddr, void *laddr, uint64_t compare, uint64_t swap,
                                        uint64_t mask, RequestToken *token,
                                        int flags = IBVFlags::SIGNAL());

  util::Status PostFetchAndAdd(void *raddr, void *laddr, uint64_t add, RequestToken *token,
                               int flags = IBVFlags::SIGNAL());

  // Poll the completion queue until the specified request token is found
  // The req_token can be set to be nullptr, which means there is no specified
  // request token to be polled. This can be used to implement a dedicated
  // polling thread
  util::Status PollCompletionQueue(RequestToken *req_token);

  util::Status PollCompletionQueueOnce(RequestToken *req_token);

  // Poll the completion queue of receive operations
  util::Status PollRecvCompletionQueue(RequestToken *req_token);

  util::Status PollRecvCompletionQueueOnce(RequestToken *req_token);

  // auto GetMetricsData(RDMAOpType type) -> util::HistogramData<uint64_t>*
  // const {
  //   if (metrics_.count(type) == 0) {
  //     return nullptr;
  //   }
  //   return metrics_[type];
  // }

 public:
  // Some convenient wrapper functions
  // SYNC write/read interfaces
  util::Status Write(void *local_addr, size_t sz, void *remote_addr, RequestToken *token,
                     int flags = IBVFlags::SIGNAL());

  util::Status Read(void *remote_addr, size_t sz, void *local_addr, RequestToken *token,
                    int flags = IBVFlags::SIGNAL());

  util::Status CompareAndSwap(void *remote_addr, void *local_addr, uint64_t compare, uint64_t swap,
                              RequestToken *token, int flags = IBVFlags::SIGNAL());

  // Require MLNX_OFED_LINUX-4.9-6.0.6.0
  util::Status MaskedCompareAndSwap(void *raddr, void *laddr, uint64_t compare, uint64_t swap,
                                    uint64_t mask, RequestToken *token,
                                    int flags = IBVFlags::SIGNAL());

  util::Status FetchAndAdd(void *raddr, void *laddr, uint64_t add, RequestToken *token,
                           int flags = IBVFlags::SIGNAL());

  util::Status PostIBVRequest(ibv_send_wr *wr, ibv_send_wr **bad_wr);

  util::Status PostExpIBVRequest(ibv_exp_send_wr *wr, ibv_exp_send_wr **bad_wr);

  void FillinSge(ibv_sge *sge, void *laddr, size_t sz, uint32_t lkey) {
    memset(sge, 0, sizeof(ibv_sge));
    sge->addr = (uint64_t)laddr;
    sge->length = sz;
    sge->lkey = lkey;
  }

  void FillinWR(ibv_send_wr *wr, void *raddr, uint32_t rkey, int flags, uint64_t wr_id) {
    memset(wr, 0, sizeof(ibv_send_wr));
    wr->send_flags = flags;
    wr->wr.rdma.remote_addr = (uint64_t)raddr;
    wr->wr.rdma.rkey = this->rmr_token_.get_remote_key();
    wr->wr_id = wr_id;
  }

 public:
  MemoryRegionToken GetLocalMemoryRegionToken() const { return lmr_token_; }

  MemoryRegionToken GetRemoteMemoryRegionToken() const { return rmr_token_; }

  uint32_t GetQueuePairNumber() const { return ibv_qp_->qp_num; }

  auto GetSendCQ() { return send_cq_; }

 private:
  // Quick function to return the required attributes
  ibv_qp_init_attr DefaultQPInitAttr();

  ibv_exp_qp_init_attr DefaultExpQPInitAttr(Context *ctx);

  ibv_qp_attr DefaultQPAttrForInit();

  ibv_qp_attr DefaultQPAttrForRTR(const SerializedQueuePair *qp_info);

  std::unique_ptr<ibv_qp_attr> DefaultQPAttrForRTS(const SerializedQueuePair *qp_info);

 private:
  Context *ctx_;
  ibv_qp *ibv_qp_;

  // Completion queue for send request and recv request
  ibv_cq *send_cq_, *recv_cq_;

  uint64_t sequence_number_;

  // The remote_region_token_ is set when the connection is established
  MemoryRegionToken lmr_token_, rmr_token_;

  // A simple metrics:
  // std::unordered_map<RDMAOpType, util::HistogramData<uint64_t>*> metrics_;
};
};  // namespace rdma
