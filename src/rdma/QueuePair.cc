
#include "rdma/QueuePair.h"

#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <infiniband/verbs_exp.h>
#include <netinet/in.h>

#include <cassert>
#include <cstdint>
#include <memory>

#include "rdma/Context.h"
#include "rdma/MemoryRegion.h"
#include "rdma/QueuePairFactory.h"
#include "rdma/RdmaConfig.h"
#include "util/Logger.h"
#include "util/Status.h"

namespace rdma {
QueuePair::QueuePair(Context *ctx, MemoryRegionToken mr_token) : ctx_(ctx), lmr_token_(mr_token) {
  // Create completion queue:
  this->send_cq_ = ibv_create_cq(ctx_->get_ib_context(), 16534, nullptr, nullptr, 0);
  this->recv_cq_ = ibv_create_cq(ctx_->get_ib_context(), 16534, nullptr, nullptr, 0);
  if (!this->send_cq_ || !this->recv_cq_) {
    LOG_ERROR("Create completion queue failed");
  }

  // auto qp_init_attr = DefaultQPInitAttr();
  // this->ibv_qp_ = ibv_create_qp(ctx->get_ib_pd(), &qp_init_attr);

  // To enable the experimental verbs, we must call ibv_exp_create_qp instead of ibv_create_qp
  // defined in the core verbs.
  auto qp_init_attr = DefaultExpQPInitAttr(ctx_);
  this->ibv_qp_ = ibv_exp_create_qp(ctx->get_ib_context(), &qp_init_attr);
  if (!this->ibv_qp_) {
    LOG_ERROR("Create queue pair failed");
  }
  this->sequence_number_ = 0;
}

util::Status QueuePair::Init() {
  auto qp_attr = DefaultQPAttrForInit();
  if (ibv_modify_qp(this->ibv_qp_, &(qp_attr),
                    IBV_QP_STATE | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS | IBV_QP_PKEY_INDEX)) {
    return util::Status(util::kNetworkError, errno, "Modify QP status failed");
  }
  return util::Status::OK();
}

// Change the states of queue pair from init to RTS

util::Status QueuePair::Activate(const SerializedQueuePair *qp_info) {
  auto qp_attr = DefaultQPAttrForRTR(qp_info);
  auto ret = ibv_modify_qp(this->ibv_qp_, &qp_attr,
                           IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                               IBV_QP_RQ_PSN | IBV_QP_MIN_RNR_TIMER | IBV_QP_MAX_DEST_RD_ATOMIC);

  if (ret != 0) {
    return util::Status(util::kNetworkError, errno, "Modify QP Status to RTR failed");
  }

  auto qp_attr2 = DefaultQPAttrForRTS(qp_info);
  ret = ibv_modify_qp(this->ibv_qp_, qp_attr2.get(),
                      IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                          IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);

  if (ret != 0) {
    return util::Status(util::kNetworkError, errno, "Modify QP Status to RTS failed");
  }

  return util::Status::OK();
}

util::Status QueuePair::PostSend(void *laddr, size_t length, RequestToken *token, int flags,
                                 bool with_imm, uint32_t imm) {
  struct ibv_sge sg;
  struct ibv_send_wr sr;
  struct ibv_send_wr *bad_wr;

  memset(&sg, 0, sizeof(ibv_sge));
  sg.addr = (uint64_t)laddr;
  sg.length = length;
  sg.lkey = lmr_token_.get_local_key();

  memset(&sr, 0, sizeof(ibv_send_wr));
  // Set the wr_id to be the address of the request token so that the completion
  // element that is polled can directly modify the "done" field
  sr.wr_id = (uint64_t)token;
  sr.sg_list = &sg;
  sr.num_sge = 1;
  sr.opcode = IBV_WR_SEND;
  sr.send_flags = flags;

  if (laddr == nullptr || length == 0) {
    sr.num_sge = 0;
  }

  if (with_imm) {
    sr.opcode = IBV_WR_SEND_WITH_IMM;
    sr.imm_data = htonl(imm);
  }

  if (ibv_post_send(this->ibv_qp_, &sr, &bad_wr)) {
    return util::Status(util::kNetworkError, errno, "Post send request failed");
  }

  return util::Status::OK();
}

util::Status QueuePair::PostRecv(void *laddr, size_t length, RequestToken *token) {
  struct ibv_sge sg;
  struct ibv_recv_wr rr;
  struct ibv_recv_wr *bad_wr;

  memset(&sg, 0, sizeof(sg));
  sg.addr = (uint64_t)laddr;
  sg.length = length;
  sg.lkey = lmr_token_.get_local_key();

  memset(&rr, 0, sizeof(rr));
  rr.wr_id = (uint64_t)token;
  rr.next = nullptr;
  rr.sg_list = &sg;
  rr.num_sge = 1;
  if (laddr == nullptr || length == 0) {
    rr.num_sge = 0;
  }

  if (ibv_post_recv(this->ibv_qp_, &rr, &bad_wr)) {
    return util::Status(util::kNetworkError, errno, "Post recv request failed");
  }

  return util::Status::OK();
}

util::Status QueuePair::PostWrite(void *laddr, size_t length, void *raddr, RequestToken *token,
                                  int flags) {
  struct ibv_sge sg;
  struct ibv_send_wr sr;
  struct ibv_send_wr *bad_wr;

  if (length < RdmaConfig::MaxDoorbellInlineSize) {
    flags |= IBVFlags::INLINE();
  }

  FillinSge(&sg, laddr, length, lmr_token_.get_local_key());
  FillinWR(&sr, raddr, rmr_token_.get_remote_key(), flags, (uint64_t)token);

  // Set the single sge to the wr
  sr.sg_list = &sg;
  sr.num_sge = 1;
  sr.opcode = IBV_WR_RDMA_WRITE;

  if (ibv_post_send(this->ibv_qp_, &sr, &bad_wr) != 0) {
    return util::Status(util::kNetworkError, errno, "Post send failed: type write");
  }

  return util::Status::OK();
}

util::Status QueuePair::PostRead(void *raddr, size_t length, void *laddr, RequestToken *token,
                                 int flags) {
  struct ibv_sge sg;
  struct ibv_send_wr sr;
  struct ibv_send_wr *bad_wr;
  auto mr_token = this->rmr_token_;

  FillinSge(&sg, laddr, length, lmr_token_.get_local_key());
  FillinWR(&sr, raddr, rmr_token_.get_remote_key(), flags, (uint64_t)token);

  sr.sg_list = &sg;
  sr.num_sge = 1;
  sr.opcode = IBV_WR_RDMA_READ;

  if (ibv_post_send(this->ibv_qp_, &sr, &bad_wr)) {
    return util::Status(util::kNetworkError, errno, "Post send failed: type read");
  }

  return util::Status::OK();
}

util::Status QueuePair::PostCompareAndSwap(void *raddr, void *laddr, uint64_t compare,
                                           uint64_t swap, RequestToken *token, int flags) {
  struct ibv_sge sg;
  struct ibv_send_wr sr;
  struct ibv_send_wr *bad_wr;
  auto mr_token = this->rmr_token_;

  // flags |= IBVFlags::INLINE();

  FillinSge(&sg, laddr, sizeof(uint64_t), lmr_token_.get_local_key());

  memset(&sr, 0, sizeof(ibv_send_wr));
  sr.wr_id = (uint64_t)token;
  sr.sg_list = &sg;
  sr.num_sge = 1;
  sr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
  sr.send_flags = flags;

  sr.wr.atomic.remote_addr = (uint64_t)raddr;
  sr.wr.atomic.rkey = mr_token.get_remote_key();
  sr.wr.atomic.compare_add = compare;
  sr.wr.atomic.swap = swap;

  if (ibv_post_send(this->ibv_qp_, &sr, &bad_wr)) {
    return util::Status(util::kNetworkError, errno, "Post request failed: type CAS");
  }

  return util::Status::OK();
}

util::Status QueuePair::PostMaskedCompareAndSwap(void *raddr, void *laddr, uint64_t compare,
                                                 uint64_t swap, uint64_t mask, RequestToken *token,
                                                 int flags) {
  struct ibv_sge sg;
  struct ibv_exp_send_wr wr;
  struct ibv_exp_send_wr *bad_wr;

  memset(&sg, 0, sizeof(sg));
  sg.addr = (uint64_t)laddr;
  sg.length = sizeof(uint64_t);
  sg.lkey = lmr_token_.get_local_key();

  memset(&wr, 0, sizeof(wr));
  wr.wr_id = (uint64_t)token;
  wr.sg_list = &sg;
  wr.num_sge = 1;
  wr.next = NULL;

  wr.exp_opcode = IBV_EXP_WR_EXT_MASKED_ATOMIC_CMP_AND_SWP;
  wr.exp_send_flags = flags | IBV_EXP_SEND_EXT_ATOMIC_INLINE;

  wr.ext_op.masked_atomics.log_arg_sz = 3;
  wr.ext_op.masked_atomics.remote_addr = (uint64_t)raddr;
  wr.ext_op.masked_atomics.rkey = rmr_token_.get_remote_key();

  auto &op = wr.ext_op.masked_atomics.wr_data.inline_data.op.cmp_swap;
  op.compare_val = compare;
  op.swap_val = swap;

  op.compare_mask = mask;
  op.swap_mask = mask;

  if (ibv_exp_post_send(this->ibv_qp_, &wr, &bad_wr)) {
    return util::Status(util::kNetworkError, errno, "Post request failed: type Masked CAS");
  }
  return util::Status::OK();
}

util::Status QueuePair::PostFetchAndAdd(void *raddr, void *laddr, uint64_t add, RequestToken *token,
                                        int flags) {
  struct ibv_sge sg;
  struct ibv_send_wr sr;
  struct ibv_send_wr *bad_wr;
  auto mr_token = this->rmr_token_;

  flags |= IBVFlags::INLINE();

  FillinSge(&sg, laddr, sizeof(uint64_t), lmr_token_.get_local_key());

  memset(&sr, 0, sizeof(ibv_send_wr));
  sr.wr_id = (uint64_t)token;
  sr.sg_list = &sg;
  sr.num_sge = 1;
  sr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
  sr.send_flags = flags;

  sr.wr.atomic.remote_addr = (uint64_t)raddr;
  sr.wr.atomic.rkey = mr_token.get_remote_key();
  sr.wr.atomic.compare_add = add;

  if (ibv_post_send(this->ibv_qp_, &sr, &bad_wr)) {
    return util::Status(util::kNetworkError, errno, "Post request failed: type FAA");
  }

  return util::Status::OK();
}

util::Status QueuePair::PollCompletionQueue(RequestToken *req_token) {
  ibv_wc wc;
  while (true) {
    int ret;
    // Fast path, some other threads might have polled the CQE
    if (req_token && req_token->IsDone()) {
      return util::Status::OK();
    }
    if ((ret = ibv_poll_cq(this->send_cq_, 1, &wc)) > 0) {
      // Set the work request to be done
      if (wc.wr_id != 0) {
        reinterpret_cast<RequestToken *>(wc.wr_id)->SetDone();
        reinterpret_cast<RequestToken *>(wc.wr_id)->SetStatus(wc.status);
      }
      // Fast Path check
      if (req_token && (uint64_t)req_token == wc.wr_id) {
        if (req_token->ok()) {
          return util::Status::OK();
        } else {
          return util::Status(util::kNetworkError, "Write request failed: status: %d",
                              req_token->wc_status_);
        }
      }
    }
    if (ret < 0) {
      return util::Status(util::kNetworkError, errno, "Poll completion queue failed");
    }
  }
}

util::Status QueuePair::PollCompletionQueueOnce(RequestToken *req_token) {
  ibv_wc wc;
  if (req_token && req_token->IsDone()) {
    return util::Status::OK();
  }
  if (ibv_poll_cq(this->send_cq_, 1, &wc) > 0) {
    // Set the work request to be done
    if (wc.wr_id != 0) {
      reinterpret_cast<RequestToken *>(wc.wr_id)->SetDone();
      reinterpret_cast<RequestToken *>(wc.wr_id)->SetStatus(wc.status);
    }
    // Fast Path check
    if (req_token && (uint64_t)req_token == wc.wr_id) {
      if (req_token->ok()) {
        return util::Status::OK();
      } else {
        return util::Status(util::kNetworkError, "Write request failed: status: %d",
                            req_token->wc_status_);
      }
    }
  }
  return util::Status(util::kNetworkError, errno, "Completition queue has no element");
}

util::Status QueuePair::PollRecvCompletionQueue(RequestToken *req_token) {
  ibv_wc wc;
  while (true) {
    int ret;
    // Fast path, some other threads might have polled the CQE
    if (req_token && req_token->IsDone()) {
      return util::Status::OK();
    }
    if ((ret = ibv_poll_cq(this->recv_cq_, 1, &wc)) > 0) {
      // For every polled completion queue, set the data.
      if (wc.wr_id != 0) {
        auto rq = reinterpret_cast<RequestToken *>(wc.wr_id);
        rq->done_ = true;
        rq->wc_status_ = wc.status;
        if (wc.wc_flags & IBV_WC_WITH_IMM) {
          rq->imm_data_ = ntohl(wc.imm_data);
        }
      }
      // Fast Path check
      if (req_token && (uint64_t)req_token == wc.wr_id) {
        if (req_token->ok()) {
          return util::Status::OK();
        } else {
          return util::Status(util::kNetworkError, "Write request failed: status: %d",
                              req_token->wc_status_);
        }
      }
    }
    if (ret < 0) {
      return util::Status(util::kNetworkError, errno, "Polling completition queue failed");
    }
  }
}

util::Status QueuePair::PollRecvCompletionQueueOnce(RequestToken *req_token) {
  ibv_wc wc;
  int ret = 0;
  if (req_token && req_token->IsDone()) {
    return util::Status::OK();
  }
  if ((ret = ibv_poll_cq(this->recv_cq_, 1, &wc)) > 0) {
    // Set the work request to be done
    if (wc.wr_id != 0) {
      auto rq = reinterpret_cast<RequestToken *>(wc.wr_id);
      rq->done_ = true;
      rq->wc_status_ = wc.status;
      if (wc.wc_flags & IBV_WC_WITH_IMM) {
        rq->imm_data_ = ntohl(wc.imm_data);
      }
    }
    // Fast Path check
    if (req_token && (uint64_t)req_token == wc.wr_id) {
      if (req_token->ok()) {
        return util::Status::OK();
      } else {
        return util::Status(util::kNetworkError, "Write request failed: status: %d",
                            req_token->wc_status_);
      }
    }
  }
  return util::Status(util::kNetworkError, errno, "Polling completition queue failed");
}

util::Status QueuePair::Write(void *laddr, size_t sz, void *raddr, RequestToken *token, int flags) {
  // kutil::LatencyGuard guard(
  //     [&](kutil::Timer *timer) {
  //     this->metrics_[kWrite]->Add(timer->ns_elapse()); });
  assert(token);
  token->SetUndone();
  auto s = PostWrite(laddr, sz, raddr, token);
  if (!s.ok()) {
    return s;
  }
  s = PollCompletionQueue(token);
  return s;
}

util::Status QueuePair::Read(void *raddr, size_t sz, void *laddr, RequestToken *token, int flags) {
  // kutil::LatencyGuard guard(
  //     [&](kutil::Timer *timer) {
  //     this->metrics_[kRead]->Add(timer->ns_elapse()); });
  assert(token);
  token->SetUndone();
  auto s = PostRead(raddr, sz, laddr, token);
  if (!s.ok()) {
    return s;
  }
  s = PollCompletionQueue(token);
  return s;
}

util::Status QueuePair::CompareAndSwap(void *raddr, void *laddr, uint64_t compare, uint64_t swap,
                                       RequestToken *token, int flags) {
  // kutil::LatencyGuard guard(
  //     [&](kutil::Timer *timer) {
  //     this->metrics_[kCAS]->Add(timer->ns_elapse()); });
  assert(token);
  token->SetUndone();
  auto s = PostCompareAndSwap(raddr, laddr, compare, swap, token);
  if (!s.ok()) {
    return s;
  }
  s = PollCompletionQueue(token);
  return s;
}

util::Status QueuePair::MaskedCompareAndSwap(void *raddr, void *laddr, uint64_t compare,
                                             uint64_t swap, uint64_t mask, RequestToken *token,
                                             int flags) {
  assert(token);
  token->SetUndone();
  auto s = PostMaskedCompareAndSwap(raddr, laddr, compare, swap, mask, token, flags);
  if (!s.ok()) {
    return s;
  }
  s = PollCompletionQueue(token);
  return s;
}

util::Status QueuePair::FetchAndAdd(void *raddr, void *laddr, uint64_t add, RequestToken *token,
                                    int flags) {
  // kutil::LatencyGuard guard(
  //     [&](kutil::Timer *timer) {
  //     this->metrics_[kFAA]->Add(timer->ns_elapse()); });
  assert(token);
  token->SetUndone();
  auto s = PostFetchAndAdd(raddr, laddr, add, token);
  if (!s.ok()) {
    return s;
  }
  s = PollCompletionQueue(token);
  return s;
}

util::Status QueuePair::PostIBVRequest(ibv_send_wr *wr, ibv_send_wr **bad_wr) {
  int ret = 0;
  if (ret = ibv_post_send(ibv_qp_, wr, bad_wr), ret) {
    return util::Status(util::kNetworkError, errno, "ibv_post_send failed: ret = %d", ret);
  }
  return util::Status::OK();
}

util::Status QueuePair::PostExpIBVRequest(ibv_exp_send_wr *wr, ibv_exp_send_wr **bad_wr) {
  int ret = 0;
  if (ret = ibv_exp_post_send(ibv_qp_, wr, bad_wr), ret) {
    return util::Status(util::kNetworkError, errno, "ibv_post_send failed: ret = %d", ret);
  }
  return util::Status::OK();
}

ibv_qp_init_attr QueuePair::DefaultQPInitAttr() {
  ibv_qp_init_attr qp_init_attr;
  memset(&qp_init_attr, 0, sizeof(qp_init_attr));

  qp_init_attr.send_cq = this->send_cq_;
  qp_init_attr.recv_cq = this->recv_cq_;
  qp_init_attr.srq = nullptr;
  qp_init_attr.cap.max_send_wr = 4096;
  qp_init_attr.cap.max_recv_wr = 4096;
  qp_init_attr.cap.max_send_sge = 1;
  qp_init_attr.cap.max_recv_sge = 1;
  qp_init_attr.cap.max_inline_data = RdmaConfig::MaxDoorbellInlineSize;
  qp_init_attr.qp_type = IBV_QPT_RC;
  qp_init_attr.sq_sig_all = 0;

  return qp_init_attr;
}

ibv_exp_qp_init_attr QueuePair::DefaultExpQPInitAttr(Context *ctx) {
  ibv_exp_qp_init_attr attr;
  memset(&attr, 0, sizeof(attr));

  attr.send_cq = this->send_cq_;
  attr.recv_cq = this->recv_cq_;
  attr.srq = nullptr;
  attr.cap.max_send_wr = 1024;
  attr.cap.max_recv_wr = 1024;
  attr.cap.max_send_sge = 1;
  attr.cap.max_recv_sge = 1;
  attr.qp_type = IBV_QPT_RC;
  attr.sq_sig_all = 0;
  attr.pd = ctx->get_ib_pd();

  attr.comp_mask = IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS | IBV_EXP_QP_INIT_ATTR_PD |
                   IBV_EXP_QP_INIT_ATTR_ATOMICS_ARG;
  attr.max_atomic_arg = 32;
  return attr;
}

ibv_qp_attr QueuePair::DefaultQPAttrForInit() {
  ibv_qp_attr qp_attr;
  memset(&qp_attr, 0, sizeof(qp_attr));

  qp_attr.qp_state = IBV_QPS_INIT;
  qp_attr.pkey_index = 0;
  qp_attr.port_num = this->ctx_->get_dev_port();
  qp_attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
                            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
  return qp_attr;
}

ibv_qp_attr QueuePair::DefaultQPAttrForRTR(const SerializedQueuePair *qp_info) {
  ibv_qp_attr ret;
  std::memset(&ret, 0, sizeof(ibv_qp_attr));

  ret.qp_state = IBV_QPS_RTR;

  // Match this attribute with your device
  ret.path_mtu = IBV_MTU_4096;

  ret.dest_qp_num = qp_info->qp_num;
  ret.rq_psn = 0;
  ret.max_dest_rd_atomic = 16;
  ret.min_rnr_timer = 12;

  ret.ah_attr.is_global = 0;
  ret.ah_attr.dlid = qp_info->lid;
  ret.ah_attr.sl = 0;
  ret.ah_attr.src_path_bits = 0;
  ret.ah_attr.port_num = this->ctx_->get_dev_port();

  // For RDMA on RoCE, the grh (Global Route Header) field must be configured
  // see: https://www.rdmamojo.com/2013/01/12/ibv_modify_qp/
  if (ctx_->get_gid_index() >= 0) {
    ret.ah_attr.is_global = 1;
    std::memcpy(&ret.ah_attr.grh.dgid, qp_info->gid, 16);
    ret.ah_attr.grh.flow_label = 0;
    ret.ah_attr.grh.hop_limit = 1;
    ret.ah_attr.grh.sgid_index = ctx_->get_gid_index();
    ret.ah_attr.grh.traffic_class = 0;
  }

  return ret;
}

std::unique_ptr<ibv_qp_attr> QueuePair::DefaultQPAttrForRTS(const SerializedQueuePair *qp_info) {
  auto attr = std::make_unique<struct ibv_qp_attr>();
  memset(attr.get(), 0, sizeof(struct ibv_qp_attr));

  attr->qp_state = IBV_QPS_RTS;
  attr->timeout = 0x12;  // 18
  attr->retry_cnt = 6;
  attr->rnr_retry = 0;
  attr->sq_psn = 0;
  attr->max_rd_atomic = 16;
  return attr;
}

};  // namespace rdma
