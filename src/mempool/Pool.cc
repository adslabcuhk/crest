
#include "mempool/Pool.h"

#include <cstddef>

#include "common/Type.h"
#include "mempool/PoolMeta.h"
#include "rdma/Context.h"
#include "rdma/MemoryRegion.h"
#include "rdma/QueuePair.h"
#include "rdma/QueuePairFactory.h"
#include "util/Logger.h"
#include "util/Status.h"

Status Pool::CreateLocalMemoryRegion() {
  ctx_ = new rdma::Context(node_info_.devname, node_info_.ib_port, node_info_.gid_idx);
  Status s = ctx_->Init();
  if (!s.ok()) {
    return s;
  }
  mr_ = new rdma::MemoryRegion(ctx_, node_info_.mr_size);
  mr_owner_ = true;
  LOG_INFO("Memory register and initialization finished");
  return Status::OK();
}

Status Pool::BuildConnection(rdma::Context *ctx, rdma::MemoryRegion *mr,
                             const std::vector<RemoteNodeAttr> &contact_info, int qp_per_node) {
  rdma::MemoryRegionToken mr_token = mr->GetMemoryRegionToken();
  rdma::QueuePairFactory qp_factory;

  // meta_arr_ = (MemoryNodeMeta *)mr_token.get_region_addr();

  if (IsCN()) {
    // Connect to each remote node
    for (const auto &info : contact_info) {
      rdma::QueuePair *qp;
      Status s = qp_factory.ConnectToRemoteHost(info.ip, info.socket_port, ctx, mr_token, &qp);
      if (unlikely(!s.ok())) {
        return s;
      }
      qps_[info.nid] = qp;
      LOG_DEBUG("CN%u establish connection to MN%u succeed", MyId(), info.nid);
    }
    this->mr_ = mr;
  } else if (IsMN()) {
    LOG_INFO("MN%u waits for incomming connection", node_info_.nid);

    Status s = qp_factory.InitAndBind(node_info_.socket_port);
    ASSERT(s.ok(), "MN%u connection init Failed", MyId());

    for (int i = 0; i < contact_info.size(); ++i) {
      for (int i = 0; i < qp_per_node; ++i) {
        rdma::QueuePair *qp = qp_factory.WaitForIncomingConnection(ctx, mr_token);
        ASSERT(qp, "MN%u waits for connection failed", MyId());
        (void)qp;
      }
      LOG_INFO("MN%u establishes connection with CN%u succeed", MyId(), contact_info[i].nid);
    }
  }
  return Status::OK();
}

Status Pool::BuildConnection(const std::vector<RemoteNodeAttr> &contact_info, int qp_per_node) {
  return BuildConnection(ctx_, mr_, contact_info, qp_per_node);
}

Status Pool::FetchMetaData(node_id_t nid) {
  rdma::QueuePair *qp = GetQueuePair(nid);
  void *remote_meta_addr = (void *)(qp->GetRemoteMemoryRegionToken().get_region_addr());
  // auto s = qp->Read(remote_meta_addr, sizeof(MemoryNodeMeta), &meta_arr_[nid],
  //                   (RequestToken *)(MAX_CORO_NUM));
  // if (!s.ok()) {
  //   return s;
  // }
  return Status::OK();
}

Status Pool::AcquireMemory(size_t sz, node_id_t nid, PoolPtr *ptr) {
  // util::Timer timer;
  // timer.reset();
  // rdma::QueuePair *qp = GetQueuePair(nid);
  // MemoryNodeMeta *meta = &meta_arr_[nid];
  // uint64_t remote_meta_addr =
  //     qp->GetRemoteMemoryRegionToken().get_region_addr();
  // uint64_t old_offset = meta->offset;
  // auto s = qp->FetchAndAdd(
  //     (void *)(remote_meta_addr + offsetof(MemoryNodeMeta, offset)),
  //     &(meta->offset), kAcquireChunkSize, (RequestToken *)(MAX_CORO_NUM));
  // if (!s.ok()) {
  //   return s;
  // }

  // // Wait until the data has been received
  // while (meta->offset == old_offset && timer.s_elapse() < 1) {
  //   ;
  // }
  // if (meta->offset == old_offset) [[unlikely]] {
  //   return Status(kNetworkError, "Acquire memory request timeout");
  // }
  // *ptr = MakePoolPtr(nid, meta->offset);
  // recordPoolAllocationTime(timer.us_elapse());
  return Status::OK();
}
