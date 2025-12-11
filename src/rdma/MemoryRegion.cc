
#include <infiniband/verbs.h>

#include <cstddef>
#include <cstdlib>
#include <cstring>

#include "rdma/MemoryRegion.h"
#include "rdma/RdmaConfig.h"
#include "rdma/Context.h"
#include "util/Logger.h"

namespace rdma {

MemoryRegionToken::MemoryRegionToken(const MemoryRegion *mr)
    : region_addr_(mr->get_address()),
      region_size_(mr->size()),
      lkey_(mr->get_local_key()),
      rkey_(mr->get_remote_key()) {}

MemoryRegionToken::MemoryRegionToken(uint64_t region_addr, uint64_t region_size, uint32_t lkey,
                                     uint32_t rkey)
    : region_addr_(region_addr), region_size_(region_size), lkey_(lkey), rkey_(rkey) {}

MemoryRegion::MemoryRegion(Context *ctx, size_t alloc_sz, bool on_chip, int flags)
    : ctx_(ctx), buf_sz_(alloc_sz) {
  // Allocate the local memory buffer
  if (!on_chip) {
    auto s = posix_memalign(&buf_addr_, RdmaConfig::PageSize, alloc_sz);
    if (s || buf_addr_ == nullptr) {
      abort();
    }

    std::memset(buf_addr_, 0, alloc_sz);

    this->ibv_mr_ = ibv_reg_mr(ctx->get_ib_pd(), this->buf_addr_, this->buf_sz_, flags);
    if (!this->ibv_mr_) {
      abort();
    }
    buf_owner_ = true;
  } else {
    // May allocate memory from the on-chip part
    // if (ctx->get_extended_device_attr()->max_dm_size < alloc_sz) {
    //   return;
    // }

    // ibv_alloc_dm_attr attr;
    // std::memset(&attr, 0, sizeof(attr));
    // attr.length = alloc_sz;

    // this->ibv_dm_ = ibv_alloc_dm(ctx->get_ib_context(), &attr);
    // if (!ibv_dm_) {
    //   return;
    // }

    // // register this device memory as memory region
    // this->ibv_mr_ =
    //     ibv_reg_dm_mr(ctx->get_ib_pd(), this->ibv_dm_, 0, alloc_sz, flags |
    //     IBV_ACCESS_ZERO_BASED);
    // if (!this->ibv_mr_) {
    //   LOG_ERROR("Register device memory Error: %s", strerror(errno));
    //   return;
    // }
  }
}

MemoryRegion::MemoryRegion(Context *ctx, void *buf_addr, size_t buf_sz, int flags)
    : ctx_(ctx), buf_addr_(buf_addr), buf_sz_(buf_sz) {
  this->ibv_mr_ = ibv_reg_mr(ctx->get_ib_pd(), this->buf_addr_, this->buf_sz_, flags);
}

MemoryRegion::~MemoryRegion() {
  ibv_dereg_mr(this->ibv_mr_);
  if (this->ibv_dm_) {
    ibv_exp_free_dm(this->ibv_dm_);
  }
  if (buf_owner_) {
    free(buf_addr_);
  }
  LOG_INFO("release memory region");
}

};  // namespace rdma