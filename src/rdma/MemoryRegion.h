#pragma once

#include <infiniband/verbs.h>
// #include <infiniband/verbs_exp.h>

#include <cstring>
#include <sstream>
#include <string>

#include "rdma/RdmaConfig.h"

namespace rdma {

class Context;
class MemoryRegion;

class MemoryRegionToken {
 public:
  MemoryRegionToken() = default;
  MemoryRegionToken(const MemoryRegion *mr);
  MemoryRegionToken(uint64_t region_addr, uint64_t region_size, uint32_t lkey, uint32_t rkey);

  uint64_t get_region_addr() const { return region_addr_; }
  uint64_t get_region_size() const { return region_size_; }
  uint64_t get_local_key() const { return lkey_; }
  uint64_t get_remote_key() const { return rkey_; }
  uint64_t get_region_bound() const { return region_addr_ + region_size_; }

  uint64_t GetOffset(uint64_t addr) const { return addr - region_addr_; }
  uint64_t GetOffset(void *addr) const { return (uint64_t)addr - region_addr_; }

  // For debug purpose
  std::string ToString() const {
    std::stringstream ss;
    ss << "addr: " << std::hex << region_addr_ << " "
       << "size: " << std::dec << region_size_ << " "
       << "lkey: " << std::dec << lkey_ << " "
       << "rkey: " << std::dec << rkey_ << " ";
    return ss.str();
  }

 private:
  uint64_t region_addr_ = 0;
  uint64_t region_size_ = 0;
  uint32_t lkey_, rkey_;
};

class MemoryRegion {
 public:
  // Initialize the memory region with specified region size.
  MemoryRegion(Context *ctx, size_t alloc_sz, bool on_chip = false,
               int reg_flags = RdmaConfig::MRRegisterFlag);

  MemoryRegion(Context *ctx, void *buf_addr, size_t buf_sz,
               int reg_flags = RdmaConfig::MRRegisterFlag);

  ~MemoryRegion();

 public:
  uint32_t get_local_key() const { return ibv_mr_->lkey; }

  uint32_t get_remote_key() const { return ibv_mr_->rkey; }

  uint64_t get_address() const { return reinterpret_cast<uint64_t>(ibv_mr_->addr); }

  uint64_t get_address(uint64_t offset) const { return get_address() + offset; }

  void *data() const { return buf_addr_; }

  size_t size() const { return ibv_mr_->length; }

  bool on_chip() const { return ibv_dm_ != nullptr; }

  MemoryRegionToken GetMemoryRegionToken() { return MemoryRegionToken(this); }

 private:
  Context *ctx_;

  ibv_mr *ibv_mr_;

  ibv_exp_dm *ibv_dm_ = nullptr;

  // Local memory buffer of this region
  void *buf_addr_;

  size_t buf_sz_;

  bool buf_owner_ = false;  // If the object owns this region
};

};  // namespace rdma
