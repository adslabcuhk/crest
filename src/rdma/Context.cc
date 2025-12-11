
#include <infiniband/verbs.h>

#include <cerrno>
#include <cstring>
#include "rdma/Context.h"

#include "util/Status.h"

namespace rdma {

util::Status Context::Init(uint32_t cqe) {
  (void)cqe;

  int dev_num = 0;
  this->dev_lists_ = ibv_get_device_list(&dev_num);

  // No IB device detected
  if (this->dev_lists_ == nullptr || dev_num <= 0) {
    return util::Status(util::kNetworkError, "No IB device detected");
  }

  // Given the index of the target device in the device list
  if (dev_id_ != kInvalidDeviceId) {
    if (dev_id_ >= dev_num) {
      return util::Status(util::kNetworkError, "No specified ib device %d", dev_id_);
    }
    this->ibv_dev_ = dev_lists_[dev_id_];
    this->devname_ = ibv_get_device_name(this->ibv_dev_);
  } else {
    // Traverse the device list to find the device which matches the devname
    for (int i = 0; i < dev_num; ++i) {
      auto name = ibv_get_device_name(dev_lists_[i]);
      if (name && (strcmp(this->devname_.c_str(), name) == 0)) {
        this->ibv_dev_ = dev_lists_[i];
        break;
      }
    }
  }
  if (this->ibv_dev_ == nullptr) {
    return util::Status(util::kNetworkError, "Open IB device error");
  }

  // Open the device to get the context
  this->ibv_ctx_ = ibv_open_device(this->ibv_dev_);
  if (this->ibv_ctx_ == nullptr) {
    return util::Status(util::kNetworkError, errno, "Create ibv_context failed");
  }
  // Allocate the protection domain
  this->ibv_pd_ = ibv_alloc_pd(this->ibv_ctx_);
  if (this->ibv_pd_ == nullptr) {
    return util::Status(util::kNetworkError, errno, "Create protection domain failed");
  }

  // Get the LID
  ibv_port_attr port_attributes;
  if (ibv_query_port(this->ibv_ctx_, this->dev_port_, &port_attributes)) {
    return util::Status(util::kNetworkError, errno, "Query IB port failed");
  }
  this->local_dev_id_ = port_attributes.lid;

  // Get the gid if necessary
  union ibv_gid tmp_gid;
  if (gid_idx_ >= 0) {
    if (ibv_query_gid(this->ibv_ctx_, dev_port_, this->gid_idx_, &tmp_gid)) {
      return util::Status(util::kNetworkError, errno, "IB Query Gid failed: queried gid=%d",
                          gid_idx_);
    }
    std::memcpy(gid_, &tmp_gid, 16);
  }

  // Get the device related attributes
  if (ibv_query_device(this->ibv_ctx_, &this->dev_attr_) != 0) {
    return util::Status(util::kNetworkError, errno, "Query device attributes failed");
  }

  if (ibv_query_device_ex(this->ibv_ctx_, nullptr, &this->dev_attr_ex_)) {
    return util::Status(util::kNetworkError, errno, "Query extended device attributes failed");
  }

  return util::Status::OK();
}

};  // namespace rdma
