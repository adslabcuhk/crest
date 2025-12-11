#pragma once

#include <infiniband/verbs.h>
#include <infiniband/verbs_exp.h>

#include <cstdint>
#include <string>

#include "util/Status.h"

namespace rdma {

class RDMADevice;

class Context {
  static constexpr uint16_t kInvalidDeviceId = -1;

 public:
  // Create a context from the device id and port
  Context(uint32_t dev_id, uint32_t port, int gid_idx = -1)
      : dev_id_(dev_id), dev_port_(port), gid_idx_(gid_idx) {}

  Context(const std::string &devname, uint32_t port, int gid_idx = -1)
      : devname_(devname), dev_port_(port), gid_idx_(gid_idx) {}

  ~Context();

  // Copy constructor is not allowed
  Context(const Context &) = delete;
  Context &operator=(const Context &) = delete;

 public:
  // Initialize the fields of this context, including the ibv_device,
  // ibv_pd. Return Ok if initialization succeeds, return the corresponding
  // error code if any error happens.
  util::Status Init(uint32_t cqe = 16534);

  const ibv_device_attr *get_device_attr() const { return &dev_attr_; }
  const ibv_device_attr_ex *get_extended_device_attr() const { return &dev_attr_ex_; }

 public:
  ibv_context *get_ib_context() const { return ibv_ctx_; }

  ibv_pd *get_ib_pd() const { return ibv_pd_; }

  uint16_t get_dev_port() const { return dev_port_; }

  uint16_t get_local_device_id() const { return local_dev_id_; }

  uint8_t *get_gid_addr() { return gid_; }

  int get_gid_index() { return gid_idx_; }

 private:
  // These fields are only initialized for once and can not be modified
  // after being set.
  ibv_context *ibv_ctx_ = nullptr;

  ibv_pd *ibv_pd_ = nullptr;

  // Uniquely identifying the device and the port opened
  ibv_device *ibv_dev_ = nullptr;

  ibv_device **dev_lists_ = nullptr;

  std::string devname_;

  uint16_t dev_id_ = kInvalidDeviceId, dev_port_;

  uint16_t local_dev_id_ = kInvalidDeviceId;

  // Gid related fields
  int gid_idx_ = -1;
  uint8_t gid_[16];

  // Attributes to query
  ibv_device_attr dev_attr_;

  ibv_device_attr_ex dev_attr_ex_;
};
};  // namespace rdma