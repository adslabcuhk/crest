#pragma once

#include <infiniband/verbs.h>
#include <cstring>

namespace rdma {

// Configuration class for all RDMA-related classes 
class RdmaConfig {
 public:
  static constexpr size_t PageSize = 4096;

  // Polling threshold for each RDMA operation. 
  static size_t PollTimeoutThresMicroseconds;

  static constexpr int MRRegisterFlag = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE |
                                        IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC;
  
  static constexpr size_t MaxDoorbellInlineSize = 124;
};
}  // namespace rdma