#pragma once

#include "common/Type.h"
#include <string>

enum PoolNodeType { kCN = 0, kMN = 1, kNone = 2 };

// Necessary information for a node to initialize its local memory and
// pool manager.
struct NodeInitAttr {
  PoolNodeType node_type; // Type of this node
  node_id_t nid;          // Node id
  std::string devname;    // The RNIC device use to create memory region
  uint32_t ib_port;       // RDMA port to use
  int gid_idx;            // gindex 
  size_t mr_size;         // The memory region size
  std::string ip;         // The ip address of this node
  uint16_t socket_port;   // The socket port used to exchange qp information
  int num_mns;            // Number of MNs    
  int num_cns;            // Number of CNs

  static constexpr uint16_t MEMCACHED_PORT = 12345;
};

// Necessary information to contact a remote node
struct RemoteNodeAttr {
  PoolNodeType node_type;
  node_id_t nid;
  std::string ip;
  uint16_t socket_port;
};