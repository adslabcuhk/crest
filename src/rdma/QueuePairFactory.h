#pragma once

#include <sstream>
#include <string>

#include "rdma/MemoryRegion.h"
#include "util/Status.h"

namespace rdma {
class QueuePair;
class Context;

// For exchanging the queue pair information
struct SerializedQueuePair {
  uint32_t qp_num;             // queue pair number
  uint16_t lid;                // lid of this port
  uint8_t gid[16];             // mandotory for RoCE
  MemoryRegionToken mr_token;  // Token of memory region

  // For Debug purpose
  std::string ToString() const {
    std::stringstream ss;
    ss << "lid: " << lid << "\n"
       << "qp_num: " << qp_num << "\n"
       << "mr_token: " << mr_token.ToString() << "\n";
    ss << "gid: ";
    for (int i = 0; i < 16; ++i) {
      ss << std::hex << (uint8_t)gid[i] << " ";
    }
    ss << "\n";
    return ss.str();
  }
};

// This factory generates the RDMA queue pair by listening on incoming
// connection request or sending connection request to the target server
class QueuePairFactory {
 public:
  QueuePairFactory() = default;

  // Wait for incomming RMDA connection request. If the connection
  // succeed and the remote server provides useful queue pair information.
  // Then create a QueuePair object to represent this connection.
  QueuePair *WaitForIncomingConnection(Context *ctx, MemoryRegionToken mr_token);

  // Connect to the target host server to create a queue pair. If the connection
  // succeed and the remote server provides useful queue pair information.
  // Then create a QueuePair object to represent this connection.
  // The token of this memory region is also transferred to the remote server
  util::Status ConnectToRemoteHost(std::string ip, uint16_t port, Context *ctx,
                                   MemoryRegionToken token, QueuePair **qp);

  // Bind the socket to the target port
  util::Status InitAndBind(uint16_t port);

  // Exchange the queue pair information with target socket
  static util::Status ExchangeQueuePairInfo(SerializedQueuePair *send, SerializedQueuePair *recv,
                                            int socket, bool is_server);

 private:
  int server_socket_;
};
}  // namespace rdma
