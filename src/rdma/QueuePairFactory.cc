
#include "rdma/QueuePairFactory.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstring>

#include "rdma/Context.h"
#include "rdma/MemoryRegion.h"
#include "rdma/QueuePair.h"
#include "util/Guard.h"
#include "util/Logger.h"
#include "util/Status.h"

namespace rdma {

util::Status QueuePairFactory::InitAndBind(uint16_t port) {
  // Create the socket object
  this->server_socket_ = socket(AF_INET, SOCK_STREAM, 0);
  if (this->server_socket_ < 0) {
    return util::Status(util::kNetworkError, errno, "Create socket error");
  }

  sockaddr_in serverAddress;
  memset(&(serverAddress), 0, sizeof(sockaddr_in));
  serverAddress.sin_family = AF_INET;
  serverAddress.sin_port = htons(port);

  int32_t enabled = 1;
  auto ret = setsockopt(this->server_socket_, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled));
  if (ret < 0) {
    return util::Status(util::kNetworkError, errno, "Set socket option error");
  }

  ret = bind(this->server_socket_, (sockaddr *)&serverAddress, sizeof(sockaddr_in));

  if (ret < 0) {
    return util::Status(util::kNetworkError, errno, "Bind socket failed");
  }

  ret = listen(this->server_socket_, 128);
  if (ret < 0) {
    return util::Status(util::kNetworkError, errno, "Listen on socket %d failed",
                        this->server_socket_);
  }
  return util::Status::OK();
}

QueuePair *QueuePairFactory::WaitForIncomingConnection(Context *ctx, MemoryRegionToken mr_token) {
  auto recv_buffer = new char[sizeof(SerializedQueuePair)];
  auto send_buffer = new char[sizeof(SerializedQueuePair)];

  // Get the socket of connection when there is incoming request
  int connect_socket = accept(this->server_socket_, (sockaddr *)nullptr, nullptr);
  if (connect_socket < 0) {
    LOG_ERROR("Socket accept error: %d", connect_socket);
    return nullptr;
  }

  util::Guard g([&]() {
    delete[] recv_buffer;
    delete[] send_buffer;
    close(connect_socket);
  });

  // The queue pair object created for this connection
  auto qp = new QueuePair(ctx, mr_token);

  // Write local queue pair information to remote node
  SerializedQueuePair *local_qp = reinterpret_cast<SerializedQueuePair *>(send_buffer);
  SerializedQueuePair *remote_qp = reinterpret_cast<SerializedQueuePair *>(recv_buffer);

  local_qp->lid = ctx->get_local_device_id();
  std::memcpy(local_qp->gid, ctx->get_gid_addr(), 16);
  local_qp->qp_num = qp->GetQueuePairNumber();
  local_qp->mr_token = qp->GetLocalMemoryRegionToken();

  // Read queue pair information formation from incomming request
  if (auto s = ExchangeQueuePairInfo(local_qp, remote_qp, connect_socket, true); !s.ok()) {
    return nullptr;
  }

  // logger::LOG(logger::kYellow, "WaitForIncommingConnection: recv data: %s",
  //             remote_qp->ToString().c_str());

  // Init and activate the queue pair
  if (auto s = qp->Init(); !s.ok()) {
    LOG_ERROR("QueuePair initialized Failed");
    return nullptr;
  }

  if (auto s = qp->Activate(remote_qp); !s.ok()) {
    LOG_FATAL("QueuePair Activate Failed");
    return nullptr;
  }

  qp->rmr_token_ = remote_qp->mr_token;
  return qp;
}

util::Status QueuePairFactory::ConnectToRemoteHost(std::string ip, uint16_t port, Context *ctx,
                                                   MemoryRegionToken mr_token, QueuePair **qp) {
  // assert(qp != nullptr);

  // Create a new socket and connect to remote QP
  sockaddr_in remote_addr;
  memset(&(remote_addr), 0, sizeof(sockaddr_in));
  remote_addr.sin_family = AF_INET;
  inet_pton(AF_INET, ip.c_str(), &(remote_addr.sin_addr));
  remote_addr.sin_port = htons(port);

  int connect_socket = socket(AF_INET, SOCK_STREAM, 0);
  if (connect_socket < 0) {
    *qp = nullptr;
    return util::Status(util::kNetworkError, errno, "Create socket failed");
  }

  int ret_value = connect(connect_socket, (sockaddr *)&(remote_addr), sizeof(sockaddr_in));
  if (ret_value < 0) {
    *qp = nullptr;
    return util::Status(util::kNetworkError, errno, "Connect to remote socket (%s, %d) failed",
                        ip.c_str(), port);
  }

  auto recv_buffer = new char[sizeof(SerializedQueuePair)];
  auto send_buffer = new char[sizeof(SerializedQueuePair)];

  util::Guard g([&]() {
    delete[] recv_buffer;
    delete[] send_buffer;
    close(connect_socket);
  });

  // The queue pair object created for this connection
  auto new_qp = new QueuePair(ctx, mr_token);
  auto local_qp = reinterpret_cast<SerializedQueuePair *>(send_buffer);
  auto remote_qp = reinterpret_cast<SerializedQueuePair *>(recv_buffer);

  local_qp->qp_num = new_qp->GetQueuePairNumber();
  std::memcpy(local_qp->gid, ctx->get_gid_addr(), 16);  // write the gid
  local_qp->lid = ctx->get_local_device_id();
  local_qp->mr_token = new_qp->GetLocalMemoryRegionToken();

  util::Status s;

  if (auto s = ExchangeQueuePairInfo(local_qp, remote_qp, connect_socket, false); !s.ok()) {
    delete new_qp;
    *qp = nullptr;
    return s;
  }

  if (s = new_qp->Init(); !s.ok()) {
    delete new_qp;
    *qp = nullptr;
    return s;
  }

  if (s = new_qp->Activate(remote_qp); !s.ok()) {
    delete new_qp;
    *qp = nullptr;
    return s;
  }
  new_qp->rmr_token_ = remote_qp->mr_token;
  *qp = new_qp;
  return util::Status::OK();
}

util::Status QueuePairFactory::ExchangeQueuePairInfo(SerializedQueuePair *local_info,
                                                     SerializedQueuePair *remote_info, int socket,
                                                     bool is_server) {
  if (!is_server) {
    // For client: First send information to remote server and wait for response
    auto wr_sz = send(socket, local_info, sizeof(SerializedQueuePair), 0);
    if (wr_sz < sizeof(SerializedQueuePair)) {
      return util::Status(util::kNetworkError,
                          "socket send error (size mismatch): expect %lu, write %d",
                          sizeof(SerializedQueuePair), wr_sz);
    }

    auto rd_sz = recv(socket, remote_info, sizeof(SerializedQueuePair), 0);
    if (rd_sz < sizeof(SerializedQueuePair)) {
      return util::Status(util::kNetworkError,
                          "socket recv error (size mismatch): expect %lu, recv %d",
                          sizeof(SerializedQueuePair), rd_sz);
    }
  } else {
    // For server: wait for incoming information
    auto rd_sz = recv(socket, remote_info, sizeof(SerializedQueuePair), 0);
    if (rd_sz < sizeof(SerializedQueuePair)) {
      return util::Status(util::kNetworkError,
                          "socket recv error (size mismatch): expect %lu, recv %d",
                          sizeof(SerializedQueuePair), rd_sz);
    }

    auto wr_sz = send(socket, local_info, sizeof(SerializedQueuePair), 0);
    if (wr_sz < sizeof(SerializedQueuePair)) {
      return util::Status(util::kNetworkError,
                          "socket send error (size mismatch): expect %lu, write %d",
                          sizeof(SerializedQueuePair), wr_sz);
    }
  }
  return util::Status::OK();
}

};  // namespace rdma
