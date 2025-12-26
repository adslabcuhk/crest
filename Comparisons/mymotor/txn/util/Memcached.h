#pragma once

#include "rlib/common.hpp"
#include "rlib/logging.hpp"
#include <libmemcached/memcached.h>

#include <string>
#include <unistd.h>


class MemcachedWrapper {
  const char *SERVER_NUM_KEY = "serverNum";

public:
  MemcachedWrapper(uint32_t max_server_num, int node_id,
                   std::string memcached_ip, uint16_t memcached_port)
      : max_server_num(max_server_num), cur_server_num(0), node_id(node_id),
        ip_addr(memcached_ip), memcached_port(memcached_port) {
    memc = memcached_create(nullptr);
  }

  ~MemcachedWrapper() { DisconnectMemcached(); }

  bool ConnectToMemcached() {
    memcached_server_st *servers = NULL;
    memcached_return rc;

    memc = memcached_create(NULL);
    rc = memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1);
    if (rc != MEMCACHED_SUCCESS) {
      RDMA_LOG(rdmaio::ERROR)
          << "Memcached: Couldn't set behavior, Error Info: "
          << memcached_strerror(memc, rc);
      sleep(1);
      return false;
    }

    servers =
        memcached_server_list_append(servers, ip_addr.c_str(), 11211, &rc);
    rc = memcached_server_push(memc, servers);
    if (rc != MEMCACHED_SUCCESS) {
      RDMA_LOG(rdmaio::ERROR) << "Memcached: Couldn't Add Servers, Error Info: "
                              << memcached_strerror(memc, rc);
      sleep(1);
      return false;
    }

    return true;
  }

  void DisconnectMemcached() {
    if (memc) {
      memcached_quit(memc);
      memcached_free(memc);
      memc = nullptr;
    }
  }

  void AddServer() {
    memcached_return rc;
    uint64_t server_num;

    while (true) {
      rc = memcached_increment(memc, SERVER_NUM_KEY, strlen(SERVER_NUM_KEY), 1,
                               &server_num);
      if (rc == MEMCACHED_SUCCESS) {
        RDMA_LOG(rdmaio::INFO) << "Successfully add Compute Node" << node_id
                               << ", Now: " << server_num;
        return;
      }
      RDMA_LOG(rdmaio::ERROR) << "Server Counld't incr value. ErrorInfo:"
                              << memcached_strerror(memc, rc);
      usleep(10000);
    }
  }

  void ResetServerNum() {
    memcached_return rc;
    const char *server_num = "0";
    // Only the first memory node can reset the server num
    if (node_id == 0) {
      rc = memcached_set(memc, SERVER_NUM_KEY, strlen(SERVER_NUM_KEY),
                         server_num, strlen(server_num), 0, 0);
      if (rc == MEMCACHED_SUCCESS) {
        RDMA_LOG(rdmaio::INFO)
            << "Successfully Set Server Number to be 0:" << node_id << ", "
            << server_num;
        return;
      }
    }
  }

  void SyncComputeNodes() {
    RDMA_LOG(rdmaio::INFO) << "Synchronizing all compute nodes...";
    size_t l;
    uint32_t flags;
    memcached_return rc;
    int curr_server_num = 1;
    // Wait until all servers are ready
    while (curr_server_num < max_server_num) {
      char *serverNumStr = memcached_get(
          memc, SERVER_NUM_KEY, strlen(SERVER_NUM_KEY), &l, &flags, &rc);
      if (rc != MEMCACHED_SUCCESS) {
        RDMA_LOG(rdmaio::ERROR)
            << "Server %d Counld't get serverNum, Error Info: %s, retry\n"
            << 0,
            memcached_strerror(memc, rc);
        continue;
      }
      curr_server_num = atoi(serverNumStr);
      free(serverNumStr);
    }
    RDMA_LOG(rdmaio::INFO) << "Synchronization Done";
  }

private:
  uint32_t max_server_num;
  uint16_t cur_server_num;
  // node_id_t node_id;
  int node_id;
  std::string ip_addr;
  uint16_t memcached_port;
  memcached_st *memc;
};