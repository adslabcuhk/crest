#include <gflags/gflags.h>
#include <infiniband/verbs.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iomanip>
#include <ios>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <random>
#include <sstream>
#include <thread>

#include "common/Config.h"
#include "common/Type.h"
#include "mempool/BufferManager.h"
#include "rdma/Context.h"
#include "rdma/MemoryRegion.h"
#include "rdma/QueuePair.h"
#include "rdma/QueuePairFactory.h"
#include "rdma/RdmaBatch.h"
#include "util/History.h"
#include "util/Macros.h"
#include "util/Statistic.h"
#include "util/Timer.h"

const int kSocketPort = 10001;

// RDMA related parameters
DEFINE_string(devname, "", "The name of the IB device to open");
DEFINE_int32(ibport, 0, "The port of the IB device to use");
DEFINE_int32(gididx, 0, "The GIdx used to specify the device port");
DEFINE_string(ip, "", "The IP address of remote the remote server to connect");
DEFINE_string(type, "", "MN or CN");
DEFINE_int32(access_records, 1, "The number of records to access for each transaction");

DEFINE_uint32(mrsize, 1024 * 1024, "Size of memory region to allocate");
DEFINE_uint32(record_size, 256, "Size of each record");
DEFINE_uint32(ops, 10000000, "Number of operations to execute (not per-thread)");
DEFINE_int32(shared_ctx, 40, "Number of QPS share the same context");
DEFINE_bool(unlock, false, "Whether issue the CAS to unlock records");

// benchmark related parameters
DEFINE_uint32(threads, 1, "Number of threads to concurrently run the benchmarks");
DEFINE_bool(merge_validation, false, "Whether merge validation");
DEFINE_bool(batch, false, "Whether enable batch");
DEFINE_string(lock, "cas", "Use CAS lock or write lock");

static const int kMaxQPNum = 512;
static const int kMaxAccessRecordNum = 16;
static rdma::QueuePair* bench_qps[kMaxQPNum];

int rdma_ops[64];
int rdma_atomic_ops[64];
int txn_ops[64];

using namespace util;

Status InitializeAllQueuePairs() {
  std::cout << "[Wait for create QueuePair...]" << std::endl;
  static rdma::QueuePairFactory qp_factory;
  Status s;

  auto ctx = new rdma::Context(FLAGS_devname, FLAGS_ibport, FLAGS_gididx);
  s = ctx->Init();
  if (!s.ok()) {
    return s;
  }

  rdma::MemoryRegion* mr = new rdma::MemoryRegion(ctx, FLAGS_mrsize, false);
  auto mr_token = mr->GetMemoryRegionToken();
  printf("Create memory region with: %p, size %lu\n", (void*)mr_token.get_region_addr(),
         mr_token.get_region_size());
  char* mr_addr = (char*)mr->GetMemoryRegionToken().get_region_addr();
  size_t mr_size = mr->GetMemoryRegionToken().get_region_size();

  rdma::Context* current_ctx = ctx;
  rdma::MemoryRegion* current_mr = mr;

  if (FLAGS_type == "mn") {
    s = qp_factory.InitAndBind(kSocketPort);
    if (!s.ok()) {
      return s;
    }
    while (true) {
      auto qp = qp_factory.WaitForIncomingConnection(ctx, mr_token);
      if (qp != nullptr) {
        std::cout << "[Create QueuePair succeed !!!]" << std::endl;
      }
    }
    return Status::OK();
  } else {
    rdma::QueuePair* qp;
    for (int i = 0; i < FLAGS_threads; ++i) {
      if (i > 0 && i % FLAGS_shared_ctx == 0) {
        current_ctx = new rdma::Context(FLAGS_devname, FLAGS_ibport, FLAGS_gididx);
        s = current_ctx->Init();
        if (!s.ok()) {
          return s;
        }
        current_mr = new rdma::MemoryRegion(current_ctx, mr_addr, mr_size);
      }
      s = qp_factory.ConnectToRemoteHost(FLAGS_ip, kSocketPort, current_ctx,
                                         current_mr->GetMemoryRegionToken(), &qp);
      if (!s.ok()) {
        return s;
      }
      bench_qps[i] = qp;
    }
    std::cout << "[Create QueuePair succeed !!!]" << std::endl;
    return util::Status::OK();
  }
}

void RunMN() {
  std::cout << "[MN running server node]" << std::endl;
  InitializeAllQueuePairs();
  char c;
  std::cout << "[Print any key to exit]\n";
  std::cin >> c;
}

struct ExecResult {
  uint32_t read_dura;
  uint32_t validate_dura;
  uint32_t commit_dura;
  uint32_t cpu_dura;
};

void Poll(rdma::QueuePair* qp, int req_num) {
  static constexpr int kMaxPollNumOnce = 32;
  int polled = 0;
  ibv_wc wc[kMaxPollNumOnce];
  while (polled < req_num) {
    int ret = ibv_poll_cq(qp->GetSendCQ(), std::min(req_num - polled, kMaxPollNumOnce), &wc[0]);
    if (ret < 0) {
      abort();
    }
    polled += ret;
    // for (int i = 0; i < ret; ++i) {
    //   if (wc[i].status != IBV_WC_SUCCESS) {
    //     printf("Error no: %d\n", wc[i].status);
    //     abort();
    //   }
    // }
  }
}

void ExecuteTransactionEmulatorMergeValidationBatch(const std::vector<RecordKey>& access_key,
                                                    BufferManager* bm, rdma::QueuePair* qp,
                                                    ExecResult* exec_result, int* ops,
                                                    int* atomic_ops) {
  bool use_cas_lock = FLAGS_lock == "cas";

  // Phase1: Lock and Read
  char *record_buf[kMaxAccessRecordNum], *lock_buf[kMaxAccessRecordNum],
      *validate_buf[kMaxAccessRecordNum];
  uint64_t raddr = qp->GetRemoteMemoryRegionToken().get_region_addr();

  rdma::RDMABatch read_batch(access_key.size());
  rdma::RDMABatch validation_batch(access_key.size());

  util::Timer timer, cpu_timer;
  for (int i = 0; i < access_key.size(); ++i) {
    auto k = access_key[i];
    char* d = bm->AllocAlign(FLAGS_record_size * 2, CACHE_LINE_SIZE);
    char* lock = bm->AllocAlign(8, WORD_SIZE);
    ASSERT(d, "Allocation failed");
    ASSERT(lock, "Allocation failed");

    record_buf[i] = d;
    validate_buf[i] = d + FLAGS_record_size;
    lock_buf[i] = lock;

    char* record_addr = (char*)raddr + k * FLAGS_record_size;
    if (use_cas_lock) {
      qp->PostMaskedCompareAndSwap(record_addr, lock_buf[i], 0, 1, 1, nullptr);
    } else {
      qp->PostWrite(lock_buf[i], 8, record_addr, nullptr);
    }
    read_batch.PostRead(record_addr, FLAGS_record_size, record_buf[i]);
  }

  // Phase2: Validation
  for (int i = 0; i < access_key.size(); ++i) {
    auto k = access_key[i];
    char* record_addr = (char*)raddr + k * FLAGS_record_size;
    validation_batch.PostRead(record_addr, 8, validate_buf[i]);
  }

  read_batch.SendRequest(qp);
  validation_batch.SendRequest(qp);
  exec_result->cpu_dura = cpu_timer.ns_elapse();

  (*ops) += 3 * access_key.size();
  (*atomic_ops) += use_cas_lock ? access_key.size() : 0;

  // 2 means the two batched operations, access_key.size() means the CAS and write
  // operations
  Poll(qp, access_key.size() + 2);
  exec_result->read_dura = timer.ns_elapse();

  // Phase3: Commit
  for (int i = 0; i < access_key.size(); ++i) {
    auto k = access_key[i];
    char* record_addr = (char*)raddr + k * FLAGS_record_size;
    qp->PostWrite(record_buf[i], FLAGS_record_size, record_addr, nullptr);
    if (FLAGS_unlock) {
      qp->PostMaskedCompareAndSwap(record_addr, lock_buf[i], 1, 0, 0x01, nullptr);
    }
  }
  int rdma_ops_per_record = FLAGS_unlock ? 2 : 1;

  (*ops) += rdma_ops_per_record * access_key.size();
  Poll(qp, rdma_ops_per_record * access_key.size());
}

void ExecuteTransactionEmulatorMergeValidation(const std::vector<RecordKey>& access_key,
                                               BufferManager* bm, rdma::QueuePair* qp,
                                               ExecResult* exec_result, int* ops, int* atomic_ops) {
  bool use_cas_lock = FLAGS_lock == "cas";

  // Phase1: Lock and Read
  util::Timer timer, cpu_timer;
  char *record_buf[kMaxAccessRecordNum], *lock_buf[kMaxAccessRecordNum],
      *validate_buf[kMaxAccessRecordNum];
  uint64_t raddr = qp->GetRemoteMemoryRegionToken().get_region_addr();

  for (int i = 0; i < access_key.size(); ++i) {
    auto k = access_key[i];
    char* d = bm->AllocAlign(FLAGS_record_size * 2, CACHE_LINE_SIZE);
    char* lock = bm->AllocAlign(8, WORD_SIZE);
    ASSERT(d, "Allocation failed");
    ASSERT(lock, "Allocation failed");

    record_buf[i] = d;
    validate_buf[i] = d + FLAGS_record_size;
    lock_buf[i] = lock;

    char* record_addr = (char*)raddr + k * FLAGS_record_size;
    // qp->Post(record_addr, lock, 0, 1, nullptr);
    if (use_cas_lock) {
      qp->PostMaskedCompareAndSwap(record_addr, lock_buf[i], 0, 1, 0x01, nullptr);
    } else {
      qp->PostWrite(lock_buf[i], 8, record_addr, nullptr);
    }
    qp->PostRead(record_addr, FLAGS_record_size, record_buf[i], nullptr);
    qp->PostRead(record_addr, 8, validate_buf[i], nullptr);
  }

  exec_result->cpu_dura = cpu_timer.ns_elapse();

  (*ops) += 3 * access_key.size();
  (*atomic_ops) += use_cas_lock ? access_key.size() : 0;

  Poll(qp, access_key.size() * 3);
  exec_result->read_dura = timer.ns_elapse();

  // Phase3: Commit
  for (int i = 0; i < access_key.size(); ++i) {
    auto k = access_key[i];
    char* record_addr = (char*)raddr + k * FLAGS_record_size;
    qp->PostWrite(record_buf[i], FLAGS_record_size, record_addr, nullptr);
    if (FLAGS_unlock) {
      qp->PostMaskedCompareAndSwap(record_addr, lock_buf[i], 1, 0, 0x01, nullptr);
    }
  }
  int rdma_ops_per_record = FLAGS_unlock ? 2 : 1;
  (*ops) += rdma_ops_per_record * access_key.size();
  Poll(qp, rdma_ops_per_record * access_key.size());
}

void ExecuteTransactionEmulator(const std::vector<RecordKey>& access_key, BufferManager* bm,
                                rdma::QueuePair* qp, ExecResult* exec_result, int* ops,
                                int* atomic_ops) {
  bool use_cas_lock = FLAGS_lock == "cas";

  // Phase1: Lock and Read
  rdma::RDMABatch read_batch(access_key.size());
  rdma::RDMABatch validation_batch(access_key.size());

  util::Timer timer, cpu_timer;
  uint32_t cpu_time = 0;
  char *record_buf[kMaxAccessRecordNum], *lock_buf[kMaxAccessRecordNum];
  uint64_t raddr = qp->GetRemoteMemoryRegionToken().get_region_addr();
  for (int i = 0; i < access_key.size(); ++i) {
    auto k = access_key[i];
    char* d = bm->AllocAlign(FLAGS_record_size, CACHE_LINE_SIZE);
    char* lock = bm->AllocAlign(8, WORD_SIZE);
    ASSERT(d, "Allocation failed");
    ASSERT(lock, "Allocation failed");
    record_buf[i] = d;
    lock_buf[i] = lock;

    char* record_addr = (char*)raddr + k * FLAGS_record_size;
    // qp->Post(record_addr, lock, 0, 1, nullptr);
    if (use_cas_lock) {
      qp->PostMaskedCompareAndSwap(record_addr, lock_buf[i], 0, 1, 0x01, nullptr);
    } else {
      qp->PostWrite(record_buf[i], FLAGS_record_size, record_addr, nullptr);
    }
    if (FLAGS_batch) {
      read_batch.PostRead(record_addr, FLAGS_record_size, d);
    } else {
      qp->PostRead(record_addr, FLAGS_record_size, d, nullptr);
    }
  }
  if (FLAGS_batch) {
    read_batch.SendRequest(qp);
  }

  cpu_time += cpu_timer.ns_elapse();

  (*ops) += 2 * access_key.size();
  (*atomic_ops) += use_cas_lock ? access_key.size() : 0;

  int poll_num = FLAGS_batch ? access_key.size() + 1 : access_key.size() * 2;

  Poll(qp, poll_num);
  exec_result->read_dura = timer.ns_elapse();

  // Phase2: Validation
  timer.reset();
  cpu_timer.reset();
  for (const auto& k : access_key) {
    char* record_addr = (char*)raddr + k * FLAGS_record_size;
    char* d = bm->AllocAlign(FLAGS_record_size, CACHE_LINE_SIZE);
    if (FLAGS_batch) {
      validation_batch.PostRead(record_addr, 8, d);
    } else {
      qp->PostRead(record_addr, 8, d, nullptr);
    }
  }
  if (FLAGS_batch) {
    read_batch.SendRequest(qp);
  }

  cpu_time += cpu_timer.ns_elapse();

  (*ops) += 1 * access_key.size();
  poll_num = FLAGS_batch ? 1 : access_key.size();
  Poll(qp, poll_num);
  exec_result->validate_dura = timer.ns_elapse();
  exec_result->cpu_dura = cpu_time;

  // Phase3: Commit
  for (int i = 0; i < access_key.size(); ++i) {
    auto k = access_key[i];
    char* record_addr = (char*)raddr + k * FLAGS_record_size;
    qp->PostWrite(record_buf[i], FLAGS_record_size, record_addr, nullptr);
    if (FLAGS_unlock) {
      qp->PostMaskedCompareAndSwap(record_addr, lock_buf[i], 1, 0, 0x01, nullptr);
    }
  }

  int rdma_ops_per_record = FLAGS_unlock ? 2 : 1;

  (*ops) += rdma_ops_per_record * access_key.size();
  Poll(qp, rdma_ops_per_record * access_key.size());
}

void TransactionThread(ThreadId tid, BufferManager* bm, History* read_hist, History* validate_hist,
                       History* lat_hist, History* cpu_hist, int* ops, int* atomic_ops,
                       int* txn_ops_ptr) {
  int ops_per_thread = FLAGS_ops / FLAGS_threads;
  int record_num = FLAGS_mrsize / FLAGS_record_size;
  std::random_device rd;  // a seed source for the random number engine
  std::mt19937 gen(tid);  // mersenne_twister_engine seeded with rd()
  std::uniform_int_distribution<> distrib(0, record_num - 1);
  util::Timer timer;
  for (int i = 0; i < ops_per_thread; ++i) {
    // Prepare the transaction input parameters
    std::vector<RecordKey> keys;
    for (int j = 0; j < FLAGS_access_records; ++j) {
      keys.push_back(distrib(gen));
    }
    ExecResult res;
    timer.reset();
    if (FLAGS_merge_validation && FLAGS_batch) {
      ExecuteTransactionEmulatorMergeValidationBatch(keys, bm, bench_qps[tid], &res, ops,
                                                     atomic_ops);
    } else if (FLAGS_merge_validation) {
      ExecuteTransactionEmulatorMergeValidation(keys, bm, bench_qps[tid], &res, ops, atomic_ops);
    } else {
      ExecuteTransactionEmulator(keys, bm, bench_qps[tid], &res, ops, atomic_ops);
    }
    (*txn_ops_ptr) += 1;
    lat_hist->Add(timer.ns_elapse());
    read_hist->Add(res.read_dura);
    validate_hist->Add(res.validate_dura);
    cpu_hist->Add(res.cpu_dura);
  }
}

void RunCN() {
  std::thread threads[64];
  Status s = InitializeAllQueuePairs();
  if (!s.ok()) {
    LOG_ERROR("%s", s.error_msg().c_str());
    return;
  }

  std::memset(rdma_ops, 0, sizeof(rdma_ops));

  History read_hists[64], validate_hists[64], lat_hists[64], cpu_hists[64];
  auto mr_token = bench_qps[0]->GetLocalMemoryRegionToken();
  char* mr_addr = (char*)mr_token.get_region_addr();
  int mr_size = mr_token.get_region_size();
  int mr_per_thread = mr_size / FLAGS_threads;
  for (int i = 0; i < FLAGS_threads; ++i) {
    threads[i] = std::thread(TransactionThread, i,
                             new BufferManager(mr_addr + i * mr_per_thread, mr_per_thread),
                             &read_hists[i], &validate_hists[i], &lat_hists[i], &cpu_hists[i],
                             &rdma_ops[i], &rdma_atomic_ops[i], &txn_ops[i]);
  }

  auto sum_ops = [&]() {
    int ret = 0;
    for (int i = 0; i < FLAGS_threads; ++i) {
      ret += rdma_ops[i];
    }
    return ret;
  };

  auto sum_atomic_ops = [&]() {
    int ret = 0;
    for (int i = 0; i < FLAGS_threads; ++i) {
      ret += rdma_atomic_ops[i];
    }
    return ret;
  };

  auto sum_txn_ops = [&]() {
    int ret = 0;
    for (int i = 0; i < FLAGS_threads; ++i) {
      ret += txn_ops[i];
    }
    return ret;
  };

  int last_ops = 0, last_atomic_ops = 0, last_txn_ops = 0;
  for (int i = 0; i < 10; ++i) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    int curr_ops = sum_ops(), curr_atomic_ops = sum_atomic_ops(), curr_txn_ops = sum_txn_ops();
    std::cout << "RDMA operations: " << (curr_ops - last_ops) / (1000000.0) << '\n';
    std::cout << "RDMA Atomic operations: " << (curr_atomic_ops - last_atomic_ops) / (1000000.0)
              << '\n';
    std::cout << "Transaction operations: " << (curr_txn_ops - last_txn_ops) / (1000000.0) << '\n';
    last_ops = curr_ops;
    last_atomic_ops = curr_atomic_ops;
    last_txn_ops = curr_txn_ops;
  }

  for (int i = 0; i < FLAGS_threads; ++i) {
    threads[i].join();
  }

  History merge_read_hist, merge_validate_hist, lat_hist, cpu_hist;
  for (int i = 0; i < FLAGS_threads; ++i) {
    merge_read_hist.Merge(read_hists[i]);
    merge_validate_hist.Merge(validate_hists[i]);
    lat_hist.Merge(lat_hists[i]);
    cpu_hist.Merge(cpu_hists[i]);
  }
  std::cout << "Read average: " << std::fixed << std::setprecision(2) << std::setw(6)
            << merge_read_hist.Average() / 1000.0 << "  "
            << "p50: " << merge_read_hist.Median() << "\n";

  std::cout << "Validation average: " << std::fixed << std::setprecision(2) << std::setw(6)
            << merge_validate_hist.Average() / 1000.0 << "  "
            << "p50: " << merge_validate_hist.Median() << "\n";

  std::cout << "Latency average: " << std::fixed << std::setprecision(2) << std::setw(6)
            << lat_hist.Average() / 1000.0 << "  "
            << "p50: " << lat_hist.Median() << "\n";

  std::cout << "CPU Time average: " << std::fixed << std::setprecision(2) << std::setw(6)
            << cpu_hist.Average() / 1000.0 << "  "
            << "p50: " << cpu_hist.Median() << "\n";
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  if (FLAGS_type == "mn") {
    RunMN();
  } else {
    RunCN();
  }
}
