// Author: Ming Zhang
// Copyright (c) 2023

#include <algorithm>
#include <atomic>
#include <boost/next_prior.hpp>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <mutex>
#include <numeric>
#include <string>
#include <thread>
#include <unordered_set>

#include "handler/handler.h"
#include "handler/worker.h"
#include "process/oplog.h"
#include "process/stat.h"
#include "tatp/tatp_table.h"
#include "util/fast_random.h"
#include "util/json_config.h"

static const std::vector<int> numa_nodes[2] = {
    // NUMA Node 0 in our machine: 0 ~ 23, 48 ~ 71
    {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15,
     16, 17, 18, 19, 20, 21, 22, 23, 48, 49, 50, 51, 52, 53, 54, 55,
     56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71},

    // NUMA Node 1 in our machine:
    {24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
     40, 41, 42, 43, 44, 45, 46, 47, 72, 73, 74, 75, 76, 77, 78, 79,
     80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95}};

///////////// For control and statistics ///////////////
std::atomic<uint64_t> tx_id_generator;
std::atomic<uint64_t> connected_t_num;
std::atomic<uint64_t> connected_recovery_t_num;

std::vector<t_id_t> tid_vec;
std::vector<double> attemp_tp_vec;
std::vector<double> tp_vec;
std::vector<double> medianlat_vec;
std::map<int, std::vector<double>> txn_lat_vec;
std::vector<int> txn_exec_vec;
std::vector<int> txn_validate_vec;
std::vector<int> txn_commit_vec;
std::vector<double> taillat_vec;
std::vector<uint64_t> total_try_times;
std::vector<uint64_t> total_commit_times;
std::vector<double> delta_usage;

// Get the frequency of accessing old versions
uint64_t access_old_version_cnt[MAX_TNUM_PER_CN];
uint64_t access_new_version_cnt[MAX_TNUM_PER_CN];

EventCount event_counter;
KeyCount key_counter;

// For crash recovery test
std::atomic<bool> to_crash[MAX_TNUM_PER_CN];
std::atomic<bool> report_crash[MAX_TNUM_PER_CN];
uint64_t try_times[MAX_TNUM_PER_CN];

std::atomic<bool> primary_fail;
std::atomic<bool> cannot_lock_new_primary;

std::atomic<bool> one_backup_fail;
std::atomic<bool> during_backup_recovery;

// For probing
std::atomic<int> probe_times;
std::atomic<bool> probe[MAX_TNUM_PER_CN];
std::vector<std::vector<TpProbe>> tp_probe_vec;
std::atomic<bool> is_running;

/////////////////////////////////////////////////////////

void TimeStop(t_id_t thread_num_per_machine, int tp_probe_interval_us) {
  while (is_running) {
    usleep(tp_probe_interval_us);
    for (int i = 0; i < thread_num_per_machine; i++) {
      probe[i] = true;
    }
    probe_times++;
  }
}

void Handler::GenThreads(std::string bench_name) {
  std::string config_filepath = "../../../config/cn_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto client_conf = json_config.get("local_compute_node");
  node_id_t machine_num = (node_id_t)client_conf.get("machine_num").get_int64();
  node_id_t machine_id = (node_id_t)client_conf.get("machine_id").get_int64();
  t_id_t thread_num_per_machine =
      (t_id_t)client_conf.get("thread_num_per_machine").get_int64();
  const int coro_num = (int)client_conf.get("coroutine_num").get_int64();
  int crash_tnum = 0;

#if HAVE_COORD_CRASH
  crash_tnum = (int)client_conf.get("crash_tnum").get_int64();
#endif
  assert(machine_id >= 0 && machine_id < machine_num &&
         thread_num_per_machine > 2 * crash_tnum);

  AddrCache *addr_caches = new AddrCache[thread_num_per_machine - crash_tnum];

  for (int i = 0; i < MAX_TNUM_PER_CN; i++) {
    access_old_version_cnt[i] = 0;
    access_new_version_cnt[i] = 0;
  }

  /*** Coordinator crash model
   * total: 40 threads
   * crash: 10 threads
   * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   * |       20 good          |   10 will-crash    |   10 prepare   |
   * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   */

#if HAVE_PRIMARY_CRASH
  primary_fail = false;
  cannot_lock_new_primary = false;
#endif

#if HAVE_BACKUP_CRASH
  one_backup_fail = false;
  during_backup_recovery = false;
#endif

#if HAVE_COORD_CRASH
  // Prepare crash info

  for (int i = 0; i < thread_num_per_machine; i++) {
    to_crash[i] = false;
    report_crash[i] = false;
  }
  memset((char *)try_times, 0, sizeof(try_times));
#endif

#if PROBE_TP
  probe_times = 0;
  for (int i = 0; i < thread_num_per_machine; i++) {
    probe[i] = false;
  }
  tp_probe_vec.resize(thread_num_per_machine);
#endif

  /* Start working */
  tx_id_generator = 1; // Initial transaction id == 1
  connected_t_num = 0; // Sync all threads' RDMA QP connections
  connected_recovery_t_num = 0;

  auto thread_arr = new std::thread[thread_num_per_machine];

  auto *global_meta_man = new MetaManager();
  RDMA_LOG(INFO) << "Alloc local memory: "
                 << (size_t)(thread_num_per_machine * PER_THREAD_ALLOC_SIZE) /
                        (1024 * 1024)
                 << " MB. Waiting...";
  auto *global_rdma_region =
      new LocalRegionAllocator(global_meta_man, thread_num_per_machine);

  auto *global_delta_region = new RemoteDeltaRegionAllocator(
      global_meta_man, global_meta_man->remote_nodes);

  auto *global_locked_key_table =
      new LockedKeyTable[thread_num_per_machine * coro_num];

  auto *param_arr = new struct thread_params[thread_num_per_machine];

  TATP *tatp_client = nullptr;
  SmallBank *smallbank_client = nullptr;
  TPCC *tpcc_client = nullptr;
  MICRO *micro_client = nullptr;

  if (bench_name == "tatp") {
    tatp_client = new TATP();
    total_try_times.resize(TATP_TX_TYPES, 0);
    total_commit_times.resize(TATP_TX_TYPES, 0);
  } else if (bench_name == "smallbank") {
    smallbank_client = new SmallBank();
    total_try_times.resize(SmallBank_TX_TYPES, 0);
    total_commit_times.resize(SmallBank_TX_TYPES, 0);
  } else if (bench_name == "tpcc") {
    tpcc_client = new TPCC();
    total_try_times.resize(TPCC_TX_TYPES, 0);
    total_commit_times.resize(TPCC_TX_TYPES, 0);
  } else if (bench_name == "micro") {
    micro_client = new MICRO();
    total_try_times.resize(MICRO_TX_TYPES, 0);
    total_commit_times.resize(MICRO_TX_TYPES, 0);
  }

  std::vector<void *> smallbank_txn_params[64 * 3];
  std::vector<SmallBankTxType> smallbank_txn_param_type[64 * 3];

  std::vector<void *> micro_txn_params[64 * 3];
  std::vector<MicroTxType> micro_txn_param_type[64 * 3];

  std::vector<void *> tatp_txn_params[64];
  std::vector<TATPTxType> tatp_txn_param_type[64];

  const int kTxnNum = 250000 * thread_num_per_machine;
  if (bench_name == "smallbank") {
    RDMA_LOG(INFO) << "Prepapring Workloads for SmallBank: ";
    FastRandom r;
    ZipfGenerator zipf(smallbank_client->num_accounts_global,
                       smallbank_client->zipfian);

    auto txn_types = smallbank_client->CreateWorkgenArray();

    // Initialize 1000000 transaction params:
    for (int i = 0; i < kTxnNum * machine_num; ++i) {
      auto tid = i % (thread_num_per_machine * machine_num);
      auto t = txn_types[int(r.NextUniform() * 100)];
      smallbank_txn_param_type[tid].push_back(t);
      switch (t) {
      case SmallBankTxType::kAmalgamate: {
        AmalgamateParam *param = new AmalgamateParam();
        param->custid_0_ = zipf.GetNextNumber();
        do {
          param->custid_1_ = zipf.GetNextNumber();
        } while (param->custid_0_ == param->custid_1_);
        smallbank_txn_params[tid].push_back(param);
        break;
      }
      case SmallBankTxType::kWriteCheck: {
        WriteCheckParam *param = new WriteCheckParam();
        param->custid_ = zipf.GetNextNumber();
        smallbank_txn_params[tid].push_back(param);
        break;
      }
      case SmallBankTxType::kDepositChecking: {
        DepositCheckingParam *param = new DepositCheckingParam();
        param->custid_ = zipf.GetNextNumber();
        smallbank_txn_params[tid].push_back(param);
        break;
      }
      case SmallBankTxType::kSendPayment: {
        SendPaymentParam *param = new SendPaymentParam();
        param->custid_0_ = zipf.GetNextNumber();
        do {
          param->custid_1_ = zipf.GetNextNumber();
        } while (param->custid_0_ == param->custid_1_);
        smallbank_txn_params[tid].push_back(param);
        break;
      }
      case SmallBankTxType::kBalance: {
        BalanceParam *param = new BalanceParam();
        param->custid_ = zipf.GetNextNumber();
        smallbank_txn_params[tid].push_back(param);
        break;
      }
      case SmallBankTxType::kTransactSaving: {
        TransactSavingParam *param = new TransactSavingParam();
        param->custid_ = zipf.GetNextNumber();
        smallbank_txn_params[tid].push_back(param);
        break;
      }
      }
    }
  } else if (bench_name == "micro") {
    RDMA_LOG(INFO) << "Prepapring Workloads for Micro: ";
    FastRandom r;
    ZipfGenerator zipf(micro_client->num_keys_global, micro_client->zipfian);
    const uint64_t kRecordNum = micro_client->num_op;

    auto txn_types = micro_client->CreateWorkgenArray();
    for (int i = 0; i < kTxnNum * machine_num; ++i) {
      auto tid = i % (thread_num_per_machine * machine_num);
      auto t = txn_types[int(r.NextUniform() * 100)];
      micro_txn_param_type[tid].push_back(t);
      switch (t) {
      case MicroTxType::kUpdateOne: {
        // Generate 4 different keys
        RWUpdateOneParam *param = new RWUpdateOneParam();
        param->num_key = kRecordNum;
        // Generate parameters:
        std::unordered_set<int64_t> selected_keys;
        for (size_t i = 0; i < micro_client->num_op; ++i) {
          int64_t k = zipf.GetNextNumber() % (micro_client->num_keys_global);
          if (selected_keys.find(k) != selected_keys.end()) {
            --i;
            continue;
          } else {
            selected_keys.insert(k);
          }
          param->keys[i] = k;
        }

        micro_txn_params[tid].push_back(param);
        break;
      }
      case MicroTxType::kReadOne: {
        // Generate 4 different keys
        RWReadOneParam *param = new RWReadOneParam();
        param->num_key = kRecordNum;
        // Generate parameters:
        std::unordered_set<int64_t> selected_keys;
        for (size_t i = 0; i < micro_client->num_op; ++i) {
          int64_t k = zipf.GetNextNumber() % (micro_client->num_keys_global);
          if (selected_keys.find(k) != selected_keys.end()) {
            --i;
            continue;
          } else {
            selected_keys.insert(k);
          }
          param->keys[i] = k;
        }

        micro_txn_params[tid].push_back(param);
        break;
      }
      default:
        RDMA_LOG(ERROR) << "Unexpected transaction type "
                        << static_cast<int>(t);
        abort();
      }
    }
  } else if (bench_name == "tatp") {
    RDMA_LOG(INFO) << "Prepapring Workloads for TATP: ";
    FastRandom r;
    FastRandom fast_randoms[64];
    auto txn_types = tatp_client->CreateWorkgenArray();

    for (int i = 0; i < kTxnNum; ++i) {
      auto tid = i % thread_num_per_machine;
      auto t = txn_types[int(r.NextUniform() * 100)];
      tatp_txn_param_type[tid].push_back(t);
      switch (t) {
      case TATPTxType::kGetSubsciberData: {
        GetSubscriberTxnParam *param = new GetSubscriberTxnParam();
        param->s_id_ = tatp_client->GetNonUniformRandomSubscriber();
        tatp_txn_params[tid].push_back(param);
        break;
      }
      case TATPTxType::kGetNewDestination: {
        GetNewDestinationTxnParam *param = new GetNewDestinationTxnParam();
        param->s_id_ = tatp_client->GetNonUniformRandomSubscriber();
        param->sf_type_ = fast_randoms[tid].RandNumber(0, 3);
        uint64_t start_time = fast_randoms[tid].RandNumber(0, 2) * 8;
        uint64_t end_time = fast_randoms[tid].RandNumber(0, 23);

        unsigned cf_to_fetch = (start_time / 8) + 1;
        assert(cf_to_fetch <= 3);
        param->fetch_num_ = cf_to_fetch;
        param->start_time_ = start_time;
        param->end_time_ = end_time;
        tatp_txn_params[tid].push_back(param);
        break;
      }
      case TATPTxType::kGetAccessData: {
        GetAccessDataTxnParam *param = new GetAccessDataTxnParam();
        param->s_id_ = tatp_client->GetNonUniformRandomSubscriber();
        param->ai_type_ = fast_randoms[tid].RandNumber(0, 3);
        tatp_txn_params[tid].push_back(param);
        break;
      }
      case TATPTxType::kUpdateSubscriberData: {
        UpdateSubscriberDataTxnParam *param =
            new UpdateSubscriberDataTxnParam();
        param->s_id_ = tatp_client->GetNonUniformRandomSubscriber();
        param->sf_type_ = fast_randoms[tid].RandNumber(0, 3);
        tatp_txn_params[tid].push_back(param);
        break;
      }
      case TATPTxType::kUpdateLocation: {
        UpdateLocationTxnParam *param = new UpdateLocationTxnParam();
        param->s_id_ = tatp_client->GetNonUniformRandomSubscriber();
        tatp_txn_params[tid].push_back(param);
        break;
      }
      case TATPTxType::kInsertCallForwarding: {
        InsertCallForwardingTxnParam *param =
            new InsertCallForwardingTxnParam();
        param->s_id_ = tatp_client->GetNonUniformRandomSubscriber();
        param->sf_type_ = fast_randoms[tid].RandNumber(0, 3);
        uint64_t start_time = fast_randoms[tid].RandNumber(0, 2) * 8;
        uint64_t end_time = fast_randoms[tid].RandNumber(0, 24);
        param->start_time_ = start_time;
        param->end_time_ = end_time;
        tatp_txn_params[tid].push_back(param);
        break;
      }
      case TATPTxType::kDeleteCallForwarding: {
        DeleteCallForwardingTxnParam *param =
            new DeleteCallForwardingTxnParam();
        tatp_txn_params[tid].push_back(param);
        break;
      }
      default:
        abort();
      }
    }
  }

  RDMA_LOG(INFO) << "Running on isolation level: "
                 << global_meta_man->iso_level;
  RDMA_LOG(INFO) << "Executing...";

  memcached_wrapper->ConnectToMemcached();
  memcached_wrapper->AddServer();
  memcached_wrapper->SyncComputeNodes();

  for (t_id_t i = 0; i < thread_num_per_machine - crash_tnum; i++) {
    // Setup the thread id
    param_arr[i].thread_local_id = i;
    param_arr[i].thread_global_id = (machine_id * thread_num_per_machine) + i;

    // Setup for Smallbank
    SmallBank *smallbank_c = new SmallBank();
    smallbank_c->coro_num = coro_num;
    smallbank_c->txn_run_types =
        &smallbank_txn_param_type[param_arr[i].thread_global_id];
    smallbank_c->txn_run_params =
        &smallbank_txn_params[param_arr[i].thread_global_id];

    // Setup for Micro
    MICRO *micro_c = new MICRO();
    micro_c->coro_num = coro_num;
    micro_c->txn_run_types =
        &micro_txn_param_type[param_arr[i].thread_global_id];
    micro_c->txn_run_params = &micro_txn_params[param_arr[i].thread_global_id];

    // Setup for TATP
    TATP *tatp_c = new TATP();
    tatp_c->coro_num = coro_num;
    tatp_c->txn_run_types = &tatp_txn_param_type[i];
    tatp_c->txn_run_params = &tatp_txn_params[i];

    param_arr[i].coro_num = coro_num;
    param_arr[i].bench_name = bench_name;
    param_arr[i].global_meta_man = global_meta_man;
    param_arr[i].addr_cache = &(addr_caches[i]);
    param_arr[i].global_rdma_region = global_rdma_region;
    param_arr[i].global_delta_region = global_delta_region;
    param_arr[i].global_locked_key_table = global_locked_key_table;
    param_arr[i].running_tnum = thread_num_per_machine - crash_tnum;
    thread_arr[i] = std::thread(run_thread, &param_arr[i], tatp_c, smallbank_c,
                                tpcc_client, micro_c, &(tp_probe_vec[i]));

    /* Pin thread i to hardware thread i */
    // cpu_set_t cpuset;
    // CPU_ZERO(&cpuset);
    // // (kqh): The right way to bind the CPU core on our machine
    // CPU_SET(numa_nodes[0][i], &cpuset);
    // int rc = pthread_setaffinity_np(thread_arr[i].native_handle(),
    //                                 sizeof(cpu_set_t), &cpuset);
    // if (rc != 0) {
    //   RDMA_LOG(WARNING) << "Error calling pthread_setaffinity_np: " << rc;
    // }
  }

#if PROBE_TP
  is_running = true;
  int tp_probe_inter_us =
      (int)client_conf.get("tp_probe_interval_ms").get_int64() * 1000;
  std::thread time_stop =
      std::thread(TimeStop, thread_num_per_machine, tp_probe_inter_us);
#endif

#if HAVE_PRIMARY_CRASH

  int crash_time_ms = (int)client_conf.get("crash_time_ms").get_int64();
  std::cerr << "sleeping " << (double)crash_time_ms / 1000.0 << " seconds..."
            << std::endl;
  usleep(crash_time_ms * 1000);

  std::cerr << "primary crashes!\n";

  primary_fail = true;
#endif

#if HAVE_BACKUP_CRASH
  int crash_time_ms = (int)client_conf.get("crash_time_ms").get_int64();
  std::cerr << "sleeping " << (double)crash_time_ms / 1000.0 << " seconds..."
            << std::endl;
  usleep(crash_time_ms * 1000);

  std::cerr << "backup crashes!\n";
  one_backup_fail = true;
#endif

#if HAVE_COORD_CRASH
  int crash_time_ms = (int)client_conf.get("crash_time_ms").get_int64();
  std::cerr << "sleeping " << (double)crash_time_ms / 1000.0 << " seconds..."
            << std::endl;
  usleep(crash_time_ms * 1000);

  // Make crash
  for (int k = thread_num_per_machine - crash_tnum - crash_tnum;
       k < thread_num_per_machine - crash_tnum; k++) {
    std::cerr << "Thread " << k << " should crash" << std::endl;
    to_crash[k] = true;
  }

  {
    // Print time
    time_t tt;
    struct timeval tv_;
    struct tm *timeinfo;
    long tv_ms = 0, tv_us = 0;
    char output[20];
    time(&tt);
    timeinfo = localtime(&tt);
    gettimeofday(&tv_, NULL);
    strftime(output, 20, "%Y-%m-%d %H:%M:%S", timeinfo);
    tv_ms = tv_.tv_usec / 1000;
    tv_us = tv_.tv_usec % 1000;
    printf("crash at :%s %ld:%ld\r\n", output, tv_ms, tv_us);
  }

  for (int crasher = thread_num_per_machine - crash_tnum - crash_tnum;
       crasher < thread_num_per_machine - crash_tnum; crasher++) {
    while (!report_crash[crasher])
      ;

    int i = crasher + crash_tnum; // i is the recovery thread's id

    param_arr[i].thread_local_id = i;
    param_arr[i].thread_global_id = (machine_id * thread_num_per_machine) + i;
    param_arr[i].coro_num = coro_num;
    param_arr[i].bench_name = bench_name;
    param_arr[i].global_meta_man = global_meta_man;
    param_arr[i].addr_cache = &(addr_caches[crasher]);
    param_arr[i].global_rdma_region = global_rdma_region;
    param_arr[i].global_delta_region = global_delta_region;
    param_arr[i].global_locked_key_table = global_locked_key_table;
    param_arr[i].running_tnum = crash_tnum;
    thread_arr[i] = std::thread(
        recovery, &param_arr[i], tatp_client, smallbank_client, tpcc_client,
        try_times[crasher], &(tp_probe_vec[i]), crasher);

    /* Pin thread i to hardware thread i */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
    int rc = pthread_setaffinity_np(thread_arr[i].native_handle(),
                                    sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      RDMA_LOG(WARNING) << "Error calling pthread_setaffinity_np: " << rc;
    }
  }
#endif

  for (t_id_t i = 0; i < thread_num_per_machine; i++) {
    if (thread_arr[i].joinable()) {
      thread_arr[i].join();
      // RDMA_LOG(INFO) << "Thread " << i << " joins";
    }
  }

#if PROBE_TP
  is_running = false;
  if (time_stop.joinable()) {
    time_stop.join();
    // RDMA_LOG(INFO) << "timer thread joins";
  }
#endif

  RDMA_LOG(INFO) << "DONE";

  delete[] addr_caches;
  delete[] global_locked_key_table;
  delete[] param_arr;
  delete global_rdma_region;
  delete global_meta_man;
  if (tatp_client)
    delete tatp_client;
  if (smallbank_client)
    delete smallbank_client;
  if (tpcc_client)
    delete tpcc_client;
}

void Handler::OutputResult(std::string bench_name, std::string system_name) {
  RDMA_LOG(INFO) << "Generate results...";
  std::string results_cmd = "mkdir -p ../../../bench_results/" + bench_name;
  system(results_cmd.c_str());
  std::ofstream of, of_detail, of_delta_usage;
  std::string res_file = "../../../bench_results/" + bench_name + "/result.txt";
  std::string delta_usage_file =
      "../../../bench_results/" + bench_name + "/delta_usage.txt";

  // of_delta_usage.open(delta_usage_file.c_str(), std::ios::app);

  double total_attemp_tp = 0;
  double total_tp = 0;
  double total_median = 0;
  double total_tail = 0;

  for (int i = 0; i < tid_vec.size(); i++) {
    total_attemp_tp += attemp_tp_vec[i];
    total_tp += tp_vec[i];
    total_median += medianlat_vec[i];
    total_tail += taillat_vec[i];
  }

  size_t thread_num = tid_vec.size();

  double avg_median = total_median / thread_num;
  double avg_tail = total_tail / thread_num;

  std::sort(medianlat_vec.begin(), medianlat_vec.end());
  std::sort(taillat_vec.begin(), taillat_vec.end());

  // of.open(res_file.c_str(), std::ios::app);
  // of << system_name << " " << total_attemp_tp / 1000 << " " << total_tp /
  // 1000
  //    << " " << avg_median << " " << avg_tail << std::endl;
  // of.close();

  std::ofstream of_abort_rate;
  std::string abort_rate_file =
      "../../../bench_results/" + bench_name + "/abort_rate.txt";
  of_abort_rate.open(abort_rate_file.c_str(), std::ios::app);
  of_abort_rate << system_name << " tx_type try_num commit_num abort_rate"
                << std::endl;
  if (bench_name == "tatp") {
    for (int i = 0; i < TATP_TX_TYPES; i++) {
      of_abort_rate << TATP_TX_NAME[i] << " " << total_try_times[i] << " "
                    << total_commit_times[i] << " "
                    << (double)(total_try_times[i] - total_commit_times[i]) /
                           (double)total_try_times[i]
                    << std::endl;

      // Output the specific txn's abort rate
      std::string onetxn_abort_rate_file = "../../../bench_results/" +
                                           bench_name + "/" + TATP_TX_NAME[i] +
                                           "_abort_rate.txt";
      std::ofstream of_onetxn_abort_rate;
      of_onetxn_abort_rate.open(onetxn_abort_rate_file.c_str(), std::ios::app);
      of_onetxn_abort_rate << system_name << " " << total_try_times[i] << " "
                           << total_commit_times[i] << " "
                           << (double)(total_try_times[i] -
                                       total_commit_times[i]) /
                                  (double)total_try_times[i]
                           << std::endl;
    }

  } else if (bench_name == "smallbank") {
    for (int i = 0; i < SmallBank_TX_TYPES; i++) {
      of_abort_rate << SmallBank_TX_NAME[i] << " " << total_try_times[i] << " "
                    << total_commit_times[i] << " "
                    << (double)(total_try_times[i] - total_commit_times[i]) /
                           (double)total_try_times[i]
                    << std::endl;

      // Output the specific txn's abort rate
      std::string onetxn_abort_rate_file =
          "../../../bench_results/" + bench_name + "/" + SmallBank_TX_NAME[i] +
          "_abort_rate.txt";
      std::ofstream of_onetxn_abort_rate;
      of_onetxn_abort_rate.open(onetxn_abort_rate_file.c_str(), std::ios::app);
      of_onetxn_abort_rate << system_name << " " << total_try_times[i] << " "
                           << total_commit_times[i] << " "
                           << (double)(total_try_times[i] -
                                       total_commit_times[i]) /
                                  (double)total_try_times[i]
                           << std::endl;
    }
  } else if (bench_name == "tpcc") {
    uint64_t all_tried_count = 0, all_committed_count = 0;
    for (int i = 0; i < TPCC_TX_TYPES; i++) {
      all_tried_count += total_try_times[i];
      all_committed_count += total_commit_times[i];
      of_abort_rate << TPCC_TX_NAME[i] << " " << total_try_times[i] << " "
                    << total_commit_times[i] << " "
                    << (double)(total_try_times[i] - total_commit_times[i]) /
                           (double)total_try_times[i]
                    << std::endl;

      // Output the specific txn's abort rate
      std::string onetxn_abort_rate_file = "../../../bench_results/" +
                                           bench_name + "/" + TPCC_TX_NAME[i] +
                                           "_abort_rate.txt";
      std::ofstream of_onetxn_abort_rate;
      of_onetxn_abort_rate.open(onetxn_abort_rate_file.c_str(), std::ios::app);
      of_onetxn_abort_rate << system_name << " " << total_try_times[i] << " "
                           << total_commit_times[i] << " "
                           << (double)(total_try_times[i] -
                                       total_commit_times[i]) /
                                  (double)total_try_times[i]
                           << std::endl;
    }
    of_abort_rate << "All Tried: " << all_tried_count
                  << ", All Committed: " << all_committed_count
                  << ", Abort Rate: "
                  << (double)(all_tried_count - all_committed_count) /
                         (double)all_tried_count
                  << std::endl;
  } else if (bench_name == "micro") {
    for (int i = 0; i < MICRO_TX_TYPES; i++) {
      of_abort_rate << MICRO_TX_NAME[i] << " " << total_try_times[i] << " "
                    << total_commit_times[i] << " "
                    << (double)(total_try_times[i] - total_commit_times[i]) /
                           (double)total_try_times[i]
                    << std::endl;

      // Output the specific txn's abort rate
      std::string onetxn_abort_rate_file = "../../../bench_results/" +
                                           bench_name + "/" + MICRO_TX_NAME[i] +
                                           "_abort_rate.txt";
      std::ofstream of_onetxn_abort_rate;
      of_onetxn_abort_rate.open(onetxn_abort_rate_file.c_str(), std::ios::app);
      of_onetxn_abort_rate << system_name << " " << total_try_times[i] << " "
                           << total_commit_times[i] << " "
                           << (double)(total_try_times[i] -
                                       total_commit_times[i]) /
                                  (double)total_try_times[i]
                           << std::endl;
    }
  }

  of_abort_rate << std::endl;
  of_abort_rate.close();

  std::cout << system_name << " " << total_attemp_tp / 1000 << " "
            << total_tp / 1000 << " " << avg_median << " " << avg_tail
            << std::endl;

  double total_delta_usage_MB = 0;
  for (int i = 0; i < delta_usage.size(); i++) {
    total_delta_usage_MB += delta_usage[i];
  }

  // std::cout << "TOTAL delta: " << total_delta_usage_MB << " MB"
  //           << ". AVG delta/thread: " << (double)total_delta_usage_MB /
  //           thread_num << " MB" << std::endl;

  // of_delta_usage << system_name << " vnum: " << MAX_VCELL_NUM << "
  // total_delta_usage_MB: " << total_delta_usage_MB << std::endl;
  // of_delta_usage.close();

#if OUTPUT_EVENT_STAT

  std::ofstream of_event_count("../../../event_count.yml", std::ofstream::out);

  of_event_count << "Abort Rate for all txns" << std::endl;
  of_event_count << system_name << " tx_type try_num commit_num abort_rate"
                 << std::endl;

  std::cout << std::endl;
  std::cout << "abort rate:" << std::endl;
  if (bench_name == "tatp") {
    for (int i = 0; i < TATP_TX_TYPES; i++) {
      of_event_count << TATP_TX_NAME[i] << " " << total_try_times[i] << " "
                     << total_commit_times[i] << " "
                     << (double)(total_try_times[i] - total_commit_times[i]) /
                            (double)total_try_times[i]
                     << std::endl;
      std::cout << TATP_TX_NAME[i] << " " << total_try_times[i] << " "
                << total_commit_times[i] << " "
                << (double)(total_try_times[i] - total_commit_times[i]) /
                       (double)total_try_times[i]
                << std::endl;
    }
  } else if (bench_name == "smallbank") {
    for (int i = 0; i < SmallBank_TX_TYPES; i++) {
      of_event_count << SmallBank_TX_NAME[i] << " " << total_try_times[i] << " "
                     << total_commit_times[i] << " "
                     << (double)(total_try_times[i] - total_commit_times[i]) /
                            (double)total_try_times[i]
                     << std::endl;
      std::cout << SmallBank_TX_NAME[i] << " " << total_try_times[i] << " "
                << total_commit_times[i] << " "
                << (double)(total_try_times[i] - total_commit_times[i]) /
                       (double)total_try_times[i]
                << std::endl;
    }
  } else if (bench_name == "tpcc") {
    for (int i = 0; i < TPCC_TX_TYPES; i++) {
      of_event_count << TPCC_TX_NAME[i] << " " << total_try_times[i] << " "
                     << total_commit_times[i] << " "
                     << (double)(total_try_times[i] - total_commit_times[i]) /
                            (double)total_try_times[i]
                     << std::endl;
      std::cout << TPCC_TX_NAME[i] << " " << total_try_times[i] << " "
                << total_commit_times[i] << " "
                << (double)(total_try_times[i] - total_commit_times[i]) /
                       (double)total_try_times[i]
                << std::endl;
    }
  } else if (bench_name == "micro") {
    for (int i = 0; i < MICRO_TX_TYPES; i++) {
      of_event_count << MICRO_TX_NAME[i] << " " << total_try_times[i] << " "
                     << total_commit_times[i] << " "
                     << (double)(total_try_times[i] - total_commit_times[i]) /
                            (double)total_try_times[i]
                     << std::endl;
      std::cout << MICRO_TX_NAME[i] << " " << total_try_times[i] << " "
                << total_commit_times[i] << " "
                << (double)(total_try_times[i] - total_commit_times[i]) /
                       (double)total_try_times[i]
                << std::endl;
    }
  }

  event_counter.Output(of_event_count);

  of_event_count.close();
#endif

  // Generate the latency file:
  std::ofstream of_latency("../../../latency.txt", std::ios::out);
  const int elements_per_line = 20; // Default value set to 20
  double discard_rate = 0.05;
  std::vector<double> avg_lantecy;
  std::vector<double> p50_lantecy;
  std::vector<double> p99_lantecy;
  std::vector<double> p999_lantecy;
  std::vector<double> total_latency;
  for (const auto &entry : txn_lat_vec) {
    of_latency << std::endl; // Ensure a new line before each transaction type
    of_latency << "Transaction Type " << entry.first << " Latency: \n";
    int count = 0;
    std::vector<double> latencies = entry.second;
    size_t discard_size = (size_t)(latencies.size() * discard_rate);
    latencies.erase(latencies.begin(),
                    latencies.begin() + discard_size); // Discard the first 5%
    latencies.erase(latencies.end() - discard_size,
                    latencies.end()); // Discard the last 5%
    total_latency.insert(total_latency.end(), latencies.begin(),
                         latencies.end());

    // Calculate the average latency:
    double sum = 0;
    for (const auto &latency : latencies) {
      sum += latency;
    }
    double average_latency = sum / latencies.size();

    // Calculate the P50, P99, P999 latency:
    std::sort(latencies.begin(), latencies.end());
    double p50_lat = latencies[latencies.size() / 2];
    double p99_lat = latencies[latencies.size() * 99 / 100];
    double p999_lat = latencies[latencies.size() * 999 / 1000];

    avg_lantecy.push_back(average_latency);
    p50_lantecy.push_back(p50_lat);
    p99_lantecy.push_back(p99_lat);
    p999_lantecy.push_back(p999_lat);

    printf("TxnType: %d, AvgLat: %.2f us, P50: %.2f us, P99: %.2f us, P999: "
           "%.2f us\n",
           entry.first, average_latency, p50_lat, p99_lat, p999_lat);

    // This is for aggregation results
    size_t i = 0;
    for (const auto &latency : entry.second) {
      if (i % 1000 == 0) {
        of_latency << latency << " ";
        if (++count % elements_per_line == 0) {
          of_latency << std::endl;
        }
      }
    }
    of_latency << std::endl;
  }

  of_latency << "[Summary:] \n";
  for (size_t i = 0; i < avg_lantecy.size(); ++i) {
    of_latency << "TxnType: " << i << ", AvgLat: " << avg_lantecy[i]
               << " us, P50: " << p50_lantecy[i]
               << " us, P99: " << p99_lantecy[i]
               << " us, P999: " << p999_lantecy[i] << " us\n";
  }

  // Calculate the total latency:
  std::sort(total_latency.begin(), total_latency.end());
  double s = 0;
  for (const auto &l : total_latency) {
    s += l;
  }
  double total_latency_avg = s / total_latency.size();
  double total_latency_p50 = total_latency[total_latency.size() / 2];
  double total_latency_p99 = total_latency[total_latency.size() * 99 / 100];
  double total_latency_p999 = total_latency[total_latency.size() * 999 / 1000];
  of_latency << "Total Latency: AvgLat: " << total_latency_avg
             << " us, P50: " << total_latency_p50
             << " us, P99: " << total_latency_p99
             << " us, P999: " << total_latency_p999 << " us\n";

  std::sort(txn_exec_vec.begin(), txn_exec_vec.end());
  uint64_t exec_total =
      std::accumulate(txn_exec_vec.begin(), txn_exec_vec.end(), 0);
  uint64_t exec_avg = exec_total / txn_exec_vec.size();
  uint64_t exec_p50 = txn_exec_vec[txn_exec_vec.size() / 2];
  uint64_t exec_p99 = txn_exec_vec[txn_exec_vec.size() * 99 / 100];
  uint64_t exec_p999 = txn_exec_vec[txn_exec_vec.size() * 999 / 1000];

  std::sort(txn_validate_vec.begin(), txn_validate_vec.end());
  uint64_t validate_total =
      std::accumulate(txn_validate_vec.begin(), txn_validate_vec.end(), 0);
  uint64_t validate_avg = validate_total / txn_validate_vec.size();
  uint64_t validate_p50 = txn_validate_vec[txn_validate_vec.size() / 2];
  uint64_t validate_p99 = txn_validate_vec[txn_validate_vec.size() * 99 / 100];
  uint64_t validate_p999 =
      txn_validate_vec[txn_validate_vec.size() * 999 / 1000];

  std::sort(txn_commit_vec.begin(), txn_commit_vec.end());
  uint64_t commit_total =
      std::accumulate(txn_commit_vec.begin(), txn_commit_vec.end(), 0);
  uint64_t commit_avg = commit_total / txn_commit_vec.size();
  uint64_t commit_p50 = txn_commit_vec[txn_commit_vec.size() / 2];
  uint64_t commit_p99 = txn_commit_vec[txn_commit_vec.size() * 99 / 100];
  uint64_t commit_p999 = txn_commit_vec[txn_commit_vec.size() * 999 / 1000];

  of_latency << "Total Latency: AvgLat: " << total_latency_avg
             << " us, P50: " << total_latency_p50
             << " us, P99: " << total_latency_p99
             << " us, P999: " << total_latency_p999 << " us\n";

  of_latency << "Exec: Avg: " << exec_avg << " us, P50: " << exec_p50
             << " us, P99: " << exec_p99 << " us, P999: " << exec_p999
             << " us\n";

  of_latency << "Validate: Avg: " << validate_avg
             << " us, P50: " << validate_p50 << " us, P99: " << validate_p99
             << " us, P999: " << validate_p999 << " us\n";

  of_latency << "Commit: Avg: " << commit_avg << " us, P50: " << commit_p50
             << " us, P99: " << commit_p99 << " us, P999: " << commit_p999
             << " us\n";

  of_latency.close();

  std::cout << "Total Latency: AvgLat: " << total_latency_avg
            << " us, P50: " << total_latency_p50
            << " us, P99: " << total_latency_p99
            << " us, P999: " << total_latency_p999 << " us\n";

  std::cout << "Exec: Avg: " << exec_avg << " us, P50: " << exec_p50
            << " us, P99: " << exec_p99 << " us, P999: " << exec_p999
            << " us\n";

  std::cout << "Validate: Avg: " << validate_avg << " us, P50: " << validate_p50
            << " us, P99: " << validate_p99 << " us, P999: " << validate_p999
            << " us\n";

  std::cout << "Commit: Avg: " << commit_avg << " us, P50: " << commit_p50
            << " us, P99: " << commit_p99 << " us, P999: " << commit_p999
            << " us\n";

  // Output the final results
  of.open(res_file.c_str(), std::ios::app);
  of << system_name << " " << total_attemp_tp / 1000 << " " << total_tp / 1000
     << " " << total_latency_avg << " " << total_latency_p50 << " "
     << total_latency_p99 << " " << total_latency_p999 << " " 
     // Dump the breakdown of latency
     << exec_avg << " " 
     << validate_avg << " "
     << commit_avg << std::endl;
  of.close();

#if OUTPUT_KEY_STAT
  key_counter.Output();
#endif

#if PROBE_TP
  std::map<int, double> tp_time_figure;
  std::map<int, double> attemp_tp_time_figure;

  std::ofstream of_probe_tp;
  time_t rawtime;
  struct tm *ptminfo;
  time(&rawtime);
  ptminfo = localtime(&rawtime);
  std::string s;
  if (ptminfo->tm_mon + 1 < 10) {
    s = std::to_string(ptminfo->tm_year + 1900) + "-0" +
        std::to_string(ptminfo->tm_mon + 1) + "-" +
        std::to_string(ptminfo->tm_mday) + "@" +
        std::to_string(ptminfo->tm_hour) + ":" +
        std::to_string(ptminfo->tm_min) + ":" + std::to_string(ptminfo->tm_sec);
  } else {
    s = std::to_string(ptminfo->tm_year + 1900) + "-" +
        std::to_string(ptminfo->tm_mon + 1) + "-" +
        std::to_string(ptminfo->tm_mday) + "@" +
        std::to_string(ptminfo->tm_hour) + ":" +
        std::to_string(ptminfo->tm_min) + ":" + std::to_string(ptminfo->tm_sec);
  }

  std::string probe_file = "../../../bench_results/crash_tests/" + bench_name +
                           "/tp_probe@" + system_name + "@" + s + ".txt";
  of_probe_tp.open(probe_file.c_str(), std::ios::out);
  for (int i = 0; i < tp_probe_vec.size(); i++) {
    for (int j = 0; j < tp_probe_vec[i].size(); j++) {
      // sum-up all threads' tp
      if (tp_time_figure.find(tp_probe_vec[i][j].ctr) == tp_time_figure.end()) {
        tp_time_figure[tp_probe_vec[i][j].ctr] = tp_probe_vec[i][j].tp;
      } else {
        tp_time_figure[tp_probe_vec[i][j].ctr] += tp_probe_vec[i][j].tp;
      }

      if (attemp_tp_time_figure.find(tp_probe_vec[i][j].ctr) ==
          attemp_tp_time_figure.end()) {
        attemp_tp_time_figure[tp_probe_vec[i][j].ctr] =
            tp_probe_vec[i][j].attemp_tp;
      } else {
        attemp_tp_time_figure[tp_probe_vec[i][j].ctr] +=
            tp_probe_vec[i][j].attemp_tp;
      }
    }
  }

  std::string config_filepath = "../../../config/cn_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto client_conf = json_config.get("local_compute_node");
  int tp_probe_interval_ms =
      (int)client_conf.get("tp_probe_interval_ms").get_int64();

  int start_time = tp_time_figure.begin()->first;
  auto iter = tp_time_figure.end();
  iter--;
  int end_time = iter->first;

  for (int i = start_time; i <= end_time; i++) {
    auto iter = tp_time_figure.find(i);
    if (iter != tp_time_figure.end()) {
      of_probe_tp << iter->first * tp_probe_interval_ms << " "
                  << iter->second / 1000.0 << std::endl;
    } else {
      of_probe_tp << i << " " << 0 << std::endl;
    }
  }

  of_probe_tp.close();
#endif
}
