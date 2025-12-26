// Author: Ming Zhang
// Copyright (c) 2022

#include "worker/handler.h"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <numeric>
#include <thread>
#include <unordered_set>

#include "dtx/stat.h"
#include "util/json_config.h"
#include "worker/worker.h"

std::atomic<uint64_t> tx_id_generator;
std::atomic<uint64_t> connected_t_num;

std::vector<t_id_t> tid_vec;
std::vector<double> attemp_tp_vec;
std::vector<double> tp_vec;
std::vector<double> medianlat_vec;
std::vector<double> taillat_vec;
std::map<int, std::vector<double>> tx_lat_vec;
std::vector<int> exec_vec;
std::vector<int> validate_vec;
std::vector<int> commit_vec;
std::vector<double> lock_durations;
std::vector<uint64_t> total_try_times;
std::vector<uint64_t> total_commit_times;

EventCount event_counter;

void Handler::ConfigureComputeNode(int argc, char *argv[]) {
  std::string config_file = "../../../config/compute_node_config.json";
  std::string system_name = std::string(argv[2]);
  if (argc == 5) {
    std::string s1 =
        "sed -i '5c \"thread_num_per_machine\": " + std::string(argv[3]) +
        ",' " + config_file;
    std::string s2 = "sed -i '6c \"coroutine_num\": " + std::string(argv[4]) +
                     ",' " + config_file;
    system(s1.c_str());
    system(s2.c_str());
  }
  // Customized test without modifying configs
  int txn_system_value = 0;
  if (system_name.find("farm") != std::string::npos) {
    txn_system_value = 0;
  } else if (system_name.find("drtm") != std::string::npos) {
    txn_system_value = 1;
  } else if (system_name.find("ford") != std::string::npos) {
    txn_system_value = 2;
  } else if (system_name.find("local") != std::string::npos) {
    txn_system_value = 3;
  }
  std::string s =
      "sed -i '8c \"txn_system\": " + std::to_string(txn_system_value) + ",' " +
      config_file;
  system(s.c_str());
  return;
}

void Handler::GenThreads(std::string bench_name) {
  std::string config_filepath = "../../../config/compute_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto client_conf = json_config.get("local_compute_node");
  node_id_t machine_num = (node_id_t)client_conf.get("machine_num").get_int64();
  node_id_t machine_id = (node_id_t)client_conf.get("machine_id").get_int64();
  t_id_t thread_num_per_machine =
      (t_id_t)client_conf.get("thread_num_per_machine").get_int64();
  const int coro_num = (int)client_conf.get("coroutine_num").get_int64();
  assert(machine_id >= 0 && machine_id < machine_num);

  /* Start working */
  tx_id_generator = 0; // Initial transaction id == 0
  connected_t_num = 0; // Sync all threads' RDMA QP connections
  auto thread_arr = new std::thread[thread_num_per_machine];

  auto *global_meta_man = new MetaManager();
  auto *global_vcache = new VersionCache();
  auto *global_lcache = new LockCache();
  RDMA_LOG(INFO) << "Alloc local memory: "
                 << (size_t)(thread_num_per_machine * PER_THREAD_ALLOC_SIZE) /
                        (1024 * 1024)
                 << " MB. Waiting...";
  auto *global_rdma_region =
      new RDMARegionAllocator(global_meta_man, thread_num_per_machine);

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

  // Generate workloads for SmallBank, MICRO and TATP
  std::vector<void *> smallbank_txn_params[64 * 3];
  std::vector<SmallBankTxType> smallbank_txn_param_type[64 * 3];

  // Generate workloads for SmallBank, MICRO and TATP
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

    auto txn_types = micro_client->CreateWorkgenArray();
    const int kRecordNum = micro_client->num_op;

    // Initialize 1000000 transaction params:
    for (int i = 0; i < kTxnNum * machine_num; ++i) {
      auto tid = i % (thread_num_per_machine * machine_num);
      auto t = txn_types[int(r.NextUniform() * 100)];
      micro_txn_param_type[tid].push_back(t);
      switch (t) {
      case MicroTxType::kUpdate: {
        // Generate a few different keys
        UpdateTxnParam *param = new UpdateTxnParam();
        param->key_num = kRecordNum;
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
        std::sort(param->keys, param->keys + kRecordNum);
        micro_txn_params[tid].push_back(param);
        break;
      }
      case MicroTxType::kRead: {
        ReadTxnParam *param = new ReadTxnParam();
        param->key_num = kRecordNum;
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
        std::sort(param->keys, param->keys + kRecordNum);
        micro_txn_params[tid].push_back(param);
        break;
      }
      default:
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
        uint64_t start_time = fast_randoms[tid].RandNumber(0, 3) * 8;
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

  // Wait for all compute nodes ready
  memcached_wrapper->ConnectToMemcached();
  memcached_wrapper->AddServer();
  memcached_wrapper->SyncComputeNodes();

  RDMA_LOG(INFO) << "Spawn threads to execute...";

  for (t_id_t i = 0; i < thread_num_per_machine; i++) {
    param_arr[i].thread_local_id = i;
    param_arr[i].thread_global_id = (machine_id * thread_num_per_machine) + i;

    SmallBank *smallbank_c = new SmallBank();
    smallbank_c->coro_num = coro_num;
    smallbank_c->txn_run_types =
        &smallbank_txn_param_type[param_arr[i].thread_global_id];
    smallbank_c->txn_run_params =
        &smallbank_txn_params[param_arr[i].thread_global_id];

    MICRO *micro_c = new MICRO();
    micro_c->coro_num = coro_num;
    micro_c->txn_run_types =
        &micro_txn_param_type[param_arr[i].thread_global_id];
    micro_c->txn_run_params = &micro_txn_params[param_arr[i].thread_global_id];

    TATP *tatp_c = new TATP();
    tatp_c->coro_num = coro_num;
    tatp_c->txn_run_types = &tatp_txn_param_type[i];
    tatp_c->txn_run_params = &tatp_txn_params[i];

    param_arr[i].coro_num = coro_num;
    param_arr[i].bench_name = bench_name;
    param_arr[i].global_meta_man = global_meta_man;
    param_arr[i].global_status = global_vcache;
    param_arr[i].global_lcache = global_lcache;
    param_arr[i].global_rdma_region = global_rdma_region;
    param_arr[i].thread_num_per_machine = thread_num_per_machine;
    param_arr[i].total_thread_num = thread_num_per_machine * machine_num;
    thread_arr[i] = std::thread(run_thread, &param_arr[i], tatp_c, smallbank_c,
                                tpcc_client, micro_c);

    /* Pin thread i to hardware thread i */
    // Do not set affinity, we will use numactl to set the affinity
    // cpu_set_t cpuset;
    // CPU_ZERO(&cpuset);
    // CPU_SET(i, &cpuset);
    // int rc = pthread_setaffinity_np(thread_arr[i].native_handle(),
    //                                 sizeof(cpu_set_t), &cpuset);
    // if (rc != 0) {
    //   RDMA_LOG(WARNING) << "Error calling pthread_setaffinity_np: " << rc;
    // }
  }

  for (t_id_t i = 0; i < thread_num_per_machine; i++) {
    if (thread_arr[i].joinable()) {
      thread_arr[i].join();
    }
  }

  RDMA_LOG(INFO) << "DONE";

  delete[] param_arr;
  delete global_rdma_region;
  delete global_meta_man;
  delete global_vcache;
  delete global_lcache;
  if (tatp_client)
    delete tatp_client;
  if (smallbank_client)
    delete smallbank_client;
  if (tpcc_client)
    delete tpcc_client;
}

void Handler::OutputResult(std::string bench_name, std::string system_name) {
  std::string results_cmd = "mkdir -p ../../../bench_results/" + bench_name;
  system(results_cmd.c_str());
  std::ofstream of, of_detail, of_abort_rate;
  std::string res_file = "../../../bench_results/" + bench_name + "/result.txt";
  std::string detail_res_file =
      "../../../bench_results/" + bench_name + "/detail_result.txt";
  std::string abort_rate_file =
      "../../../bench_results/" + bench_name + "/abort_rate.txt";

  of_detail.open(detail_res_file.c_str(), std::ios::app);
  of_abort_rate.open(abort_rate_file.c_str(), std::ios::app);

  of_detail << system_name << std::endl;
  of_detail << "tid attemp_tp tp 50lat 99lat" << std::endl;

  of_abort_rate << system_name << " tx_type try_num commit_num abort_rate"
                << std::endl;

  double total_attemp_tp = 0;
  double total_tp = 0;
  double total_median = 0;
  double total_tail = 0;

  for (int i = 0; i < tid_vec.size(); i++) {
    of_detail << tid_vec[i] << " " << attemp_tp_vec[i] << " " << tp_vec[i]
              << " " << medianlat_vec[i] << " " << taillat_vec[i] << std::endl;
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

  of_detail << total_attemp_tp << " " << total_tp << " " << medianlat_vec[0]
            << " " << medianlat_vec[thread_num - 1] << " " << avg_median << " "
            << taillat_vec[0] << " " << taillat_vec[thread_num - 1] << " "
            << avg_tail << std::endl;

  // of.open(res_file.c_str(), std::ios::app);
  // of << system_name << " " << total_attemp_tp / 1000 << " " << total_tp /
  // 1000
  //    << " " << avg_median << " " << avg_tail << std::endl;

  if (bench_name == "tatp") {
    for (int i = 0; i < TATP_TX_TYPES; i++) {
      of_abort_rate << TATP_TX_NAME[i] << " " << total_try_times[i] << " "
                    << total_commit_times[i] << " "
                    << (double)(total_try_times[i] - total_commit_times[i]) /
                           (double)total_try_times[i]
                    << std::endl;
    }
  } else if (bench_name == "smallbank") {
    uint64_t all_tried_count = 0, all_committed_count = 0;
    for (int i = 0; i < SmallBank_TX_TYPES; i++) {
      of_abort_rate << SmallBank_TX_NAME[i] << " " << total_try_times[i] << " "
                    << total_commit_times[i] << " "
                    << (double)(total_try_times[i] - total_commit_times[i]) /
                           (double)total_try_times[i]
                    << std::endl;
      all_tried_count += total_try_times[i];
      all_committed_count += total_commit_times[i];
    }
    of_abort_rate << "All Tried: " << all_tried_count
                  << ", All Committed: " << all_committed_count
                  << ", Abort Rate: "
                  << (double)(all_tried_count - all_committed_count) /
                         (double)all_tried_count
                  << std::endl;
  } else if (bench_name == "tpcc") {
    uint64_t all_tried_count = 0, all_committed_count = 0;
    for (int i = 0; i < TPCC_TX_TYPES; i++) {
      of_abort_rate << TPCC_TX_NAME[i] << " " << total_try_times[i] << " "
                    << total_commit_times[i] << " "
                    << (double)(total_try_times[i] - total_commit_times[i]) /
                           (double)total_try_times[i]
                    << std::endl;
      all_tried_count += total_try_times[i];
      all_committed_count += total_commit_times[i];
    }
    of_abort_rate << "All Tried: " << all_tried_count
                  << ", All Committed: " << all_committed_count
                  << ", Abort Rate: "
                  << (double)(all_tried_count - all_committed_count) /
                         (double)all_tried_count
                  << std::endl;
  }

  of_detail << std::endl;
  of_abort_rate << std::endl;

  of.close();
  of_detail.close();
  of_abort_rate.close();

  std::cerr << system_name << " " << total_attemp_tp / 1000 << " "
            << total_tp / 1000 << " " << avg_median << " " << avg_tail
            << std::endl;

  // The following codes about breakdown are copied from Motor's implementation
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
    uint64_t all_tried_count = 0, all_committed_count = 0;
    for (int i = 0; i < SmallBank_TX_TYPES; i++) {
      all_tried_count += total_try_times[i];
      all_committed_count += total_commit_times[i];
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
    of << "All Tried: " << all_tried_count
       << ", All committed: " << all_committed_count << ", Abort Rate: "
       << (double)(all_tried_count - all_committed_count) /
              (double)all_tried_count
       << std::endl;
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
                            (double)(total_try_times[i] + 1)
                     << std::endl;
      std::cout << MICRO_TX_NAME[i] << " " << total_try_times[i] << " "
                << total_commit_times[i] << " "
                << (double)(total_try_times[i] - total_commit_times[i]) /
                       (double)(total_try_times[i] + 1)
                << std::endl;
    }
  }

  event_counter.Output(of_event_count);

  of_event_count.close();

  // Generate the latency file:
  std::ofstream of_latency("../../../latency.txt", std::ios::out);
  const int elements_per_line = 20; // Default value set to 20
  double discard_rate = 0.05;
  std::vector<double> avg_lantecy;
  std::vector<double> p50_lantecy;
  std::vector<double> p99_lantecy;
  std::vector<double> p999_lantecy;
  std::vector<double> total_latency;
  for (const auto &entry : tx_lat_vec) {
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

    size_t i = 0;
    for (const auto &latency : entry.second) {
      // Only store every 1000 records, to save space:
      if (i++ % 1000 == 0) {
        of_latency << latency << " ";
        if (++count % elements_per_line == 0) {
          of_latency << std::endl;
        }
      }
    }
    of_latency << std::endl;
  }

  std::sort(exec_vec.begin(), exec_vec.end());
  uint64_t exec_total = std::accumulate(exec_vec.begin(), exec_vec.end(), 0);
  uint64_t exec_avg = exec_total / exec_vec.size();
  uint64_t exec_p50 = exec_vec[exec_vec.size() / 2];
  uint64_t exec_p99 = exec_vec[exec_vec.size() * 99 / 100];
  uint64_t exec_p999 = exec_vec[exec_vec.size() * 999 / 1000];

  std::sort(validate_vec.begin(), validate_vec.end());
  uint64_t validate_total =
      std::accumulate(validate_vec.begin(), validate_vec.end(), 0);
  uint64_t validate_avg = validate_total / validate_vec.size();
  uint64_t validate_p50 = validate_vec[validate_vec.size() / 2];
  uint64_t validate_p99 = validate_vec[validate_vec.size() * 99 / 100];
  uint64_t validate_p999 = validate_vec[validate_vec.size() * 999 / 1000];

  std::sort(commit_vec.begin(), commit_vec.end());
  uint64_t commit_total =
      std::accumulate(commit_vec.begin(), commit_vec.end(), 0);
  uint64_t commit_avg = commit_total / commit_vec.size();
  uint64_t commit_p50 = commit_vec[commit_vec.size() / 2];
  uint64_t commit_p99 = commit_vec[commit_vec.size() * 99 / 100];
  uint64_t commit_p999 = commit_vec[commit_vec.size() * 999 / 1000];

  // Write into the latency summary file:
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

  of.open(res_file.c_str(), std::ios::app);
  of << system_name << " " << total_attemp_tp / 1000 << " " << total_tp / 1000
     << " " << total_latency_avg << " " << total_latency_p50 << " "
     << total_latency_p99 << " " << total_latency_p999 << " "
     // Dump the latency breakdown results
     << exec_avg << " "
     << validate_avg << " "
     << commit_avg << std::endl;

  // Open it when testing the duration
#if LOCK_WAIT
  if (bench_name == "MICRO") {
    // print avg lock duration
    std::string file =
        "../../../bench_results/" + bench_name + "/avg_lock_duration.txt";
    of.open(file.c_str(), std::ios::app);

    double total_lock_dur = 0;
    for (int i = 0; i < lock_durations.size(); i++) {
      total_lock_dur += lock_durations[i];
    }

    of << system_name << " " << total_lock_dur / lock_durations.size()
       << std::endl;
    std::cerr << system_name
              << " avg_lock_dur: " << total_lock_dur / lock_durations.size()
              << std::endl;
  }
#endif
}

void Handler::ConfigureComputeNodeForMICRO(int argc, char *argv[]) {
  std::string workload_filepath = "../../../config/micro_config.json";
  std::string arg = std::string(argv[1]);
  char access_type = arg[0];
  std::string s;
  if (access_type == 's') {
    // skewed
    s = "sed -i '4c \"is_skewed\": true,' " + workload_filepath;
  } else if (access_type == 'u') {
    // uniform
    s = "sed -i '4c \"is_skewed\": false,' " + workload_filepath;
  }
  system(s.c_str());
  // write ratio
  std::string write_ratio = arg.substr(2); // e.g: 75
  s = "sed -i '7c \"write_ratio\": " + write_ratio + ",' " + workload_filepath;
  system(s.c_str());
}

void Handler::GenThreadsForMICRO() {
  std::string config_filepath = "../../../config/compute_node_config.json";
  std::string s = "sed -i '8c \"txn_system\": 2,' " + config_filepath;
  system(s.c_str());
  auto json_config = JsonConfig::load_file(config_filepath);
  auto client_conf = json_config.get("local_compute_node");
  node_id_t machine_num = (node_id_t)client_conf.get("machine_num").get_int64();
  node_id_t machine_id = (node_id_t)client_conf.get("machine_id").get_int64();
  t_id_t thread_num_per_machine =
      (t_id_t)client_conf.get("thread_num_per_machine").get_int64();
  lock_durations.resize(thread_num_per_machine);
  const int coro_num = (int)client_conf.get("coroutine_num").get_int64();
  assert(machine_id >= 0 && machine_id < machine_num);

  std::string thread_num_coro_num;
  if (coro_num < 10) {
    thread_num_coro_num = std::to_string(thread_num_per_machine) + "_0" +
                          std::to_string(coro_num);
  } else {
    thread_num_coro_num =
        std::to_string(thread_num_per_machine) + "_" + std::to_string(coro_num);
  }

  system(std::string("mkdir -p ../../../bench_results/MICRO/" +
                     thread_num_coro_num)
             .c_str());
  system(std::string("rm ../../../bench_results/MICRO/" + thread_num_coro_num +
                     "/total_lock_duration.txt")
             .c_str());

  /* Start working */
  tx_id_generator = 0; // Initial transaction id == 0
  connected_t_num = 0; // Sync all threads' RDMA QP connections
  auto thread_arr = new std::thread[thread_num_per_machine];
  auto *global_meta_man = new MetaManager();
  auto *global_vcache = new VersionCache();
  auto *global_lcache = new LockCache();
  RDMA_LOG(INFO) << "Alloc local memory: "
                 << (size_t)(thread_num_per_machine * PER_THREAD_ALLOC_SIZE) /
                        (1024 * 1024)
                 << " MB. Waiting...";
  auto *global_rdma_region =
      new RDMARegionAllocator(global_meta_man, thread_num_per_machine);

  auto *param_arr = new struct thread_params[thread_num_per_machine];

  RDMA_LOG(INFO) << "spawn threads...";
  for (t_id_t i = 0; i < thread_num_per_machine; i++) {
    param_arr[i].thread_local_id = i;
    param_arr[i].thread_global_id = (machine_id * thread_num_per_machine) + i;
    param_arr[i].coro_num = coro_num;
    param_arr[i].global_meta_man = global_meta_man;
    param_arr[i].global_status = global_vcache;
    param_arr[i].global_lcache = global_lcache;
    param_arr[i].global_rdma_region = global_rdma_region;
    param_arr[i].thread_num_per_machine = thread_num_per_machine;
    param_arr[i].total_thread_num = thread_num_per_machine * machine_num;
    param_arr[i].bench_name = "micro";
    thread_arr[i] = std::thread(run_thread, &param_arr[i], nullptr, nullptr,
                                nullptr, nullptr);

    /* Pin thread i to hardware thread */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
    pthread_setaffinity_np(thread_arr[i].native_handle(), sizeof(cpu_set_t),
                           &cpuset);
  }

  for (t_id_t i = 0; i < thread_num_per_machine; i++) {
    if (thread_arr[i].joinable()) {
      thread_arr[i].join();
    }
  }
  RDMA_LOG(INFO) << "Done";

  delete[] param_arr;
  delete global_rdma_region;
  delete global_meta_man;
  delete global_vcache;
  delete global_lcache;
}
