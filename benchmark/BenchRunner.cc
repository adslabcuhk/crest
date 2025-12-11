#include <gflags/gflags.h>

#include "SmallBank/SmallBankBenchmark.h"
#include "SmallBank/SmallBankContext.h"
#include "TATP/TATPBenchmark.h"
#include "TATP/TATPContext.h"
#include "TPCC/TpccBenchmark.h"
#include "TPCC/TpccContext.h"
#include "YCSB/YCSBBenchmark.h"
#include "YCSB/YCSBContext.h"
#include "mempool/PoolMeta.h"
#include "util/Logger.h"
#include "util/Timer.h"

// gflags definition
DEFINE_string(type, "", "Type of this node, e.g., CN or MN");
DEFINE_string(config, "", "Path to the configuration file");
DEFINE_string(workload, "", "Name of the workload to execute, e.g., TPC-C, YCSB");
DEFINE_int32(id, -1, "Id of this CN node");
DEFINE_int32(threads, 1, "Number of threads to execute");
DEFINE_int32(coro, 2, "Number of coroutines of each thread");
DEFINE_int32(duration, 5, "Duration (seconds) to run this benchmark");
DEFINE_int32(txn_num, 1000000, "Number of transactions to run in this benchmark");
DEFINE_bool(replay, false, "Whether replay the benchmark to test correctness");
DEFINE_string(output, "", "Path to the output file");

static constexpr uint16_t kSocketPort = 10001;

Benchmark::BenchmarkConfig ParseBenchmarkConfig() {
    ASSERT(FLAGS_id >= 0, "Invalid node id");
    ASSERT(!FLAGS_config.empty(), "Configuration files invalid");
    ASSERT(!FLAGS_workload.empty(), "Workload not specified");
    ASSERT(!FLAGS_type.empty(), "Node type not specified");

    Benchmark::BenchmarkConfig bench_config;
    bench_config.workload = FLAGS_workload;
    bench_config.thread_num = FLAGS_threads;
    bench_config.coro_num = FLAGS_coro;
    bench_config.dura = FLAGS_duration;
    bench_config.txn_num = FLAGS_txn_num;
    bench_config.replay = FLAGS_replay;
    bench_config.output_dir = FLAGS_output;

    auto json_config = JsonConfig::load_file(FLAGS_config);

    auto all_cn_config = json_config.get("cn");
    auto all_mn_config = json_config.get("mn");

    if (FLAGS_type == "CN" || FLAGS_type == "cn") {
        // Generate the CN configurations
        auto my_config = all_cn_config.get(FLAGS_id);
        NodeInitAttr& init_attr = bench_config.node_init_attr;
        init_attr.node_type = kCN;
        init_attr.nid = FLAGS_id;
        init_attr.num_cns = all_cn_config.size();
        init_attr.num_mns = all_mn_config.size();
        init_attr.devname = my_config.get("devname").get_str(),
        init_attr.ib_port = (uint32_t)my_config.get("ibport").get_uint64(),
        init_attr.gid_idx = (int)my_config.get("gid").get_uint64(),
        init_attr.mr_size = my_config.get("mr_size").get_uint64() * (1ULL << 30),
        init_attr.ip = my_config.get("ip").get_str();
        init_attr.socket_port = kSocketPort;

        // Generate the remote MNs' contact information
        size_t mn_number = json_config.get("mn").size();

        // Create the contact info
        std::vector<RemoteNodeAttr>& contact_info = bench_config.pool_attr;
        for (int i = 0; i < mn_number; ++i) {
            auto mn_config = json_config.get("mn").get(i);
            contact_info.push_back(RemoteNodeAttr{
                .node_type = kMN,
                .nid = (node_id_t)mn_config.get("id").get_uint64(),
                .ip = mn_config.get("ip").get_str(),
                .socket_port = kSocketPort,
            });
        }
    } else if (FLAGS_type == "MN" || FLAGS_type == "mn") {
        NodeInitAttr& init_attr = bench_config.node_init_attr;
        auto my_config = all_mn_config.get(FLAGS_id);
        init_attr.node_type = kMN;
        init_attr.nid = FLAGS_id;
        init_attr.num_cns = all_cn_config.size();
        init_attr.num_mns = all_mn_config.size();
        init_attr.devname = my_config.get("devname").get_str(),
        init_attr.ib_port = (uint32_t)my_config.get("ibport").get_uint64(),
        init_attr.gid_idx = (int)my_config.get("gid").get_uint64(),
        init_attr.mr_size = my_config.get("mr_size").get_uint64() * (1ULL << 30),
        init_attr.ip = my_config.get("ip").get_str();
        init_attr.socket_port = kSocketPort;

        // Generate the remote CNs' contact information
        size_t cn_number = json_config.get("cn").size();

        // Create the contact info
        std::vector<RemoteNodeAttr>& contact_info = bench_config.pool_attr;
        for (int i = 0; i < cn_number; ++i) {
            auto cn_config = json_config.get("cn").get(i);
            contact_info.push_back(RemoteNodeAttr{
                .node_type = kCN,
                .nid = (node_id_t)cn_config.get("id").get_uint64(),
                .ip = cn_config.get("ip").get_str(),
                .socket_port = kSocketPort,
            });
        }
    }

    // Create the table information
    if (FLAGS_workload == "tpcc") {
        bench_config.workload_config = new TpccConfig();
        *(TpccConfig*)(bench_config.workload_config) = ParseTpccConfig(FLAGS_config);
    } else if (FLAGS_workload == "smallbank") {
        bench_config.workload_config = new SmallBankConfig();
        *(SmallBankConfig*)(bench_config.workload_config) = ParseSmallBankConfig(FLAGS_config);
    } else if (FLAGS_workload == "ycsb") {
        bench_config.workload_config = new YCSBConfig();
        *(YCSBConfig*)(bench_config.workload_config) = ParseYCSBConfig(FLAGS_config);
    } else if (FLAGS_workload == "tatp") {
        bench_config.workload_config = new TATPConfig();
        *(TATPConfig*)(bench_config.workload_config) = ParseTATPConfig(FLAGS_config);
    }

    return bench_config;
}

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    Benchmark::BenchmarkConfig bench_config = ParseBenchmarkConfig();
    LOG_INFO("Read configuration file succeed");

    Benchmark* bench = nullptr;
    if (FLAGS_workload == "tpcc") {
        bench = new TpccBenchmark();
    } else if (FLAGS_workload == "smallbank") {
        bench = new SmallBankBenchmark();
    } else if (FLAGS_workload == "ycsb") {
        bench = new YCSBBenchmark();
    } else if (FLAGS_workload == "tatp") {
        bench = new TATPBenchmark();
    }

    util::Timer timer;

    timer.reset();
    Status s = bench->Initialize(bench_config);
    ASSERT(s.ok(), "benchmark initialization failed");
    LOG_INFO("Benchmark initialization done, takes: %.2lf ms", timer.ms_elapse());

    bench->PrepareWorkloads();

    s = bench->Run();
    ASSERT(s.ok(), "benchmark run failed");

    if (FLAGS_workload == "tpcc") {
        // delete (TpccBenchmark*)bench;
    } else if (FLAGS_workload == "smallbank") {
        // delete (SmallBankBenchmark*)bench;
    }

    return 0;
}