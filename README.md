# CREST

## Introduction
CREST is a high-performance transactional processing system built on disaggrgated memory (DM). CREST
targets highly-contented workload and provides high throughput under skewed workloads. This repo 
contains the source code of CREST prototype, and scripts to configure and running experiment. 

- `src/`: includes the implementation of CREST system, which is implemented from scratch and relies 
on only a few third-party libraries. 
- `benchmark/`: includes the benchmarking workloads we implemented using CREST's interface. The user
can add their customized workload under this directory. 
- `config/`: includes the cofiguration file for each workload. 
- `scripts/`: includes the script for configurating the repo, and running the experiments. 

## Prerequisites

### Testbed

#### RDMA Support
CREST is a transaction processing system on DM, which requires multiple machines equipped with RDMA
to act as memory pool and compute pool respectively. Specifically, CREST requires each machine to install the 
driver with version **MLNX_OFED_LINUX-4.9-7.1.0.0** to enable using of RDMA experimental verbs. 

>  NOTE: 
The driver version MUST be **MLNX_OFED_LINUX-4.9-7.1.0.0** as ``masked-CAS`` is only avaliable under this 
version

#### Compiler Support
CREST requires gcc and g++ with version >= 11, please install the compilers with proper version. 
In Ubuntu, you may add the compilers via ppa:

```shell
sudo add-apt-repository ppa:ubuntu-toolchain-r/test
sudo apt update && sudo apt install gcc-11 g++-11
# Use gcc-11 and g++-11 by default
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 110
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-11 110
```

Besides, we use cmake as our building tool, install cmake3 using the following commands:
```shell
sudo apt-get install -y cmake
```

### C++ Third-party dependencies

CREST builds with multiple third-party open-sourced libraries, such as boost, memcached, etc. We 
recommand you to run CREST on Ubuntu20.04, in which we test the code and scripts carefully. The 
followings are the instructions to install the dependencies (on Ubuntu20.04):

- Common third-libs such as TBB, Memcached, gflags
```shell
sudo apt-get install -y libgflags-dev libtbb-dev libmemcached-dev memcached libmemcached-tools
```

- libboost: we need a specific version of boost (1.83) to build CREST, here are the instructions:
```shell
wget https://archives.boost.io/release/1.83.0/source/boost_1_83_0.tar.gz
tar -zxf boost_1_83_0.tar.gz
cd boost_1_83_0
./bootstrap.sh --prefix=path/to/installation/prefix
sudo ./b2 install
```

- abseil: CREST leverages some base toolkits from the abseil C++ library, here are the install instructions:
```shell
git clone git@github.com:abseil/abseil-cpp.git
cd abseil-cpp
cmake -B build
cd build
sudo make install
```

### Scripts dependencies
We provide python scripts (under ``scripts`` directory) to help reproduce the results illustrated in paper. Before running these python scripts, run the following command to install the dependencies:
```
cd scripts
pip install -r requirements
```

---
## Build
After installing the above dependencies, building CREST using the following commands:
```shell
cd CREST
make release
```
After successfully building the entire codebase, you will see the following binary file:``build/benchmark/bench_runner``. This is the binary file we will use for benchmarking. 

## Run Experiment

### Setup configuration file for scripts

In CREST, the python scripts (under ``scripts`` directory) rely on a configuration file 
(``scripts/bench/config.py``) to specify the IP address, the project path and other necessary information of the testbed.

### Setup configuration file for workloads
In CREST, each configuration file (``xxx.json`` under ``config`` directory) contains all necessary
information of this workload. It contains three parts: 
- ``mn``: The configuration of each server comprising memory pool.
- ``cn``: The configuration of each server comprising compute pool.
- workload-specific configurations: Configurations related to a specific workload.
Take ``ycsb_config.json`` as an example:
``` json
{
  "mn": [
    {
      "id": 0,
      "ip": "10.118.0.45",
      "devname": "mlx5_1",
      "ibport": 1,
      "gid": 3,
      "mr_size": 64
    }
  ],
  "cn": [
    {
      "id": 0,
      "ip": "10.118.0.36",
      "devname":"mlx5_0",
      "ibport": 1,
      "gid": 3,
      "mr_size": 4
    },
  ],
  "ycsb": {
    "num_records": 1000000,
    "zipfian": 0.99,
    "num_op": 4,
    "write_ratio": 100
  }
}
```
In this example, both compute pool and memory pool only contains one single node. For each node, you need to configure the following fields:
- The unique ``id`` and ``ip`` address of this node
- ``devname``, ``ibport`` and ``gid`` that are used to initialize the RDMA device
- ``mr_size`` is the size of memory region used for storing data. You should create a large memory region for memory node and a small memory region for compute node.

If you wish to add more nodes, follow the above instructions to configure your node. 

### Run node

CREST supports mannually start the memory nodes and compute nodes, using the binary file ``bench_runner`` generated from compilation. 
```shell
# Memory node startup: 
./bench_runner --type=mn --id=<id> --config=<path_to_config> --workload=<workload>\
--threads=<threads> --coro=<coros>

# Compute ndoe startup:
./bench_runner --type=cn --id=<id> --config=<path_to_config> --workload=<workload>\
--threads=<threads> --coro=<coros> --txn_num=<txn_num> --output=<path_to_output>
```
The ``bench_runner`` has multiple parameters. Note that the following parameters need to be 
carefully set: 
* ``type``: set it to be ``mn`` or ``cn``
* ``id``: the unique identifier of the node. When starting up the node, make sure you are setting the correct id for this node.
* ``config``: set it to be the path to the configuration file
* ``workload``: the workload to execute. It needs to be consistent with the ``config` path.

After successfully boosting the memory node, the sample output would be:
```shell
[        38][BenchRunner.cc:130] Read configuration file succeed
[  20943675][Memcached.h:81] Successfully Set Server Number to be 0
[1160014960][Pool.cc:23] Memory register and initialization finished
[1160036248][Benchmark.cc:104] Create memory region 0x7fa75389d000, size 170 GiB
[1160047423][TpccBenchmarkExecutor.cc:330] MN Log Area addr: 0x7fa75389e000, size: 720 MiB
[1160492283][TpccBenchmarkPopulator.cc:332] [Primary] [Warehouse  Table] addr: 0x7fa78089e000, record_size:  256, insert:      100, failed:        0, consumes: 0.02 MiB
[1160495113][TpccBenchmarkPopulator.cc:392] [Backup ] [District   Table] addr: 0x7fa7808a4400, record_size:  320, insert:     1000, failed:        0, consumes: 0.31 MiB
...
[1200881073][TpccBenchmarkPopulator.cc:280] Populate database records: 4083.29 ms
[1200881096][TpccBenchmarkPopulator.cc:281] Primary Table Num: 5
[1200881115][TpccBenchmarkPopulator.cc:282] Backup Table Num: 4
[1200881162][TpccBenchmarkExecutor.cc:335] MN Populdate tables done
[1200881187][TpccBenchmarkExecutor.cc:354] MN write database metadata done
[1200881219][TpccBenchmarkExecutor.cc:355] MN Initialization Takes 117994.18 ms
[1200881256][Pool.cc:47] MN0 waits for incomming connection
```

The output indicates that the memory node is succesfully boosted and populate the database records successfully, you can now boost the compute node to run the workload using the above commands. 

## Run from scripts
CREST also provides scripts for users to reproduce the results presented in the paper. The scripts are written in python3 and relied on a few python3 librarires, 
use the following commands to install the libraries: 
```shell
pip3 install numpy scp paramiko
```
Then execute the following commands to reproduce the experimental results
* Exp#1 - Exp#3:
```shell
cd scripts
python3 run_scalability_bench.py crest tpcc
python3 run_scalability_bench.py crest smallbank
python3 run_scalability_bench.py crest ycsb
```

* Exp#4
```shell
cd scripts
python3 run_breakdown.py crest tpcc
python3 run_breakdown.py crest smallbank
python3 run_breakdown.py crest ycsb
```

* Exp#6
```shell
cd scripts
python3 run_contentionlevel_bench.py crest ycsb
```

* Exp#7 
```shell
cd scripts
python3 run_sensitivity.py crest ycsb
```

* Exp#8
```shell
cd scripts
python3 run_write_ratio.py crest ycsb
```

After running each script, the script will automatically gather the results from each compute node, and
aggregates them in a single file, e.g., ``crest_tpcc_aggregated_thpt``. 

The data in the aggregated file is stored in the following format:

```
       file << "crest " << tried_thpt * 1000.0 << " " << committed_thpt * 1000.0 << " "
               << avg_latency << " " << p50_latency << " " << p90_latency << " " << p99_latency
               << " " << p999_latency << " " << avg_exec_latency << " " << avg_validate_latency
               << " " << avg_commit_latency << "\n";
```
