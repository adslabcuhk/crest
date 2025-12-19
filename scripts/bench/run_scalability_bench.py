import numpy as np
import crest
import time
import sys
import config

# TPC-C
NUM_WAREHOUSE = 40

# SmallBank
NUM_ACCOUNTS = 100000
ZIPFIAN = 0.99

# Micro or YCSB
NUM_KEYS = 1000000
ZIPFIAN = 0.99
NUM_OP = 4

def run_crest_scalability(workload:str):
    bench = crest.CrestBenchmark(workload, config.cns, config.mns)
    bench.build_project()
    bench.clean_bench_results()
    wait_load_time = 0
    if workload == "tpcc":
      bench.setup_tpcc_configuration(NUM_WAREHOUSE)
      # TPC-C takes much longer time to populate data
      wait_load_time = 240
    if workload == "smallbank":
      bench.setup_smallbank_configuration(NUM_ACCOUNTS, ZIPFIAN)
      wait_load_time = 30
    if workload == "ycsb":
      bench.setup_ycsb_configuration(NUM_KEYS, ZIPFIAN, NUM_OP)
      wait_load_time = 30

    # Thread number increase from 4 to 40, the number of total coordinators 
    # increases from 24 to 240
    thread_num = [1,2] + list(np.arange(4, 44, 4))

    max_try_cnt = 5
    tried_num = [0] * len(thread_num)
    test_results = [False] * len(thread_num)

    start_time = time.time()

    for i, t in enumerate(thread_num):
      res = bench.run_once(t, 3, wait_load_time)
      try_cnt = 1
      while res is False and try_cnt < max_try_cnt: 
        res = bench.run_once(t, 3, wait_load_time)
        try_cnt += 1

      test_results[i] = res
      tried_num[i] = try_cnt

    # process the results
    bench.get_results()
    bench.process_results()

    end_time = time.time()
    minutes = (end_time - start_time) / 60
    # Summary: 
    print("[Crest {} Execution Results][Execution Time: {:.2f} Minutes]".format(workload, minutes))
    for i, t in enumerate(thread_num):
      ch = 'o'
      if test_results[i] is True:
        ch = 'v'
      else:
        ch = 'x'
      print("[Thread: {}][Try count: {}][Result: {}]".format(t, tried_num[i], ch))

if __name__ == "__main__":
  sysname = sys.argv[1]
  workload = sys.argv[2]
  if sysname == "crest":
    run_crest_scalability(workload)
