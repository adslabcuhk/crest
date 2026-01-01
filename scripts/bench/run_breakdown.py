import crest
import motor
import ford
import time
import sys
import config

# TPC-C
# NUM_WAREHOUSE:
# High Contention, and Low Contention
tpcc_contention_level = [100, 40]

# SmallBank
# High Contention, and Low Contention
NUM_ACCOUNTS = 100000
smallbank_contention_level = [0.5, 0.99]

# YCSB
NUM_RECORDS = 1000000
NUM_OPS = 4
# High Contention, and Low Contention
ycsb_contention_level = [0.5, 0.99]


def run_ford_contentionlevel(workload: str):
    bench = ford.FordBenchmark(workload, config.cns, config.mns)
    wait_load_time = 0
    bench.build_project()
    bench.clean_bench_results()
    thread_num = 40
    max_try_cnt = 5

    tried_num = []
    test_results = []
    contention_param = []

    if workload == "smallbank":
        tried_num = [0] * len(smallbank_contention_level)
        test_results = [False] * len(smallbank_contention_level)
        contention_param = smallbank_contention_level
        wait_load_time = 30

    if workload == "micro":
        tried_num = [0] * len(ycsb_contention_level)
        test_results = [False] * len(ycsb_contention_level)
        contention_param = ycsb_contention_level
        wait_load_time = 40

    if workload == "tpcc":
        tried_num = [0] * len(tpcc_contention_level)
        test_results = [False] * len(tpcc_contention_level)
        contention_param = tpcc_contention_level
        wait_load_time = 150

    start_time = time.time()
    for i in range(len(contention_param)):
        if workload == "smallbank":
            bench.setup_smallbank_configuration(NUM_ACCOUNTS, contention_param[i])
        if workload == "micro":
            bench.setup_micro_configuration(NUM_RECORDS, contention_param[i], NUM_OPS)
        if workload == "tpcc":
            bench.setup_tpcc_configuration(tpcc_contention_level[i])

        res = bench.run_once(thread_num, 3, wait_load_time)
        try_cnt = 1
        while res is False and try_cnt < max_try_cnt:
            res = bench.run_once(thread_num, 3, wait_load_time)
            try_cnt += 1

        test_results[i] = res
        tried_num[i] = try_cnt

    bench.get_results()
    bench.process_results()
    end_time = time.time()

    minutes = (end_time - start_time) / 60
    # Summary:
    print(
        "[Ford {} Execution Results][Execution Time: {:.2f} Minutes]".format(
            workload, minutes
        )
    )
    for i, t in enumerate(contention_param):
        ch = "o"
        if test_results[i] is True:
            ch = "v"
        else:
            ch = "x"
        print("[Contention: {}][Try count: {}][Result: {}]".format(t, tried_num[i], ch))


def run_motor_contentionlevel(workload: str):
    bench = motor.MotorBenchmark(workload, config.cns, config.mns)
    wait_load_time = 0
    bench.build_project()
    bench.clean_bench_results()
    thread_num = 40
    max_try_cnt = 5

    tried_num = []
    test_results = []
    contention_param = []

    if workload == "smallbank":
        tried_num = [0] * len(smallbank_contention_level)
        test_results = [False] * len(smallbank_contention_level)
        contention_param = smallbank_contention_level
        wait_load_time = 30

    if workload == "micro":
        tried_num = [0] * len(ycsb_contention_level)
        test_results = [False] * len(ycsb_contention_level)
        contention_param = ycsb_contention_level
        wait_load_time = 40

    if workload == "tpcc":
        tried_num = [0] * len(tpcc_contention_level)
        test_results = [False] * len(tpcc_contention_level)
        contention_param = tpcc_contention_level
        wait_load_time = 250

    start_time = time.time()
    for i in range(len(contention_param)):
        if workload == "smallbank":
            bench.setup_smallbank_configuration(NUM_ACCOUNTS, contention_param[i])
        if workload == "micro":
            bench.setup_micro_configuration(NUM_RECORDS, contention_param[i], NUM_OPS)
        if workload == "tpcc":
            bench.setup_tpcc_configuration(tpcc_contention_level[i])

        res = bench.run_once(thread_num, 3, wait_load_time)
        try_cnt = 1
        while res is False and try_cnt < max_try_cnt:
            res = bench.run_once(thread_num, 3, wait_load_time)
            try_cnt += 1

        test_results[i] = res
        tried_num[i] = try_cnt

    bench.get_results()
    bench.process_results()
    end_time = time.time()

    minutes = (end_time - start_time) / 60
    # Summary:
    print(
        "[Motor {} Execution Results][Execution Time: {:.2f} Minutes]".format(
            workload, minutes
        )
    )
    for i, t in enumerate(contention_param):
        ch = "o"
        if test_results[i] is True:
            ch = "v"
        else:
            ch = "x"
        print("[Contention: {}][Try count: {}][Result: {}]".format(t, tried_num[i], ch))


def run_crest_contentionlevel(workload: str):
    bench = crest.CrestBenchmark(workload, config.cns, config.mns)
    wait_load_time = 0
    bench.build_project()
    bench.clean_bench_results()
    thread_num = 40
    max_try_cnt = 5

    tried_num = []
    test_results = []
    contention_param = []

    if workload == "smallbank":
        tried_num = [0] * len(smallbank_contention_level)
        test_results = [False] * len(smallbank_contention_level)
        contention_param = smallbank_contention_level
        wait_load_time = 60

    if workload == "ycsb":
        tried_num = [0] * len(ycsb_contention_level)
        test_results = [False] * len(ycsb_contention_level)
        contention_param = ycsb_contention_level
        wait_load_time = 80

    if workload == "tpcc":
        tried_num = [0] * len(tpcc_contention_level)
        test_results = [False] * len(tpcc_contention_level)
        contention_param = tpcc_contention_level
        wait_load_time = 240

    start_time = time.time()
    for i in range(len(contention_param)):
        if workload == "smallbank":
            bench.setup_smallbank_configuration(NUM_ACCOUNTS, contention_param[i])
        if workload == "ycsb":
            bench.setup_ycsb_configuration(NUM_RECORDS, contention_param[i], NUM_OPS)
        if workload == "tpcc":
            bench.setup_tpcc_configuration(tpcc_contention_level[i])

        res = bench.run_once(thread_num, 3, wait_load_time)
        try_cnt = 1
        while res is False and try_cnt < max_try_cnt:
            res = bench.run_once(thread_num, 3, wait_load_time)
            try_cnt += 1

        test_results[i] = res
        tried_num[i] = try_cnt

    bench.get_results()
    bench.process_results()
    end_time = time.time()

    minutes = (end_time - start_time) / 60
    # Summary:
    print(
        "[Crest {} Execution Results][Execution Time: {:.2f} Minutes]".format(
            workload, minutes
        )
    )
    for i, t in enumerate(contention_param):
        ch = "o"
        if test_results[i] is True:
            ch = "v"
        else:
            ch = "x"
        print("[Contention: {}][Try count: {}][Result: {}]".format(t, tried_num[i], ch))


if __name__ == "__main__":
    sysname = sys.argv[1]
    workload = sys.argv[2]
    if sysname in ["ford", "motor"] and workload == "ycsb":
        workload = "micro"
    if sysname == "ford":
        run_ford_contentionlevel(workload)
    elif sysname == "motor":
        run_motor_contentionlevel(workload)
    elif sysname == "crest":
        run_crest_contentionlevel(workload)
