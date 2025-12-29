import motor
import ford
import crest
import time
import sys
import config


# YCSB
NUM_RECORDS = 1000000
NUM_OPS = [1, 2, 3, 4, 1, 2, 3, 4]
# High Contention, and Low Contention
zipfian = 0.99


def run_ford_sensitivity(workload: str):
    bench = ford.FordBenchmark(workload, config.cns, config.mns)
    wait_load_time = 0
    bench.build_project()
    bench.clean_bench_results()
    thread_num = 40
    max_try_cnt = 5

    tried_num = []
    test_results = []
    op_param = []

    if workload == "micro":
        tried_num = [0] * len(NUM_OPS)
        test_results = [False] * len(NUM_OPS)
        op_param = NUM_OPS
        wait_load_time = 40

    start_time = time.time()
    for i in range(len(op_param)):
        zipfian = 0.99
        if i >= 4:
            zipfian = 0.1
        if workload == "micro":
            bench.setup_micro_configuration(NUM_RECORDS, zipfian, op_param[i])

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
    for i, t in enumerate(op_param):
        ch = "o"
        if test_results[i] is True:
            ch = "v"
        else:
            ch = "x"
        print("[OPS: {}][Try count: {}][Result: {}]".format(t, tried_num[i], ch))


def run_motor_sensitivity(workload: str):
    bench = motor.MotorBenchmark(workload, config.cns, config.mns)
    wait_load_time = 0
    bench.build_project()
    bench.clean_bench_results()
    thread_num = 40
    max_try_cnt = 5

    tried_num = []
    test_results = []
    op_param = []

    if workload == "micro":
        tried_num = [0] * len(NUM_OPS)
        test_results = [False] * len(NUM_OPS)
        op_param = NUM_OPS
        wait_load_time = 40

    start_time = time.time()
    for i in range(len(op_param)):
        zipfian = 0.99
        if i >= 4:
            zipfian = 0.1
        if workload == "micro":
            bench.setup_micro_configuration(NUM_RECORDS, zipfian, op_param[i])

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
    for i, t in enumerate(op_param):
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
    op_param = []

    if workload == "ycsb":
        tried_num = [0] * len(NUM_OPS)
        test_results = [False] * len(NUM_OPS)
        op_param = NUM_OPS
        wait_load_time = 40

    start_time = time.time()
    for i in range(len(op_param)):
        zipfian = 0.99
        if i >= 4:
            zipfian = 0.1
        if workload == "ycsb":
            bench.setup_ycsb_configuration(NUM_RECORDS, zipfian, op_param[i])

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
    for i, t in enumerate(op_param):
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
        run_ford_sensitivity(workload)
    elif sysname == "motor":
        run_motor_sensitivity(workload)
    elif sysname == "crest":
        run_crest_contentionlevel(workload)
