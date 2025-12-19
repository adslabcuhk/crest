import crest
import time
import sys
import config

# YCSB
NUM_RECORDS = 1000000
WRITE_RATIO = [100, 75, 50, 25, 0]
NUM_OPS = 4
# High Contention, and Low Contention
zipfian = 0.99


def run_crest_write_ratio(workload: str):
    bench = crest.CrestBenchmark(workload, config.cns, config.mns)
    wait_load_time = 0
    bench.build_project()
    bench.clean_bench_results()
    thread_num = 40
    max_try_cnt = 5

    tried_num = []
    test_results = []

    if workload == "ycsb":
        tried_num = [0] * len(WRITE_RATIO)
        test_results = [False] * len(WRITE_RATIO)
        wait_load_time = 80

    start_time = time.time()
    for i in range(len(WRITE_RATIO)):
        zipfian = 0.1
        if workload == "ycsb":
            bench.setup_ycsb_configuration(
                NUM_RECORDS, zipfian, NUM_OPS, WRITE_RATIO[i]
            )

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
    for i, t in enumerate(WRITE_RATIO):
        ch = "o"
        if test_results[i] is True:
            ch = "v"
        else:
            ch = "x"
        print("[Contention: {}][Try count: {}][Result: {}]".format(t, tried_num[i], ch))


if __name__ == "__main__":
    sysname = sys.argv[1]
    workload = sys.argv[2]
    if sysname == "crest":
        run_crest_write_ratio(workload)
