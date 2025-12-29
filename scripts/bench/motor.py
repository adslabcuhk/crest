import shutil
from node import Node
import time
import config
import threading
import os
import numpy as np
from scp import SCPClient
from typing import List, Tuple


class MotorMemoryNode(Node):
    def __init__(self, ip: str, id: int):
        super().__init__(ip, id)
        self.project_path = config.motorPath

    def exec_cmd(self, cmd: str) -> bool:
        stdin, stdout, stderr = self.ssh.exec_command(cmd, timeout=300, get_pty=True)
        # Execute this command with a timeout of 60 seconds
        timeout = config.execCmdTimeout
        endtime = time.time() + timeout
        long_running = False
        while not stdout.channel.exit_status_ready():
            time.sleep(1)
            if time.time() > endtime:
                stdout.channel.close()
                long_running = True
                break

        if long_running:
            print("The command is running too long..., kill it")
            return False

        print("Command: {} exits".format(cmd))
        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            stderr_text = stderr.read().decode("utf-8")
            stdout_text = stdout.read().decode("utf-8")
            print("Error Message: {}, {}\n".format(stderr_text, stdout_text))
            return False
        else:
            stdout_text = stdout.read().decode("utf-8")
            if stdout_text != "" and stdout_text != "\n":
                # print("Output Message: \n", stdout_text)
                print("Execute OK")
            return True

    # Start up this memory node
    def run(self, workload: str) -> bool:
        binpath = self.project_path + "/build/memory_node/server/"
        # Use nohup to ensure this program is still running after ssh client is shutdown
        cmd = "cd {} && nohup ./motor_mempool > /dev/null 2>&1".format(binpath)
        print(cmd)
        stdin, stdout, stderr = self.ssh.exec_command(cmd, get_pty=True)
        return True

    # Kill this memory node
    def shutdown(self):
        self.exec_cmd("killall motor_mempool")


class MotorComputeNode(Node):
    def __init__(self, ip: str, id: int):
        super().__init__(ip, id)
        self.project_path = config.motorPath
        self.bin_path = self.project_path + "/build/compute_node/run/"
        if id == 0:
            self.numa_node = 0
        else:
            self.numa_node = 1

    def exec_cmd(self, cmd: str) -> bool:
        stdin, stdout, stderr = self.ssh.exec_command(cmd, timeout=300, get_pty=True)
        # Execute this command with a timeout of 60 seconds
        timeout = config.execCmdTimeout
        endtime = time.time() + timeout
        long_running = False
        while not stdout.channel.exit_status_ready():
            time.sleep(1)
            if time.time() > endtime:
                stdout.channel.close()
                long_running = True
                break

        if long_running:
            print("The command is running too long..., kill it")
            return False

        print("Command: {} exits".format(cmd))
        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            stderr_text = stderr.read().decode("utf-8")
            stdout_text = stdout.read().decode("utf-8")
            print("Error Message: {}, {}\n".format(stderr_text, stdout_text))
            return False
        else:
            stdout_text = stdout.read().decode("utf-8")
            if stdout_text != "" and stdout_text != "\n":
                # print("Output Message: \n", stdout_text)
                print("Execute OK")
            return True

    def run(self, workload: str, threads: int, coro: int) -> bool:
        # Spwan the compute node with the command
        print("Start running Motor Compute Node: ip = {}".format(self.ip))
        cmd = "cd {} && numactl --membind={} --cpunodebind={} ./run {} {} {} SR > output 2>&1".format(
            self.bin_path, self.numa_node, self.numa_node, workload, threads, coro
        )
        print(cmd)
        return self.exec_cmd(cmd)

    # Kill this compute node
    def shutdown(self):
        self.exec_cmd("killall run")

    def get_results(self, workload: str, dst_dir: str):
        # Gather the results from the compute node
        src_path = self.project_path + "/bench_results/{}/result.txt".format(workload)
        dst_path = dst_dir + "/motor_{}_results_cn{}.txt".format(workload, self.id)
        with SCPClient(self.ssh.get_transport()) as scp:
            scp.get(src_path, dst_path)

    def rollback_last_result(self, workload: str):
        src_path = self.project_path + "/bench_results/{}/result.txt".format(workload)
        # Remove the last line of the file
        cmd = f'sed -i "$ d" {src_path}'
        self.exec_cmd(cmd)

    def get_detail(self, dst_dir: str):
        src_path = self.project_path + "/event_count.yml"
        dst_path = dst_dir + "/event_count_cn{}.yml".format(self.id)
        with SCPClient(self.ssh.get_transport()) as scp:
            scp.get(src_path, dst_path)

        src_path = self.project_path + "/latency.txt"
        dst_path = dst_dir + "/latency_cn{}.txt".format(self.id)
        with SCPClient(self.ssh.get_transport()) as scp:
            scp.get(src_path, dst_path)


class MotorBenchmark:
    def __init__(
        self, workload: str, cns: List[Tuple[str, int]], mns: List[Tuple[str, int]]
    ):
        self.workload = workload

        # Create the nodes in memory pool and compute pool
        self.memory_pool = []
        for n in mns:
            self.memory_pool.append(MotorMemoryNode(n[0], n[1]))

        self.compute_pool = []
        for n in cns:
            self.compute_pool.append(MotorComputeNode(n[0], n[1]))

    def clean_bench_results(self):
        for n in self.compute_pool:
            n.exec_cmd(
                "rm -rf {}/bench_results/{}".format(n.project_path, self.workload)
            )

    def build_project(self):
        # Configure both compute pool and memory pool
        for n in self.compute_pool + self.memory_pool:
            reset_command = r"sed -i 's/\(#define WORKLOAD_[A-Z]\+\) 1/\1 0/g' {}/txn/flags.h".format(
                n.project_path
            )
            n.exec_cmd(reset_command)
            if self.workload == "tpcc":
                config_cmd = r"sed -i 's/#define WORKLOAD_TPCC 0/#define WORKLOAD_TPCC 1/' {}/txn/flags.h".format(
                    n.project_path
                )
                n.exec_cmd(config_cmd)
            if self.workload == "smallbank":
                config_cmd = r"sed -i 's/#define WORKLOAD_SMALLBANK 0/#define WORKLOAD_SMALLBANK 1/' {}/txn/flags.h".format(
                    n.project_path
                )
                n.exec_cmd(config_cmd)
            if self.workload == "micro":
                config_cmd = r"sed -i 's/#define WORKLOAD_MICRO 0/#define WORKLOAD_MICRO 1/' {}/txn/flags.h".format(
                    n.project_path
                )
                n.exec_cmd(config_cmd)
            build_cmd = "cd {}; make -j".format(n.project_path)
            n.exec_cmd(build_cmd)

    # Setup warehouse number for TPC-C:
    def setup_tpcc_configuration(self, w: int):
        if self.workload == "tpcc":
            self.num_warehouse = w
            for n in self.memory_pool:
                config_cmd = 'sed -i \'s/"num_warehouse": [0-9]*/"num_warehouse": {}/g\' {}/config/tpcc_config.json'.format(
                    w, n.project_path
                )
                n.exec_cmd(config_cmd)

                config_cmd = 'sed -i \'s/"warehouse_bkt_num": [0-9]*/"warehouse_bkt_num": {}/g\' {}/config/tpcc_config.json'.format(
                    w + 8, n.project_path
                )
                n.exec_cmd(config_cmd)

                config_file_path = n.project_path + "/" + "config/mn_config.json"
                config_cmd = (
                    'sed -i \'s/"workload": "[^"]*"/"workload": "TPCC"/\' {}'.format(
                        config_file_path
                    )
                )
                n.exec_cmd(config_cmd)

                # Change the memory size of the memory node
                # 120 (warehouse) -> 170GiB
                mr_size = int(160)
                config_cmd = (
                    'sed -i \'s/"reserve_GB": [0-9]*/"reserve_GB": {}/g\' {}'.format(
                        mr_size, config_file_path
                    )
                )
                n.exec_cmd(config_cmd)

            for n in self.compute_pool:
                config_cmd = 'sed -i \'s/"num_warehouse": [0-9]*/"num_warehouse": {}/g\' {}/config/tpcc_config.json'.format(
                    w, n.project_path
                )
                n.exec_cmd(config_cmd)

                config_cmd = 'sed -i \'s/"warehouse_bkt_num": [0-9]*/"warehouse_bkt_num": {}/g\' {}/config/tpcc_config.json'.format(
                    w + 8, n.project_path
                )
                n.exec_cmd(config_cmd)

    def setup_smallbank_configuration(self, num_accounts: int, theta: float):
        if self.workload == "smallbank":
            self.num_accounts = num_accounts
            self.theta = theta
            # Change the configuration file for both memory pool and compute pool
            # Setup their warehouse number
            for n in self.memory_pool:
                config_cmd = 'sed -i \'s/"num_accounts": [0-9]*/"num_accounts": {}/g\' {}/config/smallbank_config.json'.format(
                    self.num_accounts, n.project_path
                )
                n.exec_cmd(config_cmd)

                config_cmd = 'sed -i \'s/"zipfian": [0-9]*\\(\\.[0-9]*\\)*/"zipfian": {}/g\' {}/config/smallbank_config.json'.format(
                    self.theta, n.project_path
                )
                n.exec_cmd(config_cmd)

                config_file_path = n.project_path + "/" + "config/mn_config.json"
                config_cmd = 'sed -i \'s/"workload": "[^"]*"/"workload": "SmallBank"/\' {}'.format(
                    config_file_path
                )
                n.exec_cmd(config_cmd)

                # Change the memory size of the memory node
                mr_size = 24
                config_cmd = (
                    'sed -i \'s/"reserve_GB": [0-9]*/"reserve_GB": {}/g\' {}'.format(
                        mr_size, config_file_path
                    )
                )
                n.exec_cmd(config_cmd)

            for n in self.compute_pool:
                config_cmd = 'sed -i \'s/"num_accounts": [0-9]*/"num_accounts": {}/g\' {}/config/smallbank_config.json'.format(
                    self.num_accounts, n.project_path
                )
                n.exec_cmd(config_cmd)

                config_cmd = 'sed -i \'s/"zipfian": [0-9]*\\(\\.[0-9]*\\)*/"zipfian": {}/g\' {}/config/smallbank_config.json'.format(
                    self.theta, n.project_path
                )
                n.exec_cmd(config_cmd)

    def setup_micro_configuration(
        self, num_keys: int, theta: float, num_op: int, write_raio: int = 100
    ):
        if self.workload == "micro":
            self.num_keys = num_keys
            self.theta = theta
            self.num_op = num_op
            self.write_ratio = write_raio
            # Change the configuration file for both memory pool and compute pool
            # Setup their warehouse number
            for n in self.memory_pool:
                config_cmd = 'sed -i \'s/"num_keys": [0-9]*/"num_keys": {}/g\' {}/config/micro_config.json'.format(
                    self.num_keys, n.project_path
                )
                n.exec_cmd(config_cmd)

                config_cmd = 'sed -i \'s/"zipfian": [0-9]*\\(\\.[0-9]*\\)*/"zipfian": {}/g\' {}/config/micro_config.json'.format(
                    self.theta, n.project_path
                )
                n.exec_cmd(config_cmd)

                config_cmd = 'sed -i \'s/"num_op": [0-9]*/"num_op": {}/g\' {}/config/micro_config.json'.format(
                    self.num_op, n.project_path
                )
                n.exec_cmd(config_cmd)

                config_file_path = n.project_path + "/" + "config/mn_config.json"
                config_cmd = (
                    'sed -i \'s/"workload": "[^"]*"/"workload": "MICRO"/\' {}'.format(
                        config_file_path
                    )
                )
                n.exec_cmd(config_cmd)

                # Change the memory size of the memory node
                mr_size = 24
                config_cmd = (
                    'sed -i \'s/"reserve_GB": [0-9]*/"reserve_GB": {}/g\' {}'.format(
                        mr_size, config_file_path
                    )
                )
                n.exec_cmd(config_cmd)

            for n in self.compute_pool:
                config_cmd = 'sed -i \'s/"num_keys": [0-9]*/"num_keys": {}/g\' {}/config/micro_config.json'.format(
                    self.num_keys, n.project_path
                )
                n.exec_cmd(config_cmd)

                config_cmd = 'sed -i \'s/"zipfian": [0-9]*\\(\\.[0-9]*\\)*/"zipfian": {}/g\' {}/config/micro_config.json'.format(
                    self.theta, n.project_path
                )
                n.exec_cmd(config_cmd)

                config_cmd = 'sed -i \'s/"num_op": [0-9]*/"num_op": {}/g\' {}/config/micro_config.json'.format(
                    self.num_op, n.project_path
                )
                n.exec_cmd(config_cmd)

                config_cmd = 'sed -i \'s/"write_ratio": [0-9]*/"write_ratio": {}/g\' {}/config/micro_config.json'.format(
                    self.write_ratio, n.project_path
                )
                n.exec_cmd(config_cmd)

    def print_run_header(self, thread_num: int, coro_num: int):
        if self.workload == "tpcc":
            print(
                "[=] Run Motor TPCC Benchmark: {} Threads, {} Coros".format(
                    thread_num, coro_num
                )
            )
        if self.workload == "smallbank":
            print(
                "[=] Run Motor SmallBank Benchmark: {} Threads, {} Coros".format(
                    thread_num, coro_num
                )
            )
        if self.workload == "micro":
            print(
                "[=] Run Motor Micro Benchmark: {} Threads, {} Coros".format(
                    thread_num, coro_num
                )
            )

    def launch_mempool(self):
        for n in self.memory_pool:
            n.run(self.workload)

    def launch_compute_pool(self, thread_num: int, coro_num: int):
        # Create threads to spawn processes on both servers simultaneously
        exec_results = [False] * len(self.compute_pool)

        def spawn_run(n: MotorComputeNode):
            t = n.run(self.workload, thread_num, coro_num)
            exec_results[n.id] = t

        threads = []
        timers = []
        TIMEOUT = config.execCmdTimeout
        for n in self.compute_pool:
            t = threading.Thread(target=spawn_run, args=(n,))
            timer = threading.Timer(
                TIMEOUT, t.join
            )  # Force thread completion after timeout
            threads.append(t)
            timers.append(timer)
            t.start()
            timer.start()

        # Wait for both threads to complete
        for thread, timer in zip(threads, timers):
            thread.join()
            timer.cancel()

        # Close the memory pool
        for n in self.memory_pool:
            n.shutdown()

        for n in self.compute_pool:
            n.shutdown()

        # Check if all threads are successful
        all_succ = True
        for r in exec_results:
            all_succ = all_succ and r

        # Get the log:
        for n in self.compute_pool:
            n.get_detail(self.detail_dir)

        res = ""
        if self.workload == "tpcc":
            res = (
                "Motor {} Benchmark: Warehouse[{}], Threads[{}], Coroutines[{}]".format(
                    self.workload, self.num_warehouse, thread_num, coro_num
                )
            )
        if self.workload == "smallbank":
            res = "Motor {} Benchmark: Accounts[{}], Zipfian[{}], Threads[{}], Coroutines[{}]".format(
                self.workload, self.num_accounts, self.theta, thread_num, coro_num
            )
        if self.workload == "micro":
            res = "Motor {} Benchmark: Keys[{}], Zipfian[{}], Operations[{}], Threads[{}], Coroutines[{}]".format(
                self.workload,
                self.num_keys,
                self.theta,
                self.num_op,
                thread_num,
                coro_num,
            )
        if all_succ:
            print("[v] {} SUCC".format(res))
            time.sleep(config.roundSep)
            return True
        else:
            print("[x] {} FAIL".format(res))
            # We need to delete the generated results of the succ servers:
            for i in range(len(exec_results)):
                if exec_results[i] is True:
                    self.compute_pool[i].rollback_last_result(self.workload)
                else:
                    print("CN{} failed execution".format(i))
            print("<<< Rollback execution results of SUCC nodes")
            time.sleep(config.roundSep)
            return False

    def run_once(self, thread_num: int, coro_num: int, load_time):
        self.print_run_header(thread_num, coro_num)

        # Create the detail directory first:
        self.create_detail_dir(thread_num, coro_num)

        # Launch memory pool:
        self.launch_mempool()

        # Wait for some time to load the data
        time.sleep(load_time)

        return self.launch_compute_pool(thread_num, coro_num)

    def create_detail_dir(self, thread_num: int, coro_num: int):
        if self.workload == "tpcc":
            self.detail_dir = (
                "motor_tpcc_detail_warehouse_{}_threads_{}_coro_{}".format(
                    self.num_warehouse, thread_num, coro_num
                )
            )
        if self.workload == "smallbank":
            self.detail_dir = "motor_smallbank_detail_accounts_{}_zipfian_{}_threads_{}_coro_{}".format(
                self.num_accounts, self.theta, thread_num, coro_num
            )
        if self.workload == "micro":
            self.detail_dir = "motor_micro_detail_keys_{}_zipfian_{}_ops_{}_wr_{}_threads_{}_coro_{}".format(
                self.num_keys,
                self.theta,
                self.num_op,
                self.write_ratio,
                thread_num,
                coro_num,
            )
        if os.path.exists(self.detail_dir):
            shutil.rmtree(self.detail_dir)
        os.makedirs(self.detail_dir)

    def get_results(self):
        # Bring the results back
        for n in self.compute_pool:
            n.get_results(self.workload, ".")

    def process_results(self):
        all_data = []
        # 1. Process the commit throughput and tried throughput:
        for n in self.compute_pool:
            fname = "motor_{}_results_cn{}.txt".format(self.workload, n.id)
            with open(fname, "r") as f:
                lines = f.readlines()
                file_data = []
                for line in lines:
                    # The first column is the system name, we won't need it
                    res = np.array([float(x) for x in line.split()[1:]])
                    res[2:] /= len(self.compute_pool)
                    file_data.append(res)
                all_data.append(file_data)
                f.close()
        all_data = np.array(all_data)
        all_data = all_data.sum(axis=0)
        # Store the result data into a file
        with open("motor_{}_aggregated_thpt".format(self.workload), "w") as f:
            column_headers = [
                "tried_thpt(KOPS)",
                "committed_thpt",
                "avg_latency(us)",
                "p50_latency",
                "p99_latency",
                "p999_latency",
                "avg_exec_latency",
                "avg_validate_latency",
                "avg_commit_latency",
            ]
            f.write(" ".join(column_headers) + "\n")
            for row in all_data:
                formatted_row = [
                    f"{x:.2f}" if isinstance(x, float) else str(x) for x in row
                ]
                f.write(" ".join(formatted_row) + "\n")
        # 1. Throughput aggregation is done

        # 2. Process the latency of each transaction type:
