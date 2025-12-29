import shutil
from node import Node
import time
import threading
import os
import config
import numpy as np
from scp import SCPClient
from typing import List, Tuple


class CrestMemoryNode(Node):
    def __init__(self, ip: str, id: int):
        super().__init__(ip, id)
        self.project_path = config.projectPath

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

    def run(self, workload: str, thread_num: int, coro_num: int) -> bool:
        binpath = self.project_path + "/build/benchmark/"
        cmd = (
            "cd {} && nohup ./bench_runner --type=mn --id={} ".format(binpath, self.id)
            + "--config=../../config/{}_config.json --workload={} ".format(
                workload, workload
            )
            + "--threads={} --coro={} --replay=true > output 2>&1".format(
                thread_num, coro_num
            )
        )
        stdin, stdout, stderr = self.ssh.exec_command(cmd, get_pty=True)
        return True

    def shutdown(self):
        self.exec_cmd("killall bench_runner")
        self.exec_cmd("killall zm_mem_pool")
        self.exec_cmd("killall motor_mempool")
        self.exec_cmd("killall run")


class CrestComputeNode(Node):
    def __init__(self, ip: str, id: int):
        super().__init__(ip, id)
        self.project_path = config.projectPath
        self.bin_path = self.project_path + "/build/benchmark/"
        if id == 0:
            self.numa_node = 0
        else:
            self.numa_node = 1

    def exec_cmd(self, cmd: str) -> bool:
        stdin, stdout, stderr = self.ssh.exec_command(cmd, get_pty=True)
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
                print(stdout_text)
            return True

    def run(self, workload: str, thread_num: int, coro_num: int) -> bool:
        txn_num = 0
        if workload == "tpcc":
            txn_num = 25000 * thread_num
        if workload == "smallbank":
            txn_num = 250000 * thread_num
        if workload == "ycsb":
            txn_num = 250000 * thread_num
        print("Start running CREST Compute Node: ip = {}".format(self.ip))
        cmd = (
            "cd {} && ".format(self.bin_path)
            + "nohup numactl --cpunodebind={} --membind={} ".format(
                self.numa_node, self.numa_node
            )
            + "./bench_runner --type=cn --id={} ".format(self.id)
            + "--config=../../config/{}_config.json --workload={} ".format(
                workload, workload
            )
            + "--threads={} --coro={} ".format(thread_num, coro_num)
            + "--txn_num={} ".format(txn_num)
            + "--output=../../bench_results/{} ".format(workload)
            + "> output 2>&1"
        )
        t = self.exec_cmd(cmd)
        if t is False:
            self.shutdown()
        return t

    def shutdown(self):
        self.exec_cmd("killall bench_runner")

    def get_results(self, workload: str, dst_dir: str):
        # gather the results from this compute node
        src_path = self.project_path + "/bench_results/{}/results.txt".format(workload)
        dst_path = dst_dir + "/crest_{}_results_cn{}.txt".format(workload, self.id)
        with SCPClient(self.ssh.get_transport()) as scp:
            scp.get(src_path, dst_path)

        # Get the latency data:
        src_path = self.project_path + "/bench_results/{}/details.txt".format(workload)
        dst_path = dst_dir + "/crest_{}_details_cn{}.txt".format(workload, self.id)
        with SCPClient(self.ssh.get_transport()) as scp:
            scp.get(src_path, dst_path)

    def rollback_last_result(self, workload: str):
        src_path = self.project_path + "/bench_results/{}/results.txt".format(workload)
        # Remove the last line of the file
        cmd = f'sed -i "$ d" {src_path}'
        self.exec_cmd(cmd)


class CrestBenchmark:
    def __init__(
        self, workload: str, cns: List[Tuple[str, int]], mns: List[Tuple[str, int]]
    ):
        self.workload = workload
        self.cns = [CrestComputeNode(ip, id) for ip, id in cns]
        self.mns = [CrestMemoryNode(ip, id) for ip, id in mns]

    def clean_bench_results(self):
        for n in self.cns:
            n.exec_cmd(
                "rm -rf {}/bench_results/{}".format(n.project_path, self.workload)
            )

    def build_project(self):
        return

    def setup_tpcc_configuration(self, num_warehouse: int):
        if self.workload == "tpcc":
            self.num_warehouse = num_warehouse
            for n in self.mns:
                config_file_path = n.project_path + "/config/tpcc_config.json"
                config_cmd = 'sed -i \'s/"num_warehouse": [0-9]*/"num_warehouse": {}/g\' {}/config/tpcc_config.json'.format(
                    num_warehouse, n.project_path
                )
                n.exec_cmd(config_cmd)

            for n in self.cns:
                config_file_path = n.project_path + "/config/tpcc_config.json"
                config_cmd = 'sed -i \'s/"num_warehouse": [0-9]*/"num_warehouse": {}/g\' {}/config/tpcc_config.json'.format(
                    num_warehouse, n.project_path
                )
                n.exec_cmd(config_cmd)

    def setup_smallbank_configuration(self, num_accounts: int, theta: float):
        if self.workload == "smallbank":
            self.num_accounts = num_accounts
            self.theta = theta

            for n in self.mns:
                config_cmd = 'sed -i \'s/"num_accounts": [0-9]*/"num_accounts": {}/g\' {}/config/smallbank_config.json'.format(
                    self.num_accounts, n.project_path
                )
                n.exec_cmd(config_cmd)

                config_cmd = 'sed -i \'s/"zipfian": [0-9]*\\(\\.[0-9]*\\)*/"zipfian": {}/g\' {}/config/smallbank_config.json'.format(
                    self.theta, n.project_path
                )
                n.exec_cmd(config_cmd)

            for n in self.cns:
                config_cmd = 'sed -i \'s/"num_accounts": [0-9]*/"num_accounts": {}/g\' {}/config/smallbank_config.json'.format(
                    self.num_accounts, n.project_path
                )
                n.exec_cmd(config_cmd)

                config_cmd = 'sed -i \'s/"zipfian": [0-9]*\\(\\.[0-9]*\\)*/"zipfian": {}/g\' {}/config/smallbank_config.json'.format(
                    self.theta, n.project_path
                )
                n.exec_cmd(config_cmd)

    def setup_ycsb_configuration(
        self, key_num: int, theta: float, op_num: int, write_ratio: int = 100
    ):
        self.num_key = key_num
        self.theta = theta
        self.num_op = op_num
        self.write_ratio = write_ratio

        for n in self.mns:
            config_cmd = 'sed -i \'s/"num_records": [0-9]*/"num_records": {}/g\' {}/config/ycsb_config.json'.format(
                self.num_key, n.project_path
            )
            n.exec_cmd(config_cmd)

            config_cmd = 'sed -i \'s/"zipfian": [0-9]*\\(\\.[0-9]*\\)*/"zipfian": {}/g\' {}/config/ycsb_config.json'.format(
                self.theta, n.project_path
            )
            n.exec_cmd(config_cmd)

            config_cmd = 'sed -i \'s/"num_op": [0-9]*/"num_op": {}/g\' {}/config/ycsb_config.json'.format(
                self.num_op, n.project_path
            )
            n.exec_cmd(config_cmd)

        for n in self.cns:
            config_cmd = 'sed -i \'s/"num_records": [0-9]*/"num_records": {}/g\' {}/config/ycsb_config.json'.format(
                self.num_key, n.project_path
            )
            n.exec_cmd(config_cmd)

            config_cmd = 'sed -i \'s/"zipfian": [0-9]*\\(\\.[0-9]*\\)*/"zipfian": {}/g\' {}/config/ycsb_config.json'.format(
                self.theta, n.project_path
            )
            n.exec_cmd(config_cmd)

            config_cmd = 'sed -i \'s/"num_op": [0-9]*/"num_op": {}/g\' {}/config/ycsb_config.json'.format(
                self.num_op, n.project_path
            )
            n.exec_cmd(config_cmd)

            config_cmd = 'sed -i \'s/"write_ratio": [0-9]*/"write_ratio": {}/g\' {}/config/ycsb_config.json'.format(
                self.write_ratio, n.project_path
            )
            n.exec_cmd(config_cmd)

    def print_run_header(self, thread_num: int, coro_num: int):
        if self.workload == "tpcc":
            print(
                "[=] Run CREST TPCC Benchmark: {} Threads, {} Coros".format(
                    thread_num, coro_num
                )
            )
        if self.workload == "smallbank":
            print(
                "[=] Run CREST SmallBank Benchmark: {} Threads, {} Coros".format(
                    thread_num, coro_num
                )
            )
        if self.workload == "ycsb":
            print(
                "[=] Run CREST YCSB Benchmark: {} Threads, {} Coros".format(
                    thread_num, coro_num
                )
            )

    def launch_memory_pool(self, thread_num: int, coro_num: int):
        print("Launching Memory Nodes ...")
        for n in self.mns:
            n.run(self.workload, thread_num, coro_num)
        print("Launching Memory Done")

    def launch_compute_pool(self, thread_num: int, coro_num: int):
        print("Launching Compute Nodes ...")
        exec_results = [False] * len(self.cns)

        def spawn_run(n: CrestComputeNode):
            t = n.run(self.workload, thread_num, coro_num)
            exec_results[n.id] = t

        threads = []
        for n in self.cns:
            t = threading.Thread(target=spawn_run, args=(n,))
            threads.append(t)
            t.start()

        print("Launching Compute Nodes Done")

        # Wait for both threads to complete
        for thread in threads:
            thread.join()

        # Close the memory pool
        for n in self.mns:
            n.shutdown()

        for n in self.cns:
            n.shutdown()

        # Check if all threads are successful
        all_succ = True
        for r in exec_results:
            all_succ = all_succ and r

        res = ""
        if self.workload == "tpcc":
            res = (
                "CREST {} Benchmark: Warehouse[{}], Threads[{}], Coroutines[{}]".format(
                    self.workload, self.num_warehouse, thread_num, coro_num
                )
            )
        if self.workload == "smallbank":
            res = "CREST {} Benchmark: Accounts[{}], Zipfian[{}], Threads[{}], Coroutines[{}]".format(
                self.workload, self.num_accounts, self.theta, thread_num, coro_num
            )
        if self.workload == "ycsb":
            res = "CREST {} Benchmark: NumKeys[{}], Zipfian[{}], Threads[{}], Coroutines[{}]".format(
                self.workload, self.num_key, self.theta, thread_num, coro_num
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
                    self.cns[i].rollback_last_result(self.workload)
                else:
                    print("CN{} failed execution".format(i))
            print("<<< Rollback execution results of SUCC nodes")
            time.sleep(config.roundSep)
            return False

    def run_once(self, thread_num: int, coro_num: int, load_time: int):
        self.print_run_header(thread_num, coro_num)

        # Create the detail directory first:
        self.create_detail_dir(thread_num, coro_num)

        # Launch the memory pool
        self.launch_memory_pool(thread_num, coro_num)

        time.sleep(load_time)

        return self.launch_compute_pool(thread_num, coro_num)

    def create_detail_dir(self, thread_num: int, coro_num: int):
        if self.workload == "tpcc":
            self.detail_dir = (
                "crest_tpcc_detail_warehouse_{}_threads_{}_coro_{}".format(
                    self.num_warehouse, thread_num, coro_num
                )
            )
        if self.workload == "smallbank":
            self.detail_dir = "crest_smallbank_detail_accounts_{}_zipfian_{}_threads_{}_coro_{}".format(
                self.num_accounts, self.theta, thread_num, coro_num
            )
        if self.workload == "ycsb":
            self.detail_dir = (
                "crest_ycsb_detail_keys_{}_zipfian_{}_ops_{}_threads_{}_coro_{}".format(
                    self.num_key, self.theta, self.num_op, thread_num, coro_num
                )
            )
        if os.path.exists(self.detail_dir):
            shutil.rmtree(self.detail_dir)
        os.makedirs(self.detail_dir)

    def get_results(self):
        for n in self.cns:
            n.get_results(self.workload, ".")

    def process_results(self):
        all_data = []
        # 1. Process the commit throughput and tried throughput:
        for n in self.cns:
            fname = "crest_{}_results_cn{}.txt".format(self.workload, n.id)
            with open(fname, "r") as f:
                lines = f.readlines()
                file_data = []
                for line in lines:
                    res = np.array([float(x) for x in line.split()[1:]])
                    res[2:] /= len(self.cns)
                    file_data.append(res)
                all_data.append(file_data)
                f.close()
        all_data = np.array(all_data)
        all_data = all_data.sum(axis=0)
        # Store the result data into a file
        with open("crest_{}_aggregated_thpt".format(self.workload), "w") as f:
            column_headers = [
                "tried_thpt(KOPS)",
                "committed_thpt",
                "avg_latency(us)",
                "p50_latency",
                "p90_latency",
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
