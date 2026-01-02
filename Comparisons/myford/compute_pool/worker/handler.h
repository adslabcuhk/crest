// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

#include "util/Memcached.h"

class Handler {
   public:
    Handler();
    // For macro-benchmark
    void ConfigureComputeNode(int argc, char* argv[]);
    void GenThreads(std::string bench_name);
    void OutputResult(std::string bench_name, std::string system_name);

    // For micro-benchmark
    void ConfigureComputeNodeForMICRO(int argc, char* argv[]);
    void GenThreadsForMICRO();

    MemcachedWrapper* memcached_wrapper;
};
