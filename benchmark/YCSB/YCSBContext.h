#pragma once

#include "Generator.h"
#include "YCSB/YCSBConstant.h"
#include "db/PoolHashIndex.h"
#include "util/JsonConfig.h"
#include "util/Macros.h"

struct YCSBConfig {
    int num_records;
    double zipfian;
    int num_op;
    int write_ratio;
    PoolHashIndex::Config ycsb_index_conf;
};

ALWAYS_INLINE
YCSBConfig ParseYCSBConfig(const std::string& fname) {
    auto json_config = JsonConfig::load_file(fname);
    auto table_config = json_config.get("ycsb");
    YCSBConfig config;

    // The YCSB records can be directly indexed by its id:
    config.num_records = table_config.get("num_records").get_uint64();
    config.zipfian = table_config.get("zipfian").get_double();
    config.num_op = table_config.get("num_op").get_uint64();
    config.write_ratio = table_config.get("write_ratio").get_uint64();

    config.ycsb_index_conf.bucket_num_ = config.num_records;
    config.ycsb_index_conf.records_per_bucket_ = 1;
    config.ycsb_index_conf.hash_type_ = kDirect;

    return config;
}

struct YCSBContext {
    YCSBContext(const YCSBConfig& config)
        : config(config), zipf(config.num_records, config.zipfian) {}

    int64_t GenerateRecordKey() { return zipf.GetNextNumber() % config.num_records; }

    YCSBConfig config;
    ZipfGenerator zipf;
};