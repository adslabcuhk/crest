#pragma once

#include "Generator.h"
#include "SmallBank/SmallBankConstant.h"
#include "common/Type.h"
#include "db/PoolHashIndex.h"
#include "util/JsonConfig.h"
#include "util/Macros.h"

struct SmallBankConfig {
    int num_accounts;
    int num_hot_accounts;
    int hot_accounts_percent;
    double zipfian;

    PoolHashIndex::Config accounts_index_conf;
    PoolHashIndex::Config savings_index_conf;
    PoolHashIndex::Config checkings_index_conf;
};

ALWAYS_INLINE
SmallBankConfig ParseSmallBankConfig(const std::string& fname) {
    auto json_config = JsonConfig::load_file(fname);
    auto table_config = json_config.get("smallbank");
    SmallBankConfig config;

    // The Warehouse records can be directly indexed by its id
    config.num_accounts = table_config.get("num_accounts").get_uint64();
    config.num_hot_accounts = table_config.get("num_hot_accounts").get_uint64();
    config.zipfian = table_config.get("zipfian").get_double();

    config.accounts_index_conf.bucket_num_ = config.num_accounts;
    config.accounts_index_conf.records_per_bucket_ = 1;
    config.accounts_index_conf.hash_type_ = kDirect;

    config.savings_index_conf.bucket_num_ = config.num_accounts;
    config.savings_index_conf.records_per_bucket_ = 1;
    config.savings_index_conf.hash_type_ = kDirect;

    config.checkings_index_conf.bucket_num_ = config.num_accounts;
    config.checkings_index_conf.records_per_bucket_ = 1;
    config.checkings_index_conf.hash_type_ = kDirect;

    return config;
}

struct SmallBankContext {
    SmallBankContext(const SmallBankConfig& config)
        : config(config), zipf(config.num_accounts, config.zipfian) {}

    int64_t GenerateAccountsId() { return zipf.GetNextNumber() % config.num_accounts; }

    ZipfGenerator zipf;
    SmallBankConfig config;
};