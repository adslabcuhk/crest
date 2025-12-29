// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <cassert>
#include <vector>

#include "memstore/hash_store.h"
#include "micro/micro_table.h"
#include "util/fast_random.h"
#include "util/json_config.h"

class MICRO {
   public:
    std::string bench_name;

    uint64_t num_keys_global;

    double zipfian;

    uint64_t num_op;

    uint32_t coro_num;

    /* Tables */
    // HashStore *micro_table;

    HashStore *micro_table0;

    HashStore *micro_table1;

    std::vector<HashStore *> primary_table_ptrs;

    std::vector<HashStore *> backup_table_ptrs;

    std::vector<MicroTxType> *txn_run_types;

    std::vector<void *> *txn_run_params;

    int write_ratio = 100;

    int read_ratio = 0;

    // For server usage: Provide interfaces to servers for loading tables
    // Also for client usage: Provide interfaces to clients for generating ids
    // during tests
    MICRO() {
        bench_name = "MICRO";
        std::string config_filepath = "../../../config/micro_config.json";
        auto json_config = JsonConfig::load_file(config_filepath);
        auto conf = json_config.get("micro");
        num_keys_global = conf.get("num_keys").get_int64();
        zipfian = conf.get("zipfian").get_double();
        num_op = std::min(conf.get("num_op").get_uint64(), MICRO_OP_RECORD_MAX_NUM);
        this->write_ratio = conf.get("write_ratio").get_uint64();
        this->read_ratio = 100 - this->write_ratio;
        micro_table0 = nullptr;
        micro_table1 = nullptr;
    }

    ~MICRO() {
        if (micro_table0) delete micro_table0;
        if (micro_table1) delete micro_table1;
    }

    void LoadTable(node_id_t node_id, node_id_t num_server,
                   MemStoreAllocParam *mem_store_alloc_param, size_t &total_size,
                   size_t &ht_loadfv_size, size_t &ht_size, size_t &initfv_size,
                   size_t &real_cvt_size);

    void PopulateMicroTable(int table_id);

    void LoadRecord(HashStore *table, itemkey_t item_key, void *val_ptr, size_t val_size,
                    table_id_t table_id);

    ALWAYS_INLINE
    std::vector<HashStore *> GetPrimaryHashStore() { return primary_table_ptrs; }

    ALWAYS_INLINE
    std::vector<HashStore *> GetBackupHashStore() { return backup_table_ptrs; }

    MicroTxType *CreateWorkgenArray() {
        MicroTxType *workgen_arr = new MicroTxType[100];

        int i = 0, j = 0;

        j += this->write_ratio;
        for (; i < j; i++) workgen_arr[i] = MicroTxType::kUpdateOne;

        j += this->read_ratio;
        for (; i < j; i++) workgen_arr[i] = MicroTxType::kReadOne;

        assert(i == 100 && j == 100);
        return workgen_arr;
    }
};
