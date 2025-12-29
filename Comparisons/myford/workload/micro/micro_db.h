// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <cassert>
#include <cstdint>
#include <vector>

#include "config/table_type.h"
#include "memstore/hash_store.h"
#include "util/fast_random.h"
#include "util/json_config.h"

// (kqh): We change the MICRO table to be a simple key-value store:

static constexpr size_t MAX_MICRO_OP_RECORD_NUM = 12;

#define FREQUENCY_UPDATE 100
#define FREQUENCY_READ 00

union micro_key_t {
    uint64_t micro_id;
    uint64_t item_key;

    micro_key_t() { item_key = 0; }
};

static_assert(sizeof(micro_key_t) == sizeof(uint64_t), "");

struct micro_val_t {
    // 40 bytes, consistent with FaSST
    // uint64_t magic[5];
    char d1[58];
    char d2[58];
    char d3[58];
    char d4[58];
};
static_assert(sizeof(micro_val_t) == 232, "");

// Magic numbers for debugging. These are unused in the spec.
#define Micro_MAGIC 97 /* Some magic number <= 255 */
#define micro_magic (Micro_MAGIC)

// Helpers for generating workload
#define MICRO_TX_TYPES 2
const std::string MICRO_TX_NAME[MICRO_TX_TYPES] = {"Update", "Read"};
enum class MicroTxType : int {
    // kLockContention,
    kUpdate = 0,
    kRead,
};

// For MicroTable, we split the table into two tables: each contains
// half of the keys
enum class MicroTableType : uint64_t {
    kMicroTable0 = TABLE_MICRO,
    kMicroTable1,
};

static ALWAYS_INLINE uint64_t align_pow2(uint64_t v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    return v + 1;
}

struct UpdateTxnParam {
    uint64_t key_num;  // number of record accessed in this txn
    uint64_t keys[MAX_MICRO_OP_RECORD_NUM];
    uint64_t columns[MAX_MICRO_OP_RECORD_NUM];
};

struct ReadTxnParam {
    // number of records accessed in this txn
    uint64_t key_num;
    uint64_t keys[MAX_MICRO_OP_RECORD_NUM];
};

class MICRO {
   public:
    std::string bench_name;

    uint64_t num_keys_global;

    double zipfian;

    uint64_t num_op;

    int coro_num;

    std::vector<MicroTxType> *txn_run_types;

    std::vector<void *> *txn_run_params;

    /* Tables */
    HashStore *micro_table0;
    HashStore *micro_table1;

    std::vector<HashStore *> primary_table_ptrs;

    std::vector<HashStore *> backup_table_ptrs;

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
        auto num_keys = conf.get("num_keys").get_int64();
        // num_keys_global = align_pow2(num_keys);
        this->num_keys_global = num_keys;
        this->zipfian = conf.get("zipfian").get_double();
        this->num_op = std::min(conf.get("num_op").get_uint64(), MAX_MICRO_OP_RECORD_NUM);
        this->write_ratio = conf.get("write_ratio").get_int64();
        this->read_ratio = 100 - this->write_ratio;
        micro_table0 = nullptr;
        micro_table1 = nullptr;
    }

    ~MICRO() {
        if (micro_table0) delete micro_table0;
        if (micro_table1) delete micro_table1;
    }

    ALWAYS_INLINE
    MicroTxType *CreateWorkgenArray() {
        MicroTxType *workgen_arr = new MicroTxType[100];

        int i = 0, j = 0;

        j += this->write_ratio;
        for (; i < j; i++) workgen_arr[i] = MicroTxType::kUpdate;

        j += this->read_ratio;
        for (; i < j; i++) workgen_arr[i] = MicroTxType::kRead;

        assert(i == 100 && j == 100);
        return workgen_arr;
    }

    void LoadTable(node_id_t node_id, node_id_t num_server,
                   MemStoreAllocParam *mem_store_alloc_param,
                   MemStoreReserveParam *mem_store_reserve_param);

    void PopulateMicroTable(MemStoreReserveParam *mem_store_reserve_param, int id);

    int LoadRecord(HashStore *table, itemkey_t item_key, void *val_ptr, size_t val_size,
                   table_id_t table_id, MemStoreReserveParam *mem_store_reserve_param);

    ALWAYS_INLINE
    std::vector<HashStore *> GetPrimaryHashStore() { return primary_table_ptrs; }

    ALWAYS_INLINE
    std::vector<HashStore *> GetBackupHashStore() { return backup_table_ptrs; }
};
