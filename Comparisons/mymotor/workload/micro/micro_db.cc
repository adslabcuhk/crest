// Author: Ming Zhang
// Copyright (c) 2023

#include "micro/micro_db.h"

#include "base/common.h"
#include "unistd.h"
#include "util/json_config.h"

/* Called by main. Only initialize here. The worker threads will populate. */
void MICRO::LoadTable(node_id_t node_id, node_id_t num_server,
                      MemStoreAllocParam *mem_store_alloc_param,
                      size_t &total_size, size_t &ht_loadfv_size,
                      size_t &ht_size, size_t &initfv_size,
                      size_t &real_cvt_size) {
  // Initiate + Populate table for primary role
  {
    RDMA_LOG(DBG) << "Loading MICRO table";
    std::string config_filepath = "../../../config/micro_config.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto table_config = json_config.get("micro");
    // micro_table = new HashStore((table_id_t)MicroTableType::kMicroTable,
    //                             table_config.get("num_keys").get_uint64(),
    //                             mem_store_alloc_param);
    micro_table0 = new HashStore((table_id_t)MicroTableType::kMicroTable0,
                                 table_config.get("num_keys").get_uint64(),
                                 mem_store_alloc_param);

    micro_table1 = new HashStore((table_id_t)MicroTableType::kMicroTable1,
                                 table_config.get("num_keys").get_uint64(),
                                 mem_store_alloc_param);
    PopulateMicroTable(0);
    PopulateMicroTable(1);
    total_size += micro_table0->GetTotalSize();
    ht_loadfv_size += micro_table0->GetHTInitFVSize();
    ht_size += micro_table0->GetHTSize();
    initfv_size += micro_table0->GetInitFVSize();
    real_cvt_size += micro_table0->GetLoadCVTSize();

    total_size += micro_table1->GetTotalSize();
    ht_loadfv_size += micro_table1->GetHTInitFVSize();
    ht_size += micro_table1->GetHTSize();
    initfv_size += micro_table1->GetInitFVSize();
    real_cvt_size += micro_table1->GetLoadCVTSize();
  }

  std::cout << "----------------------------------------------------------"
            << std::endl;
  // Assign primary

  if ((node_id_t)MicroTableType::kMicroTable0 % num_server == node_id) {
    RDMA_LOG(EMPH) << "[Primary] MICRO-0 table ID: "
                   << (node_id_t)MicroTableType::kMicroTable0;
    std::cerr << "Number of initial records: " << std::dec
              << micro_table0->GetInitInsertNum() << std::endl;
    primary_table_ptrs.push_back(micro_table0);
  }

  if ((node_id_t)MicroTableType::kMicroTable1 % num_server == node_id) {
    RDMA_LOG(EMPH) << "[Primary] MICRO-1 table ID: "
                   << (node_id_t)MicroTableType::kMicroTable1;
    std::cerr << "Number of initial records: " << std::dec
              << micro_table1->GetInitInsertNum() << std::endl;
    primary_table_ptrs.push_back(micro_table1);
  }

  std::cout << "----------------------------------------------------------"
            << std::endl;
  // Assign backup

  if (BACKUP_NUM < num_server) {
    for (node_id_t i = 1; i <= BACKUP_NUM; i++) {
      if ((node_id_t)MicroTableType::kMicroTable0 % num_server ==
          (node_id - i + num_server) % num_server) {
        RDMA_LOG(EMPH) << "[Backup] MICRO-0 table ID: "
                       << (node_id_t)MicroTableType::kMicroTable0;
        std::cerr << "Number of initial records: " << std::dec
                  << micro_table0->GetInitInsertNum() << std::endl;
        backup_table_ptrs.push_back(micro_table0);
      }

      if ((node_id_t)MicroTableType::kMicroTable1 % num_server ==
          (node_id - i + num_server) % num_server) {
        RDMA_LOG(EMPH) << "[Backup] MICRO-1 table ID: "
                       << (node_id_t)MicroTableType::kMicroTable1;
        std::cerr << "Number of initial records: " << std::dec
                  << micro_table1->GetInitInsertNum() << std::endl;
        backup_table_ptrs.push_back(micro_table1);
      }
    }
  }
}

void MICRO::PopulateMicroTable(int table_id) {
  RDMA_LOG(DBG) << "NUM KEYS TOTAL: " << num_keys_global;
  for (uint64_t id = 0; id < num_keys_global; id++) {
    if (id % 2 != table_id) {
      continue;
    }
    micro_key_t micro_key;
    micro_key.micro_id = (uint64_t)id;

    micro_val_t micro_val;

    // write into the micro_val:
    strcpy(micro_val.d1, "d1");
    strcpy(micro_val.d2, "d2");
    strcpy(micro_val.d3, "d3");
    strcpy(micro_val.d4, "d4");

    HashStore *table = (table_id == 0 ? micro_table0 : micro_table1);
    table_id_t insert_table_id = (table_id == 0 ? MicroTableType::kMicroTable0
                                                : MicroTableType::kMicroTable1);

    LoadRecord(table, micro_key.item_key, (void *)&micro_val,
               sizeof(micro_val_t), insert_table_id);
  }
}

void MICRO::LoadRecord(HashStore *table, itemkey_t item_key, void *val_ptr,
                       size_t val_size, table_id_t table_id) {
  assert(val_size <= MAX_VALUE_SIZE);
  table->LocalInsertTuple(item_key, (char *)val_ptr, val_size);
}
