#pragma once
#include "TPCC/TpccConstant.h"
#include "common/Type.h"
#include "db/PoolHashIndex.h"
#include "util/JsonConfig.h"
#include "util/Macros.h"

struct TpccConfig {
  int num_warehouse;  // Number of warehouse

  int num_district_per_warehouse;  // #District of each warehouse

  int num_customer_per_district;  // #Client of each district

  int num_item;  // #item of each warehouse

  int num_stock_per_warehouse;

  bool uniform_item_dist = false;

  PoolHashIndex::Config warehouse_index_conf;
  PoolHashIndex::Config district_index_conf;
  PoolHashIndex::Config customer_index_conf;
  PoolHashIndex::Config history_index_conf;
  PoolHashIndex::Config neworder_index_conf;
  PoolHashIndex::Config order_index_conf;
  PoolHashIndex::Config orderline_index_conf;
  PoolHashIndex::Config item_index_conf;
  PoolHashIndex::Config stock_index_conf;
};

// Generate the TPC-C configuration from file
ALWAYS_INLINE
TpccConfig ParseTpccConfig(const std::string& fname) {
  auto json_config = JsonConfig::load_file(fname);
  auto table_config = json_config.get("tpcc");
  TpccConfig config;

  // The Warehouse records can be directly indexed by its id
  config.num_warehouse = table_config.get("num_warehouse").get_uint64();
  config.warehouse_index_conf.hash_type_ = kDirect;
  config.warehouse_index_conf.bucket_num_ = config.num_warehouse;
  config.warehouse_index_conf.records_per_bucket_ = 1;

  // The District records can be directly indexed by the district id, i.e.,
  //  the warehouse_id * district_per_warehouse + district_id
  config.num_district_per_warehouse = table_config.get("num_district_per_warehouse").get_uint64();
  config.district_index_conf.hash_type_ = kDirect;
  config.district_index_conf.bucket_num_ = config.num_warehouse * config.num_district_per_warehouse;
  config.district_index_conf.records_per_bucket_ = 1;

  // The Customer records can be directly indexed by the customer id, i.e.,
  //  the district_id * customers_per_district + customer_id
  config.num_customer_per_district = table_config.get("num_customer_per_district").get_uint64();
  config.customer_index_conf.bucket_num_ =
      config.num_warehouse * config.num_district_per_warehouse * config.num_customer_per_district;
  config.customer_index_conf.records_per_bucket_ = 1;
  config.customer_index_conf.hash_type_ = kDirect;

  // The History Records can be directly indexed by the customer id and his payment timestamp
  config.history_index_conf.hash_type_ = kDirect;
  config.history_index_conf.records_per_bucket_ = 1;
  config.history_index_conf.bucket_num_ =
      config.customer_index_conf.bucket_num_ * tpcc::MAX_PAYMENT_CNT_PER_CUSTOMER;

  // The Records in Order Table can be indexed using the district_id + order_id, in which the
  // order_id is limited with tpcc::MAX_ORDER_PER_DISTRICT
  config.order_index_conf.bucket_num_ =
      config.district_index_conf.bucket_num_ * tpcc::MAX_ORDERID_PER_DISTRICT;
  config.order_index_conf.records_per_bucket_ = 1;
  config.order_index_conf.hash_type_ = kDirect;

  // The Records in New Order Table can be indexed using the district_id + order_id, in which the
  // order_id is limited with tpcc::MAX_ORDER_PER_DISTRICT
  config.neworder_index_conf.bucket_num_ =
      config.district_index_conf.bucket_num_ * tpcc::MAX_ORDERID_PER_DISTRICT;
  config.neworder_index_conf.records_per_bucket_ = 1;
  config.neworder_index_conf.hash_type_ = kDirect;

  // The Records in OrderLine can be indexed using the district_id + order_id, in which the
  // order_id is limited with tpcc::MAX_ORDER_PER_DISTRICT
  config.orderline_index_conf.bucket_num_ =
      config.district_index_conf.bucket_num_ * tpcc::MAX_ORDERID_PER_DISTRICT;

  // The Slot number is the maximum number of orders can be made
  config.orderline_index_conf.records_per_bucket_ = tpcc::MAX_OL_CNT;
  config.orderline_index_conf.hash_type_ = kCustomized;
  std::function<uint64_t(RecordKey)> hasher = [](RecordKey key) { return key / 15; };
  config.orderline_index_conf.hasher_ = hasher;

  // Index configuration for the Stock table
  config.num_stock_per_warehouse = table_config.get("num_stock_per_warehouse").get_uint64();
  config.stock_index_conf.bucket_num_ = config.num_warehouse * config.num_stock_per_warehouse;
  config.stock_index_conf.records_per_bucket_ = 1;
  config.stock_index_conf.hash_type_ = kDirect;

  // Index configuration for the Item table
  config.num_item = table_config.get("num_item").get_uint64();
  config.item_index_conf.bucket_num_ = config.num_item;
  config.item_index_conf.records_per_bucket_ = 1;
  config.item_index_conf.hash_type_ = kDirect;

  return config;
}

struct TpccContext {
 public:
  TpccContext(const TpccConfig& config) : config(config) {}

  RecordKey MakeStockKey(int32_t w_id, int32_t i_id) const {
    return w_id * config.num_stock_per_warehouse + i_id;
  }

  RecordKey MakeDistrictKey(int32_t w_id, int32_t d_id) const {
    return w_id * config.num_district_per_warehouse + d_id;
  }

  RecordKey MakeCustomerKey(int32_t w_id, int32_t d_id, int32_t c_id) const {
    uint64_t upper_id = w_id * config.num_district_per_warehouse + d_id;
    return upper_id * config.num_customer_per_district + c_id;
  }

  RecordKey MakeOrderLineKey(int32_t w_id, int32_t d_id, uint32_t o_id, uint32_t number) const {
    uint32_t upper_id = w_id * config.num_district_per_warehouse + d_id;
    int64_t oid = (uint64_t)upper_id * tpcc::DISTRICT_MAX_OID + (uint64_t)o_id;
    uint64_t olid = oid * tpcc::MAX_OL_CNT + number;
    return olid;
  }

  RecordKey MakeNewOrderKey(int32_t w_id, int32_t d_id, uint32_t o_id) const {
    uint64_t upper_id = w_id * config.num_district_per_warehouse + d_id;
    // This would make the insertion easier
    return upper_id * tpcc::DISTRICT_MAX_OID + o_id;
  }

  RecordKey MakeOrderKey(int32_t w_id, int32_t d_id, uint32_t o_id) const {
    uint64_t upper_id = w_id * config.num_district_per_warehouse + d_id;
    // This would make the insertion easier
    return upper_id * tpcc::DISTRICT_MAX_OID + o_id;
  }

  RecordKey MakeHistoryKey(uint32_t h_w_id, uint32_t h_d_id, uint32_t h_c_w_id, uint32_t h_c_d_id,
                        uint32_t h_c_id) const {
    uint64_t cid = (h_c_w_id * config.num_district_per_warehouse + h_c_d_id) *
                       config.num_customer_per_district +
                   h_c_id;
    uint64_t did = h_d_id + (h_w_id * config.num_district_per_warehouse);
    return (cid << 20) | did;
  }

  // Indexing the history record using the customer's id and his payment timepoint "h_date"
  RecordKey MakeHistoryKey(uint32_t w_id, uint32_t d_id, uint32_t c_id, uint64_t h_date) const {
    uint64_t cid =
        (w_id * config.num_district_per_warehouse + d_id) * config.num_customer_per_district + c_id;
    return (cid << 30) | h_date;
  }

  TpccConfig config;
};
