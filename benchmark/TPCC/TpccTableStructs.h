#pragma once

#include <cstdlib>
#include <cstring>

#include "Base/BenchTypes.h"
#include "TPCC/TpccConstant.h"

namespace tpcc {
struct warehouse_value_t {
  double w_tax;
  double w_ytd;
  char w_name[tpcc::WAREHOUSE_NAME_SIZE_MAX + 1];
  char w_street_1[tpcc::WAREHOUSE_STREET_SIZE_MAX + 1];
  char w_street_2[tpcc::WAREHOUSE_STREET_SIZE_MAX + 1];
  char w_city[tpcc::WAREHOUSE_CITY_SIZE_MAX + 1];
  char w_state[tpcc::WAREHOUSE_STATE_SIZE + 1];
  char w_zip[tpcc::WAREHOUSE_ZIP_SIZE + 1];

  warehouse_value_t() = default;
  warehouse_value_t(const warehouse_value_t& w) {
    std::memcpy(this, &w, sizeof(warehouse_value_t));
  }
  warehouse_value_t& operator=(const warehouse_value_t& w) {
    std::memcpy(this, &w, sizeof(warehouse_value_t));
    return *this;
  }

  bool compare(const warehouse_value_t& w) {
    // We do not compare other read-only fields
    return w_tax == w.w_tax && w_ytd == w.w_ytd;
  }
};

struct district_value_t {
  double d_tax;
  double d_ytd;
  uint64_t d_next_oid;
  char d_name[tpcc::DISTRICT_NAME_SIZE_MAX + 1];
  char d_street_1[tpcc::DISTRICT_STREET_SIZE_MAX + 1];
  char d_street_2[tpcc::DISTRICT_STREET_SIZE_MAX + 1];
  char d_city[tpcc::DISTRICT_CITY_SIZE_MAX + 1];
  char d_state[tpcc::DISTRICT_STATE_SIZE + 1];
  char d_zip[tpcc::DISTRICT_ZIP_SIZE + 1];

  district_value_t() = default;
  district_value_t(const district_value_t& w) { std::memcpy(this, &w, sizeof(district_value_t)); }
  district_value_t& operator=(const district_value_t& w) {
    std::memcpy(this, &w, sizeof(district_value_t));
    return *this;
  }

  bool compare(const district_value_t& d) { return d_tax == d.d_tax && d_ytd == d.d_ytd; }
};

struct customer_value_t {
  double c_balance;
  double c_ytd_payment;
  uint32_t c_payment_cnt;
  char c_data[tpcc::CUSTOMER_DATA_SIZE_MAX + 1];

  char c_first[tpcc::CUSTOMER_FIRST_SIZE_MAX + 1];
  char c_middle[tpcc::CUSTOMER_MIDDLE_SIZE + 1];
  char c_last[tpcc::CUSTOMER_LAST_SIZE_MAX + 1];
  char c_street_1[tpcc::CUSTOMER_STREET_SIZE_MAX + 1];
  char c_street_2[tpcc::CUSTOMER_STREET_SIZE_MAX + 1];
  char c_city[tpcc::CUSTOMER_CITY_SIZE_MAX + 1];
  char c_state[tpcc::CUSTOMER_STATE_SIZE + 1];
  char c_zip[tpcc::CUSTOMER_ZIP_SIZE + 1];
  char c_phone[tpcc::CUSTOMER_PHONE_SIZE + 1];
  uint32_t c_since;
  char c_credit[tpcc::CUSTOMER_CREDIT_SIZE + 1];
  double c_credit_limit;
  double c_discount;
  uint32_t c_delivery_cnt;

  customer_value_t() = default;
  customer_value_t(const customer_value_t& c) { std::memcpy(this, &c, sizeof(customer_value_t)); }
  customer_value_t& operator=(const customer_value_t& c) {
    std::memcpy(this, &c, sizeof(customer_value_t));
    return *this;
  }

  bool compare(const customer_value_t& c) {
    return c_balance == c.c_balance && c_ytd_payment == c.c_ytd_payment &&
           c_payment_cnt == c.c_payment_cnt;
  }
};

struct history_value_t {
  double h_amount;
  uint32_t h_date;
  char h_data[tpcc::HISTORY_DATA_SIZE_MAX + 1];

  history_value_t() = default;
  history_value_t(const history_value_t& h) { std::memcpy(this, &h, sizeof(history_value_t)); }
  history_value_t& operator=(const history_value_t& h) {
    std::memcpy(this, &h, sizeof(history_value_t));
    return *this;
  }
};

struct order_value_t {
  int32_t o_c_id;
  int32_t o_carrier_id;
  int32_t o_ol_cnt;
  int32_t o_all_local;
  int64_t o_entry_d;

  order_value_t() = default;
  order_value_t(const order_value_t& o) { std::memcpy(this, &o, sizeof(order_value_t)); }
  order_value_t& operator=(const order_value_t& o) {
    std::memcpy(this, &o, sizeof(order_value_t));
    return *this;
  }
};

struct new_order_value_t {
  int32_t w_id;
  int32_t d_id;
  int32_t o_id;

  new_order_value_t() = default;
  new_order_value_t(const new_order_value_t& no) {
    std::memcpy(this, &no, sizeof(new_order_value_t));
  }
  new_order_value_t& operator=(const new_order_value_t& no) {
    std::memcpy(this, &no, sizeof(new_order_value_t));
    return *this;
  }
};

struct order_line_value_t {
  int32_t ol_i_id;
  int32_t ol_supply_w_id;
  int32_t ol_quantity;
  double ol_amount;
  uint32_t ol_delivery_d;
  char ol_dist_info[tpcc::ORDER_LINE_DIST_INFO_SIZE + 1];

  order_line_value_t() = default;
  order_line_value_t(const order_line_value_t& ol) {
    std::memcpy(this, &ol, sizeof(order_line_value_t));
  }
  order_line_value_t& operator=(const order_line_value_t& ol) {
    std::memcpy(this, &ol, sizeof(order_line_value_t));
    return *this;
  }

  bool compare(const order_line_value_t& ol) const {
    return ol_i_id == ol.ol_i_id && ol_supply_w_id == ol.ol_supply_w_id &&
           ol_quantity == ol.ol_quantity && ol_amount == ol.ol_amount;
  }
};

struct item_value_t {
  int32_t i_im_id;
  double i_price;
  char i_name[tpcc::ITEM_NAME_SIZE_MAX + 1];
  char i_data[tpcc::ITEM_DATA_SIZE_MAX + 1];

  item_value_t() = default;
  item_value_t(const item_value_t& i) { std::memcpy(this, &i, sizeof(item_value_t)); }
  item_value_t& operator=(const item_value_t& i) {
    std::memcpy(this, &i, sizeof(item_value_t));
    return *this;
  }

  bool compare(const item_value_t& v) const { return i_price == v.i_price; }
};

struct stock_value_t {
  int32_t s_quantity;
  int32_t s_ytd;
  int32_t s_order_cnt;
  int32_t s_remote_cnt;
  char s_dist[tpcc::DISTRICTS_PER_WAREHOUSE][tpcc::STOCK_DIST_INFO_EACH + 1];
  char s_data[tpcc::STOCK_DATA_SIZE + 1];

  stock_value_t() = default;
  stock_value_t(const stock_value_t& s) { std::memcpy(this, &s, sizeof(stock_value_t)); }
  stock_value_t& operator=(const stock_value_t& s) {
    std::memcpy(this, &s, sizeof(stock_value_t));
    return *this;
  }

  bool compare(const stock_value_t& v) const {
    // For simplicity, we do not compare the VarChar fields
    return s_quantity == v.s_quantity && s_ytd == v.s_ytd && s_order_cnt == v.s_order_cnt &&
           s_remote_cnt == v.s_remote_cnt;
  }
};
};  // namespace tpcc