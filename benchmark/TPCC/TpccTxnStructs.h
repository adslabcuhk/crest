#pragma once

#include <cstdint>

#include "Base/BenchTypes.h"
#include "TPCC/TpccConstant.h"
#include "TPCC/TpccContext.h"
#include "TPCC/TpccTableStructs.h"
#include "common/Type.h"

namespace tpcc {
class NewOrderTxnParam : public TxnParam {
 public:
  NewOrderTxnParam(TpccContext* ctx) : TxnParam(tpcc::kNewOrder), ctx_(ctx) {}

  RecordKey WarehouseKey() const { return (RecordKey)warehouse_id_; }

  RecordKey DistrictKey() const { return ctx_->MakeDistrictKey(warehouse_id_, district_id_); }

  RecordKey CustomerKey() const {
    return ctx_->MakeCustomerKey(warehouse_id_, district_id_, customer_id_);
  }

  RecordKey StockKey(int i) const { return stock_keys_[i]; }

  RecordKey ItemKey(int i) const { return (RecordKey)(item_ids_[i]); }

  RecordKey OrderLineKey(uint64_t oid, int i) const {
    return ctx_->MakeOrderLineKey(warehouse_id_, district_id_, oid, i);
  }

  RecordKey NewOrderKey(uint64_t oid) const {
    return ctx_->MakeNewOrderKey(warehouse_id_, district_id_, oid);
  }

  RecordKey OrderKey(uint64_t oid) const {
    return ctx_->MakeOrderKey(warehouse_id_, district_id_, oid);
  }

  char* Serialize(char* dst) const override {
    *(int32_t*)dst = warehouse_id_;
    dst += sizeof(int32_t);
    *(int32_t*)dst = district_id_;
    dst += sizeof(int32_t);
    *(int32_t*)dst = customer_id_;
    dst += sizeof(int32_t);
    *(int*)dst = ol_num_;
    dst += sizeof(int);

    std::memcpy(dst, (char*)stock_keys_, ol_num_ * sizeof(RecordKey));
    dst += ol_num_ * sizeof(RecordKey);

    std::memcpy(dst, (char*)item_ids_, ol_num_ * sizeof(int64_t));
    dst += ol_num_ * sizeof(int64_t);

    std::memcpy(dst, (char*)supply_warehouse_ids_, ol_num_ * sizeof(int32_t));
    dst += ol_num_ * sizeof(int32_t);

    std::memcpy(dst, (char*)quantities_, ol_num_ * sizeof(int32_t));
    dst += ol_num_ * sizeof(int32_t);

    return dst;
  }

  const char* Deserialize(const char* src) override { return src; }

 public:
  int32_t warehouse_id_;
  int32_t district_id_;
  int32_t customer_id_;

  int ol_num_;  // Number of items to access
  RecordKey stock_keys_[MAX_OL_CNT];
  int64_t item_ids_[MAX_OL_CNT];
  int32_t supply_warehouse_ids_[MAX_OL_CNT];
  int32_t quantities_[MAX_OL_CNT];

  const TpccContext* ctx_;
};

class PaymentTxnParam : public TxnParam {
 public:
  PaymentTxnParam(TpccContext* ctx) : TxnParam(tpcc::kPayment), ctx_(ctx) {}

  RecordKey WarehouseKey() const { return (RecordKey)warehouse_id_; }

  RecordKey DistrictKey() const { return ctx_->MakeDistrictKey(warehouse_id_, district_id_); }

  RecordKey CustomerKey() const {
    return ctx_->MakeCustomerKey(warehouse_id_, district_id_, customer_id_);
  }

  RecordKey HistoryKey() const {
    return ctx_->MakeHistoryKey(warehouse_id_, district_id_, customer_id_, h_date_);
  }

  char* Serialize(char* dst) const override { return dst; }

  const char* Deserialize(const char* src) override { return src; }

 public:
  int32_t warehouse_id_;
  int32_t district_id_;
  int32_t customer_id_;
  int32_t c_w_id_;
  int32_t c_d_id_;
  double h_amount_;
  int64_t h_date_;
  const TpccContext* ctx_;
};

class DeliveryTxnParam : public TxnParam {
 public:
  DeliveryTxnParam(TpccContext* ctx) : TxnParam(tpcc::kDelivery), ctx_(ctx) {}

  char* Serialize(char* dst) const override { return dst; }

  const char* Deserialize(const char* src) override { return src; }

 public:
  int32_t warehouse_id_;
  int32_t o_id_;
  int32_t o_carrier_id_;
  int64_t ol_delivery_d_;
  const TpccContext* ctx_;
};

class OrderStatusTxnParam : public TxnParam {
 public:
  OrderStatusTxnParam(TpccContext* ctx) : TxnParam(tpcc::kOrderStatus), ctx_(ctx) {}

  char* Serialize(char* dst) const override { return dst; }

  const char* Deserialize(const char* src) override { return src; }

  RecordKey CustomerKey() const {
    return ctx_->MakeCustomerKey(warehouse_id_, district_id_, customer_id_);
  }

  RecordKey OrderKey() const { return ctx_->MakeOrderKey(warehouse_id_, district_id_, order_id_); }

  RecordKey OrderLineKey(int i) const {
    return ctx_->MakeOrderLineKey(warehouse_id_, district_id_, order_id_, i);
  }

 public:
  const TpccContext* ctx_;
  int32_t warehouse_id_;
  int32_t district_id_;
  int32_t customer_id_;
  int32_t order_id_;
};

class NewOrderTxnResult : public TxnResult {
 public:
  NewOrderTxnResult() : TxnResult(tpcc::kNewOrder) {}

  char* Serialize(char* dst) const override { return dst; }

  const char* Deserialize(const char* src) override { return src; }

 public:
#if DEBUG_LOCALIZATION
  int txn_count, rw_txn_count;
  int unref_txn_count, unref_rw_txn_count;
  uint64_t pv_access_bm, pv_rw_bm;
#endif

  double w_tax;

  double d_tax;
  uint64_t next_o_id;
  double wr_d_ytd;

  double c_discount;
  char c_last[tpcc::CUSTOMER_LAST_SIZE_MAX + 1];
  char c_credit[tpcc::CUSTOMER_CREDIT_SIZE + 1];

  stock_value_t stock_vals[MAX_OL_CNT];

  item_value_t item_vals[MAX_OL_CNT];

  order_value_t o_val;

  new_order_value_t no_val;

  order_line_value_t ol_vals[MAX_OL_CNT];

#if DEBUG_LOCALIZATION_PERF
  int yield_next_count[16];
  double yield_next_time;
#endif

#if DEBUG_LOCALIZATION
  uint64_t dg_commit_bm;
  uint64_t dg_lock_bm;
  int16_t txn_counts[MAX_OL_CNT];
  int16_t unref_txn_counts[MAX_OL_CNT];
  uint64_t commit_bm[MAX_OL_CNT];
#endif

#if DEBUG_NEWORDER
  int32_t wr_quantity_vals[MAX_OL_CNT];
  int32_t rd_quantity_vals[MAX_OL_CNT];
#endif

  // If the comparison failed, this function returns the ``TableId'' which fails the comparison + 1
  int Compare(const TxnParam* p, const TxnResult* r, int* idx) const {
    NewOrderTxnParam* params = (NewOrderTxnParam*)p;
    NewOrderTxnResult* res = (NewOrderTxnResult*)r;

    // We return the unmatched table id (+1) for debugging
    if (w_tax != res->w_tax) return WAREHOUSE_TABLE + 1;

    if (d_tax != res->d_tax) return DISTRICT_TABLE + 1;

    if (next_o_id != res->next_o_id) return DISTRICT_TABLE + 1;

    if (c_discount != res->c_discount) return CUSTOMER_TABLE + 1;

    if (strcmp(c_last, res->c_last) || strcmp(c_credit, res->c_credit)) return CUSTOMER_TABLE + 1;

    for (int i = 0; i < params->ol_num_; ++i) {
      if (!stock_vals[i].compare(res->stock_vals[i])) {
        *idx = i;
        return STOCK_TABLE + 1;
      }
      if (!item_vals[i].compare(res->item_vals[i])) {
        *idx = i;
        return ITEM_TABLE + 1;
      }
    }
    return 0;
  }
};

class PaymentTxnResult : public TxnResult {
 public:
  PaymentTxnResult() : TxnResult(kPayment) {}

  char* Serialize(char* dst) const override { return dst; }

  const char* Deserialize(const char* src) override { return src; }

 public:
  warehouse_value_t w_val;

#if DEBUG_LOCALIZATION_PERF
  int yield_next_count[16];
  double yield_next_time;
  double rd_d_ytd;
  double wr_d_ytd;
  double rd_w_ytd;
  double wr_w_ytd;
#endif

  district_value_t d_val;
  uint64_t dg_lock_bm;
  uint64_t dg_commit_bm;

  customer_value_t c_val;

  history_value_t h_val;

  int Compare(const TxnParam* p, const TxnResult* r) const {
    PaymentTxnParam* params = (PaymentTxnParam*)p;
    PaymentTxnResult* res = (PaymentTxnResult*)r;

    // For Payment transaction, we only care about the modified value, i.e.,
    // the ytd value of warehouse table and distrit table. For simplicity,
    // we ommit comparing the string values and only compare the numerical
    // values
    if (w_val.w_ytd != res->w_val.w_ytd) {
      return WAREHOUSE_TABLE + 1;
    }

    if (d_val.d_ytd != res->d_val.d_ytd) {
      return DISTRICT_TABLE + 1;
    }

    // clang-format off
    if (c_val.c_balance != res->c_val.c_balance ||
        c_val.c_ytd_payment != res->c_val.c_ytd_payment ||
        c_val.c_payment_cnt != res->c_val.c_payment_cnt) {
      return CUSTOMER_TABLE + 1;
    }
    // clang-format on

    return 0;
  }
};

class DeliveryTxnResult : public TxnResult {
 public:
  DeliveryTxnResult() : TxnResult(kDelivery) {}

  char* Serialize(char* dst) const override { return dst; }

  const char* Deserialize(const char* src) override { return src; }
};

class OrderStatusTxnResult : public TxnResult {
 public:
  OrderStatusTxnResult() : TxnResult(kDelivery) {}

  char* Serialize(char* dst) const override { return dst; }

  const char* Deserialize(const char* src) override { return src; }

  int Compare(const TxnParam* p, const TxnResult* r) const {
    OrderStatusTxnParam* params = (OrderStatusTxnParam*)p;
    OrderStatusTxnResult* res = (OrderStatusTxnResult*)r;

    if (this->c_balance != res->c_balance) {
      return CUSTOMER_TABLE + 1;
    }

    bool o_val_equal = 
      (this->o_val.o_entry_d == res->o_val.o_entry_d) &&
      (this->o_val.o_carrier_id == res->o_val.o_carrier_id) &&
      (this->o_val.o_ol_cnt == res->o_val.o_ol_cnt);
    if (!o_val_equal) {
      return ORDER_TABLE + 1;
    }

    int ol_cnt = o_val.o_ol_cnt;
    for (int i = 0; i < ol_cnt; ++i) {
      if ((this->ol_val[i]).compare(res->ol_val[i])) {
        return ORDER_LINE_TABLE + 1;
      }
    }
    return 0;
  }

 public:
  double c_balance;
  order_value_t o_val;
  order_line_value_t ol_val[MAX_OL_CNT];
};
}  // namespace tpcc