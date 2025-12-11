#pragma once

#include <cstdint>

#include "Base/BenchTypes.h"
#include "TATP/TATPConstant.h"
#include "TATP/TATPContext.h"
#include "TATP/TATPTableStructs.h"
#include "common/Type.h"

namespace tatp {
class GetSubscriberTxnParam : public TxnParam {
   public:
    GetSubscriberTxnParam(TATPContext* ctx) : TxnParam(tatp::kGetSubscriber), ctx_(ctx) {}

    RecordKey SubscriberKey() const { return ctx_->MakeSubscriberKey(s_id_); }

    char* Serialize(char* dst) const override { return dst; }

    const char* Deserialize(const char* src) override { return src; }

   public:
    int32_t s_id_;
    const TATPContext* ctx_;
};

class GetSubscriberTxnResult : public TxnResult {
   public:
    GetSubscriberTxnResult() : TxnResult(tatp::kGetSubscriber) {}

    char* Serialize(char* dst) const override { return dst; }

    const char* Deserialize(const char* src) override { return src; }

   public:
    subscriber_value_t s_val;
};

class GetNewDestinationTxnParam : public TxnParam {
   public:
    GetNewDestinationTxnParam(TATPContext* ctx) : TxnParam(tatp::kGetNewDestination), ctx_(ctx) {}

    // RecordKey AccessInfoKey() const { return ctx_->MakeAccessInfoKey(s_id_, sf_type_); }
    RecordKey SpecialFacilityKey() const { return ctx_->MakeSpecialFacilityKey(s_id_, sf_type_); }

    RecordKey CallForwardingKey(int i) const {
        return ctx_->MakeCallForwardingKey(s_id_, sf_type_, i);
    }

    char* Serialize(char* dst) const override { return dst; }

    const char* Deserialize(const char* src) override { return src; }

   public:
    int32_t s_id_;
    int32_t sf_type_;
    int fetch_num_;  // Number of CallForwarding records to fetch
    uint64_t start_time_, end_time_;
    const TATPContext* ctx_;
};

class GetNewDestinationTxnResult : public TxnResult {
   public:
    GetNewDestinationTxnResult() : TxnResult(tatp::kGetNewDestination) {}

    char* Serialize(char* dst) const override { return dst; }

    const char* Deserialize(const char* src) override { return src; }
};

class GetAccessDataTxnParam : public TxnParam {
   public:
    GetAccessDataTxnParam(TATPContext* ctx) : TxnParam(tatp::kGetAccessData), ctx_(ctx) {}

    RecordKey AccessInfoKey() const { return ctx_->MakeAccessInfoKey(s_id_, ai_type_); }

    char* Serialize(char* dst) const override { return dst; }

    const char* Deserialize(const char* src) override { return src; }

   public:
    int32_t s_id_;
    int32_t ai_type_;
    const TATPContext* ctx_;
};

class GetAccessDataTxnResult : public TxnResult {
   public:
    GetAccessDataTxnResult() : TxnResult(tatp::kGetAccessData) {}

    char* Serialize(char* dst) const override { return dst; }

    const char* Deserialize(const char* src) override { return src; }

   public:
    accessinf_val_t ai_val;
};

class UpdateSubscriberDataTxnParam : public TxnParam {
   public:
    UpdateSubscriberDataTxnParam(TATPContext* ctx)
        : TxnParam(tatp::kUpdateSubscriberData), ctx_(ctx) {}

    RecordKey SubscriberKey() const { return ctx_->MakeSubscriberKey(s_id_); }

    RecordKey SpecialFacilityKey() const { return ctx_->MakeSpecialFacilityKey(s_id_, sf_type_); }

    char* Serialize(char* dst) const override { return dst; }

    const char* Deserialize(const char* src) override { return src; }

   public:
    int32_t s_id_;
    int32_t sf_type_;
    subscriber_value_t s_val;
    const TATPContext* ctx_;
};

class UpdateSubscriberDataTxnResult : public TxnResult {
   public:
    UpdateSubscriberDataTxnResult() : TxnResult(tatp::kUpdateSubscriberData) {}
    char* Serialize(char* dst) const override { return dst; }

    const char* Deserialize(const char* src) override { return src; }
};

class UpdateLocationTxnParam : public TxnParam {
   public:
    UpdateLocationTxnParam(TATPContext* ctx) : TxnParam(tatp::kUpdateLocation), ctx_(ctx) {}

    RecordKey SubscriberKey() const { return ctx_->MakeSubscriberKey(s_id_); }

    RecordKey SecSubsciberKey() const { return ctx_->MakeSecSubsciberKey(s_id_); }

    char* Serialize(char* dst) const override { return dst; }

    const char* Deserialize(const char* src) override { return src; }

   public:
    int32_t s_id_;
    int32_t vlr_location_;
    const TATPContext* ctx_;
};

class UpdateLocationTxnResult : public TxnResult {
   public:
    UpdateLocationTxnResult() : TxnResult(tatp::kUpdateLocation) {}

    char* Serialize(char* dst) const override { return dst; }

    const char* Deserialize(const char* src) override { return src; }
};

class InsertCallForwardingTxnParam : public TxnParam {
   public:
    InsertCallForwardingTxnParam(TATPContext* ctx)
        : TxnParam(tatp::kInsertCallForwarding), ctx_(ctx) {}

    RecordKey CallForwardingKey() const {
        return ctx_->MakeCallForwardingKey(s_id_, sf_type_, start_time_);
    }

    char* Serialize(char* dst) const override { return dst; }

    const char* Deserialize(const char* src) override { return src; }

    RecordKey SecSubscriberKey() const { return ctx_->MakeSecSubsciberKey(s_id_); }

    RecordKey SubscriberKey() const { return ctx_->MakeSubscriberKey(s_id_); }

   public:
    int32_t s_id_;
    int32_t sf_type_;
    int32_t start_time_;
    int32_t end_time_;
    callfwd_val_t cf_val;
    const TATPContext* ctx_;
};

class InsertCallForwardingTxnResult : public TxnResult {
   public:
    InsertCallForwardingTxnResult() : TxnResult(tatp::kInsertCallForwarding) {}

    char* Serialize(char* dst) const override { return dst; }

    const char* Deserialize(const char* src) override { return src; }
};

};  // namespace tatp
