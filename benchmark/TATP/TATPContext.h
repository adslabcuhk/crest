#pragma once
#include "TATP/TATPConstant.h"
#include "TATP/TATPTableStructs.h"
#include "common/Type.h"
#include "db/PoolHashIndex.h"
#include "util/JsonConfig.h"
#include "util/Macros.h"

struct TATPConfig {
    int num_subscriber;  // number of records of subscriber table
    // The number of records of the Subscriber Table determines the size
    // of other tables, so we only need to set this number

    /* TATP spec parameter for non-uniform random generation */
    uint32_t A;

    PoolHashIndex::Config subscriber_index_conf;
    PoolHashIndex::Config secondary_subscriber_index_conf;
    PoolHashIndex::Config access_info_index_conf;
    PoolHashIndex::Config special_facility_index_conf;
    PoolHashIndex::Config call_forwarding_index_conf;
};

ALWAYS_INLINE
TATPConfig ParseTATPConfig(const std::string& fname) {
    auto json_config = JsonConfig::load_file(fname);
    auto table_config = json_config.get("tatp");
    TATPConfig config;

    auto subscriber_size = table_config.get("num_subscriber").get_uint64();

    // The Subscriber Table can be directly indexed by its id:
    config.num_subscriber = subscriber_size;
    config.subscriber_index_conf.hash_type_ = kDirect;
    config.subscriber_index_conf.bucket_num_ = config.num_subscriber;
    config.subscriber_index_conf.records_per_bucket_ = 1;

    // Set the Non-Uniformly Access Parameters:
    if (subscriber_size <= 1000000) {
        config.A = 65535;
    } else if (subscriber_size <= 10000000) {
        config.A = 1048575;
    } else {
        config.A = 2097151;
    }

    // The SecondarySubscriber Table can be directly indexed by s_id:
    config.secondary_subscriber_index_conf.hash_type_ = kDirect;
    config.secondary_subscriber_index_conf.bucket_num_ = config.num_subscriber * 2;
    // Each bucket contains 5 records
    config.secondary_subscriber_index_conf.records_per_bucket_ = 5;

    // The AccessInfo Table can be directly indexed by s_id + ai_type:
    config.access_info_index_conf.hash_type_ = kDirect;
    config.access_info_index_conf.bucket_num_ =
        config.num_subscriber * tatp::AI_TYPE_PER_SUBSCRIBER;
    config.access_info_index_conf.records_per_bucket_ = 1;


    // The SpecialFacility Table can be directly indexed by s_id + sf_type:
    config.special_facility_index_conf.hash_type_ = kDirect;
    config.special_facility_index_conf.bucket_num_ =
        config.num_subscriber * tatp::SF_TYPE_PER_SUBSCRIBER;
    config.special_facility_index_conf.records_per_bucket_ = 1;

    // The CallForwarding Table can be directly indexed by s_id + sf_type + start_time
    config.call_forwarding_index_conf.hash_type_ = kDirect;
    config.call_forwarding_index_conf.hash_type_ = kDirect;
    config.call_forwarding_index_conf.bucket_num_ =
        config.num_subscriber * tatp::SF_TYPE_PER_SUBSCRIBER * tatp::STARTTIME_PER_FACILITY;
    config.call_forwarding_index_conf.records_per_bucket_ = 1;
    return config;
}

struct TATPContext {
   public:
    TATPContext(const TATPConfig& config) : config(config) {
        map_1000 = (uint16_t*)malloc(1000 * sizeof(uint16_t));
        for (size_t i = 0; i < 1000; i++) {
            uint32_t dig_1 = (i / 1) % 10;
            uint32_t dig_2 = (i / 10) % 10;
            uint32_t dig_3 = (i / 100) % 10;
            map_1000[i] = (dig_3 << 8) | (dig_2 << 4) | dig_1;
        }
    }

    RecordKey MakeSubscriberKey(int32_t s_id) const { return s_id; }

    RecordKey MakeSecSubsciberKey(int32_t s_id) const {
        tatp::tatp_sub_number sub_number;
        sub_number.key = 0;
        sub_number.dec_0_1_2 = map_1000[s_id % 1000];
        s_id /= 1000;
        sub_number.dec_3_4_5 = map_1000[s_id % 1000];
        s_id /= 1000;
        sub_number.dec_6_7_8 = map_1000[s_id % 1000];
        return sub_number.key;
    }

    RecordKey MakeAccessInfoKey(int32_t s_id, int32_t ai_type) const {
        return s_id * tatp::AI_TYPE_PER_SUBSCRIBER + ai_type;
    }

    RecordKey MakeSpecialFacilityKey(int32_t s_id, int32_t sf_type) const {
        return s_id * tatp::SF_TYPE_PER_SUBSCRIBER + sf_type;
    }

    RecordKey MakeCallForwardingKey(int32_t s_id, int32_t sf_type, int32_t start_time) const {
        auto upper_id = s_id * tatp::SF_TYPE_PER_SUBSCRIBER + sf_type;
        return upper_id * tatp::STARTTIME_PER_FACILITY + start_time;
    }

    ALWAYS_INLINE
    tatp::tatp_sub_number FastGetSubscribeNumFromSubscribeID(uint32_t s_id) const {
        tatp::tatp_sub_number sub_number;
        sub_number.key = 0;
        sub_number.dec_0_1_2 = map_1000[s_id % 1000];
        s_id /= 1000;
        sub_number.dec_3_4_5 = map_1000[s_id % 1000];
        s_id /= 1000;
        sub_number.dec_6_7_8 = map_1000[s_id % 1000];

        return sub_number;
    }

    TATPConfig config;
    /* Map 0--999 to 12b, 4b/digit decimal representation */
    uint16_t* map_1000;
};