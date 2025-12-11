#pragma once

#include <cstdlib>
#include <cstring>

#include "Base/BenchTypes.h"
#include "TATP/TATPConstant.h"

namespace tatp {
union tatp_sub_number {
    struct {
        uint32_t dec_0 : 4;
        uint32_t dec_1 : 4;
        uint32_t dec_2 : 4;
        uint32_t dec_3 : 4;
        uint32_t dec_4 : 4;
        uint32_t dec_5 : 4;
        uint32_t dec_6 : 4;
        uint32_t dec_7 : 4;
        uint32_t dec_8 : 4;
        uint32_t dec_9 : 4;
        uint32_t dec_10 : 4;
        uint32_t dec_11 : 4;
        uint32_t dec_12 : 4;
        uint32_t dec_13 : 4;
        uint32_t dec_14 : 4;
        uint32_t dec_15 : 4;
    };

    struct {
        uint64_t dec_0_1_2 : 12;
        uint64_t dec_3_4_5 : 12;
        uint64_t dec_6_7_8 : 12;
        uint64_t dec_9_10_11 : 12;
        uint64_t unused : 16;
    };
    uint64_t key;
};

static_assert(sizeof(tatp_sub_number) == 8, "Invalid size for TATP_NUMBER");

struct subscriber_value_t {
    tatp_sub_number sub_number;
    char hex[5];
    char bytes[10];
    short bits;
    uint32_t msc_location;
    uint32_t vlr_location;

    subscriber_value_t() = default;
    subscriber_value_t(const subscriber_value_t& s) {
        std::memcpy(this, &s, sizeof(subscriber_value_t));
    }
    subscriber_value_t& operator=(const subscriber_value_t& s) {
        std::memcpy(this, &s, sizeof(subscriber_value_t));
        return *this;
    }
    bool compare(const subscriber_value_t& v) {
        return std::memcmp(this, &v, sizeof(subscriber_value_t)) == 0;
    }
};

struct secondary_subscriber_value_t {
    uint32_t sid;

    secondary_subscriber_value_t() = default;
    secondary_subscriber_value_t(const secondary_subscriber_value_t& s) {
        std::memcpy(this, &s, sizeof(secondary_subscriber_value_t));
    }
    secondary_subscriber_value_t& operator=(const secondary_subscriber_value_t& s) {
        std::memcpy(this, &s, sizeof(secondary_subscriber_value_t));
        return *this;
    }
    bool compare(const secondary_subscriber_value_t& v) {
        return std::memcmp(this, &v, sizeof(secondary_subscriber_value_t)) == 0;
    }
};

struct accessinf_val_t {
    char data1;
    char data2;
    char data3[3];
    char data4[5];
    uint8_t unused[6];

    accessinf_val_t() = default;
    accessinf_val_t(const accessinf_val_t& a) {
        std::memcpy(this, &a, sizeof(accessinf_val_t));
    }
    accessinf_val_t& operator=(const accessinf_val_t& a) {
        std::memcpy(this, &a, sizeof(accessinf_val_t));
        return *this;
    }
    bool compare(const accessinf_val_t& v) {
        return std::memcmp(this, &v, sizeof(accessinf_val_t)) == 0;
    }
};

struct specfac_val_t {
    char is_active;
    char error_cntl;
    char data_a;
    char data_b[5];

    specfac_val_t() = default;
    specfac_val_t(const specfac_val_t& s) {
        std::memcpy(this, &s, sizeof(specfac_val_t));
    }
    specfac_val_t& operator=(const specfac_val_t& s) {
        std::memcpy(this, &s, sizeof(specfac_val_t));
        return *this;
    }
    bool compare(const specfac_val_t& v) {
        return std::memcmp(this, &v, sizeof(specfac_val_t)) == 0;
    }
};

struct callfwd_val_t {
    uint8_t end_time;
    char numberx[15];
    callfwd_val_t() = default;
    callfwd_val_t(const callfwd_val_t& c) {
        std::memcpy(this, &c, sizeof(callfwd_val_t));
    }
    callfwd_val_t& operator=(const callfwd_val_t& c) {
        std::memcpy(this, &c, sizeof(callfwd_val_t));
        return *this;
    }
    bool compare(const callfwd_val_t& v) {
        return std::memcmp(this, &v, sizeof(callfwd_val_t)) == 0;
    }
};

};  // namespace tatp