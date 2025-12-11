#pragma once

#include <cstdlib>
#include <cstring>

#include "Base/BenchTypes.h"
#include "YCSB/YCSBConstant.h"

namespace ycsb {
struct ycsb_value_t {
    char data[YCSB_COLUMN_SIZE][YCSB_COLUMN_NUM];

    ycsb_value_t() = default;
    ycsb_value_t(const ycsb_value_t& v) { std::memcpy(this, &v, sizeof(ycsb_value_t)); }
    ycsb_value_t& operator=(const ycsb_value_t& v) {
        std::memcpy(this, &v, sizeof(ycsb_value_t));
        return *this;
    }

    bool compare(const ycsb_value_t& v) {
        for (size_t i = 0; i < YCSB_COLUMN_NUM; i++) {
            if (memcmp(data[i], v.data[i], YCSB_COLUMN_SIZE) != 0) {
                return false;
            }
        }
        return true;
    }
};
};  // namespace ycsb