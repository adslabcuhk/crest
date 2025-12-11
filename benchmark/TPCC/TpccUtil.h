#pragma once
#include <cassert>
#include <chrono>

#include "Generator.h"
#include "util/Macros.h"

// The base timepoint for "GetCurrentTimeMillis". The
// "time_since_epoch" function returns the elapse time since 1970.1.1, we need to
// cast the start time point to make these values fit within a 36bit value
// Remember to set this value when preparing the workloads
// uint64_t currentTimeBase;

extern uint64_t current_time;

ALWAYS_INLINE
uint64_t GetCurrentTimeMillis() {
    // implement a scalable GetCurrentTimeMillis()
    // for now, we just give each core an increasing number
    return current_time++;
}

ALWAYS_INLINE
void ResetCurrentTime(uint64_t t) { current_time = t; }

// utils for generating random #s and strings
ALWAYS_INLINE
int CheckBetweenInclusive(int v, int lower, int upper) {
    assert(v >= lower);
    assert(v <= upper);
    return v;
}

ALWAYS_INLINE
int RandomNumber(FastRandom& r, int min, int max) {
    return CheckBetweenInclusive((int)(r.NextUniform() * (max - min) + min), min, max);
}

ALWAYS_INLINE
int NonUniformRandom(FastRandom& r, int A, int C, int min, int max) {
    return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) % (max - min + 1)) + min;
}

// pick a number between [start, end)
ALWAYS_INLINE
unsigned PickWarehouseId(FastRandom& r, unsigned start, unsigned end) {
    assert(start < end);
    const unsigned diff = end - start;
    if (diff == 1) return start;
    return (r.Next() % diff) + start;
}

ALWAYS_INLINE
int GetCustomerId(FastRandom& r, int num_customer_per_district) {
    return CheckBetweenInclusive(NonUniformRandom(r, 1023, 259, 0, num_customer_per_district - 1),
                                 0, num_customer_per_district - 1);
}

ALWAYS_INLINE
int64_t GetItemId(FastRandom& r, bool uniform_item_dist, int num_item) {
    return CheckBetweenInclusive(uniform_item_dist
                                     ? RandomNumber(r, 0, num_item - 1)
                                     : NonUniformRandom(r, 8191, 7911, 0, num_item - 1),
                                 0, num_item - 1);
}

ALWAYS_INLINE
std::string RandomStr(FastRandom& r, uint64_t len) {
    // this is a property of the oltpbench implementation...
    if (!len) return "";

    uint64_t i = 0;
    std::string buf(len, 0);
    while (i < (len)) {
        const char c = (char)r.NextChar();
        // oltpbench uses java's Character.isLetter(), which
        // is a less restrictive filter than isalnum()
        if (!isalnum(c)) continue;
        buf[i++] = c;
    }
    return buf;
}

ALWAYS_INLINE
char* RandomStr(FastRandom& r, uint64_t len, char* buf) {
    // this is a property of the oltpbench implementation...
    if (!len || !buf) return buf;

    uint64_t i = 0;
    while (i < (len)) {
        const char c = (char)r.NextChar();
        // oltpbench uses java's Character.isLetter(), which
        // is a less restrictive filter than isalnum()
        if (!isalnum(c)) continue;
        buf[i++] = c;
    }
    return buf + i;
}

// RandomNStr() actually produces a string of length len
ALWAYS_INLINE
std::string RandomNStr(FastRandom& r, uint64_t len) {
    const char base = '0';
    std::string buf(len, 0);
    for (uint64_t i = 0; i < len; i++) buf[i] = (char)(base + (r.Next() % 10));
    return buf;
}