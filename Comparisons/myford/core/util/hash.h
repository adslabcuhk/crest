// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include "base/common.h"

enum class HashCore : int {
  kDirectFunc = 0,
  kMurmurFunc,
  // A specializd hash function for TPC-C Orderline table, 
  // we wish to make all inserted orderline records within a NewOrder transaction 
  // locate in the same bucket, to save RDMA operations
  kTPCCOrderLineFunc,
};

// 64-bit hash for 64-bit platforms
static ALWAYS_INLINE
uint64_t MurmurHash64A(uint64_t key, unsigned int seed) {
  const uint64_t m = 0xc6a4a7935bd1e995;
  const int r = 47;
  uint64_t h = seed ^ (8 * m);
  const uint64_t* data = &key;
  const uint64_t* end = data + 1;

  while (data != end) {
    uint64_t k = *data++;
    k *= m;
    k ^= k >> r;
    k *= m;
    h ^= k;
    h *= m;
  }

  // const unsigned char* data2 = (const unsigned char*)data;

  // switch (8 & 7) {
  //   case 7:
  //     h ^= uint64_t(data2[6]) << 48;
  //   case 6:
  //     h ^= uint64_t(data2[5]) << 40;
  //   case 5:
  //     h ^= uint64_t(data2[4]) << 32;
  //   case 4:
  //     h ^= uint64_t(data2[3]) << 24;
  //   case 3:
  //     h ^= uint64_t(data2[2]) << 16;
  //   case 2:
  //     h ^= uint64_t(data2[1]) << 8;
  //   case 1:
  //     h ^= uint64_t(data2[0]);
  //     h *= m;
  // };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

static ALWAYS_INLINE
uint64_t MurmurHash64ALen(const char* key, uint32_t len, uint64_t seed) {
  const uint64_t m = 0xc6a4a7935bd1e995;
  const int r = 47;

  uint64_t h = seed ^ (len * m);

  const uint64_t* data = (const uint64_t*)key;
  const uint64_t* end = data + (len / 8);

  while (data != end) {
    uint64_t k = *data++;

    k *= m;
    k ^= k >> r;
    k *= m;

    h ^= k;
    h *= m;
  }

  const unsigned char* data2 = (const unsigned char*)data;

  switch (len & 7) {
    case 7:
      h ^= (uint64_t)((uint64_t)data2[6] << (uint64_t)48);
    case 6:
      h ^= (uint64_t)((uint64_t)data2[5] << (uint64_t)40);
    case 5:
      h ^= (uint64_t)((uint64_t)data2[4] << (uint64_t)32);
    case 4:
      h ^= (uint64_t)((uint64_t)data2[3] << (uint64_t)24);
    case 3:
      h ^= (uint64_t)((uint64_t)data2[2] << (uint64_t)16);
    case 2:
      h ^= (uint64_t)((uint64_t)data2[1] << (uint64_t)8);
    case 1:
      h ^= (uint64_t)((uint64_t)data2[0]);
      h *= m;
  };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

ALWAYS_INLINE
static uint64_t GetHash(itemkey_t key, size_t bucket_num, HashCore hash_core) {
  if (hash_core == HashCore::kDirectFunc) {
    return (key % bucket_num);
  } else if (hash_core == HashCore::kMurmurFunc) {
    return MurmurHash64A(key, 0xdeadbeef) % bucket_num;
  } else if (hash_core == HashCore::kTPCCOrderLineFunc) {
    // return MurmurHash64A(key / 15, 0xdeadbeef) % bucket_num;
    uint64_t o_key = key / 15;
    uint64_t item_id = key - o_key * 15;
    uint64_t group_id = o_key % (bucket_num / 3);
    return group_id * 3 + item_id / 5;
    // return (key / 5) % bucket_num;
  } else {
    return 0;
  }
}
