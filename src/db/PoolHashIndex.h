#pragma once

#include <cstdint>
#include <functional>

#include "common/Type.h"
#include "util/Hash.h"
#include "util/Macros.h"

enum HashType {
  kDirect = 0,  // Use the key iteself as the hash value
  kCityHash,    // Use google city hash to get the value
  kCustomized,  // Customized hash function
};

struct IndexSlot {
  BucketId bkt_id;
  SlotId s_id;

  IndexSlot(BucketId bkt_id, SlotId s_id) : bkt_id(bkt_id), s_id(s_id) {}
};

// A HashIndex consists of multiple static hash buckets, each of
// which is comprised of a few data rows. HashIndex assumes all
// buckets are placed as a continuous array. For indexing, it only
// returns the offset of the target row.
struct PoolHashIndex {
  struct Config {
    uint32_t records_per_bucket_;
    uint32_t bucket_num_;
    HashType hash_type_;
    // The hasher function maps a key to specific bucket id
  std::function<uint64_t(RecordKey)> hasher_;
  } config_;

  static Config DefaultConfig() {
    return Config{0, 0, kDirect, [](RecordKey rkey) { return 0; }};
  }

  PoolHashIndex(const Config& config) : config_(config) {}

  size_t RecordsPerBucket() const { return config_.records_per_bucket_; }

  // Get the bucket number
  BucketId LocateBucketId(RecordKey key) {
    if (config_.hash_type_ == kDirect) {
      return (uint64_t)key % config_.bucket_num_;
    } else if (config_.hash_type_ == kCityHash) {
      return util::Hash(key) % config_.bucket_num_;
    } else if (config_.hash_type_ == kCustomized) {
      return config_.hasher_(key);
    }
    return 0;
  }
};