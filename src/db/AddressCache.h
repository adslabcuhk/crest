#pragma once

#include <absl/container/flat_hash_map.h>

#include "common/Type.h"

class RecordHandle;
// AddressCache is per-thread per-table structs maintaining the PoolAddress 
// of specific records. Currently we haven't implemented the evict function 
// yet. 
class AddressCache {
  using UnorderedMap = absl::flat_hash_map<RecordKey, PoolPtr>;

public:
  AddressCache() : address_cache_() {}
  ~AddressCache() {}

  bool Insert(RecordKey row_key, PoolPtr ptr);

  bool Delete(RecordKey row_key);

  PoolPtr Search(RecordKey row_key);

  size_t Size() const;

private:
  UnorderedMap address_cache_;
};

class HandleCache {
  using UnorderedMap = absl::flat_hash_map<RecordKey, RecordHandle*>;
public:
  HandleCache() : handle_cache_() {
    handle_cache_.reserve(100000);
  }
  ~HandleCache() {}

  bool Insert(RecordKey row_key, RecordHandle*);

  RecordHandle* Search(RecordKey row_key);

  size_t Size() const { return handle_cache_.size(); }

private:
  UnorderedMap handle_cache_;
};

