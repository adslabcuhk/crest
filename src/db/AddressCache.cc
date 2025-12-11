#include "db/AddressCache.h"
#include "common/Type.h"

bool AddressCache::Insert(RecordKey rkey, PoolPtr ptr) {
  address_cache_.insert_or_assign(rkey, ptr);
  return true;
}

PoolPtr AddressCache::Search(RecordKey row_key) {
  auto it = address_cache_.find(row_key);
  if (it != address_cache_.end()) {
    return it->second;
  }
  return POOL_PTR_NULL;
}

bool AddressCache::Delete(RecordKey row_key) {
  auto it = address_cache_.find(row_key);
  if (it != address_cache_.end()) {
    address_cache_.erase(it);
    return true;
  }
  return false;
}

bool HandleCache::Insert(RecordKey rkey, RecordHandle* hdl) {
  handle_cache_.insert_or_assign(rkey, hdl);
  return true;
}

RecordHandle* HandleCache::Search(RecordKey rkey) {
  auto it = handle_cache_.find(rkey);
  if (it != handle_cache_.end()) {
    return it->second;
  }
  return nullptr;
}
