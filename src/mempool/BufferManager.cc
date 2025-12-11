#include "mempool/BufferManager.h"

char *BufferManager::Alloc(size_t sz) {
  // A CircularAllocator allocates memory in an Append-Only manner.
  // It simply falls back to the start position of this managed memory region
  // when the "allocation tail" reaches the end of this region. Such allocation
  // approach allows the newer writer uses the memory allocated before just
  // like a circular buffer.
  if (LeftSize() < sz) [[unlikely]] {
    curr_ = start_;
  }
  curr_ += sz;
  return (char *)(curr_ - sz);
}

char *BufferManager::AllocAlign(size_t sz, size_t alignment) {
  curr_ = ((curr_ - 1) / alignment + 1) * alignment;
  // The aligned address is across the boundary, or the left size is not enough
  if (curr_ >= end_ || sz > LeftSize()) {
    curr_ = ((start_ - 1) / alignment + 1) * alignment;
  }
  if (sz > LeftSize()) {
    return nullptr;
  }
  curr_ += sz;
  return (char *)(curr_ - sz);
}
