#pragma once

#include <cstdint>
#include <cstdlib>

// BufferManager manages the memory allocation works of registerred RDMA
// memory region. Currently we just implement a simple circular-log allocator
class BufferManager {
 public:
  BufferManager(char *start, size_t sz)
      : start_(uint64_t(start)), end_(start_ + sz), curr_(start_), size_(sz) {}

  char *Alloc(size_t sz);

  char *AllocAlign(size_t sz, size_t aligment);

  void Release(char *d, size_t sz) {
    // A CirtuclarAllocator does not support the memory release operations
    return;
  }

  char *Tail() { return Alloc(0); }

  size_t Offset() { return curr_ - start_; }

 private:
  size_t LeftSize() const { return size_ - (curr_ - start_); }

 private:
  uint64_t start_, end_, curr_;
  uint64_t size_;
};