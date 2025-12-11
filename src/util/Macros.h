#pragma once

#include <atomic>

#include "util/Logger.h"

#define CACHELINE_SIZE 64  // XXX: don't assume x86
#define LG_CACHELINE_SIZE __builtin_ctz(CACHELINE_SIZE)

#define CACHE_ALIGNED __attribute__((aligned(CACHELINE_SIZE)))
#define PACKED __attribute__((packed))

#define NEVER_INLINE __attribute__((noinline))
#define ALWAYS_INLINE inline __attribute__((always_inline))

#define ASSERT(expr, fmt, ...)                                                       \
  if (unlikely(!(expr))) {                                                           \
    util::LoggerInstance()->WriteMessage((char*)__FILE__, __LINE__, util::kRed, fmt, \
                                         ##__VA_ARGS__);                             \
    abort();                                                                         \
  }

#define CHECK(expr)        \
  if (unlikely(!(expr))) { \
    abort();               \
  }

#ifdef NDEBUG
#define ALWAYS_ASSERT(expr) (likely((expr)) ? (void)0 : abort())
#else
#define ALWAYS_ASSERT(expr) assert((expr))
#define CHECK_INVARIANTS 1
#endif /* NDEBUG */

#if CHECK_INVARIANTS
#define INVARIANT(expr) ALWAYS_ASSERT(expr)
#else
#define INVARIANT(expr) ((void)0)
#endif /* CHECK_INVARIANTS */

// Return the number of trailing zeros for a given number
#define TRAILING_ZEROS(N) __builtin_ctz((N))

#define ALIGNED(N) __attribute__((aligned(N)))

// Branch prediction hint
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#define TXN_INTERNAL_PROFILE 1

template <typename T>
inline void ATOMIC_STORE(T* dst, T v) {
  __atomic_store(dst, &v, __ATOMIC_RELEASE);
}

template <typename T>
inline T ATOMIC_LOAD(const T* src) {
  T v;
  __atomic_load(src, &v, __ATOMIC_ACQUIRE);
  return v;
}

template <typename T>
inline bool ATOMIC_COMPARE_AND_SWAP(T* dst, T old_v, T new_v) {
  T v = __sync_val_compare_and_swap(dst, old_v, new_v, __ATOMIC_SEQ_CST);
  return v == old_v;
}

// clz return the number of consecutive LEADING 0s of a 64-bit integer
inline int clz64(uint64_t a) { return __builtin_clzll(a); }

// clo return the number of consecutive LEADING 1s of a 64-bit integer
inline int clo64(uint64_t a) { return clz64(~a); }

#define DEBUG_LOCALIZATION 0

#define DEBUG_NEWORDER 0

#define DEBUG_PAYMENT 0

#define RDMA_PERF 1

#define DEBUG_LOCALIZATION_PERF 1
