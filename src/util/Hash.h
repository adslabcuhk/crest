#pragma once

#ifdef ENABLE_CITY_HASH
#include <city.h>
#endif
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <string>

#undef get16bits
#if (defined(__GNUC__) && defined(__i386__)) || defined(__WATCOMC__) || defined(_MSC_VER) || \
    defined(__BORLANDC__) || defined(__TURBOC__)
#define get16bits(d) (*((const uint16_t *)(d)))
#endif

#if !defined(get16bits)
#define get16bits(d) \
  ((((uint32_t)(((const uint8_t *)(d))[1])) << 8) + (uint32_t)(((const uint8_t *)(d))[0]))
#endif

inline uint32_t SuperFastHash(const char *data, int len) {
  uint32_t hash = len, tmp;
  int rem;

  if (len <= 0 || data == NULL) return 0;

  rem = len & 3;
  len >>= 2;

  /* Main loop */
  for (; len > 0; len--) {
    hash += get16bits(data);
    tmp = (get16bits(data + 2) << 11) ^ hash;
    hash = (hash << 16) ^ tmp;
    data += 2 * sizeof(uint16_t);
    hash += hash >> 11;
  }

  /* Handle end cases */
  switch (rem) {
    case 3:
      hash += get16bits(data);
      hash ^= hash << 16;
      hash ^= ((signed char)data[sizeof(uint16_t)]) << 18;
      hash += hash >> 11;
      break;
    case 2:
      hash += get16bits(data);
      hash ^= hash << 11;
      hash += hash >> 17;
      break;
    case 1:
      hash += (signed char)*data;
      hash ^= hash << 10;
      hash += hash >> 1;
  }

  /* Force "avalanching" of final 127 bits */
  hash ^= hash << 3;
  hash += hash >> 5;
  hash ^= hash << 4;
  hash += hash >> 17;
  hash ^= hash << 25;
  hash += hash >> 6;

  return hash;
}

namespace util {
// Compute the hash value of a continous memory buffer specified by (data,
// length)
inline uint64_t Hash(const char *data, size_t length) {
#ifdef ENABLE_CITY_HASH
  return CityHash64(data, length);
#else
  return SuperFastHash(data, length);
#endif
}

// Get the hash value with user-specific input seeds
inline uint64_t Hash(const char *data, size_t length, uint64_t seed) {
#ifdef ENABLE_CITY_HASH
  return CityHash64WithSeed(data, length, seed);
#else
  (void)seed;
  return SuperFastHash(data, length);
#endif
}

// Wrappers for some basic data types
inline uint64_t Hash(int num) { return Hash((const char *)&num, sizeof(num)); }

inline uint64_t Hash(int64_t num) { return Hash((const char *)&num, sizeof(num)); }

inline uint64_t Hash(uint64_t num) { return Hash((const char *)&num, sizeof(num)); }

inline uint64_t Hash(double num) { return Hash((const char *)&num, sizeof(num)); }

inline uint64_t Hash(std::string str) { return Hash(str.c_str(), str.size()); }

};  // namespace util