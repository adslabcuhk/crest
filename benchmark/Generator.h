#pragma once
#include <cassert>
#include <cmath>
#include <cstdint>
#include <string>

#include "util/Macros.h"

// not thread-safe
//
// taken from java:
//   http://developer.classpath.org/doc/java/util/Random-source.html
class FastRandom {
   public:
    FastRandom(unsigned long sed) : seed(0) { SetSeed0(sed); }

    FastRandom() : seed(0) { SetSeed0(seed); }

    inline unsigned long Next() { return ((unsigned long)Next(32) << 32) + Next(32); }

    inline uint32_t NextU32() { return Next(32); }

    inline uint16_t NextU16() { return Next(16); }

    /** [0.0, 1.0) */
    inline double NextUniform() {
        return (((unsigned long)Next(26) << 27) + Next(27)) / (double)(1L << 53);
    }

    inline char NextChar() { return Next(8) % 256; }

    inline std::string NextString(size_t len) {
        std::string s(len, 0);
        for (size_t i = 0; i < len; i++) s[i] = NextChar();
        return s;
    }

    inline unsigned long GetSeed() { return seed; }

    inline void SetSeed(unsigned long sed) { this->seed = sed; }

    inline void SetSeed0(unsigned long sed) {
        this->seed = (sed ^ 0x5DEECE66DL) & ((1L << 48) - 1);
    }

    inline uint64_t RandNumber(int min, int max) {
        return CheckBetweenInclusive((uint64_t)(NextUniform() * (max - min + 1) + min), min, max);
    }

    inline uint64_t CheckBetweenInclusive(uint64_t v, uint64_t min, uint64_t max) {
        assert(v >= min);
        assert(v <= max);
        return v;
    }

   private:
    inline unsigned long Next(unsigned int bits) {
        seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
        return (unsigned long)(seed >> (48 - bits));
    }

    unsigned long seed;
};

class ZipfGenerator {
   public:
    ZipfGenerator(const uint64_t& n, const double& theta, uint64_t seed = 0) : rand_generator(seed) {
        // range: 1-n
        the_n = n;
        zipf_theta = theta;
        zeta_2_theta = zeta(2, zipf_theta);
        denom = zeta(the_n, zipf_theta);
    }

    double zeta(uint64_t n, double theta) {
        double sum = 0;
        for (uint64_t i = 1; i <= n; i++) sum += std::pow(1.0 / i, theta);
        return sum;
    }

    int GenerateInteger(const int& min, const int& max) {
        return rand_generator.Next() % (max - min + 1) + min;
    }

    uint64_t GetNextNumber() {
        double alpha = 1 / (1 - zipf_theta);
        double zetan = denom;
        double eta = (1 - std::pow(2.0 / the_n, 1 - zipf_theta)) / (1 - zeta_2_theta / zetan);
        double u = (double)(GenerateInteger(1, 10000000) % 10000000) / 10000000;
        double uz = u * zetan;
        if (uz < 1) return 1;
        if (uz < 1 + std::pow(0.5, zipf_theta)) return 2;
        return 1 + (uint64_t)(the_n * std::pow(eta * u - eta + 1, alpha));
    }

    uint64_t the_n;
    double zipf_theta;
    double denom;
    double zeta_2_theta;
    FastRandom rand_generator;
};

ALWAYS_INLINE
uint32_t FastRand(uint64_t* seed) {
    *seed = *seed * 1103515245 + 12345;
    return (uint32_t)(*seed >> 32);
}
