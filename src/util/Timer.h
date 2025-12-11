#pragma once

#include <bits/types/struct_timeval.h>
#include <sys/time.h>

#include <chrono>

namespace util {

// Record time duration
class Timer {
 public:
  using TimePoint = decltype(std::chrono::steady_clock::now());

  static TimePoint NowTime() { return std::chrono::steady_clock::now(); }

  Timer() { reset(); }
  Timer(const Timer &) = default;
  Timer &operator=(const Timer &) = default;

  // Reset the start timepoint
  void reset() { tp_ = std::chrono::steady_clock::now(); }

  // Calculate the elapsed time since last reset
  double ns_elapse() const {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(NowTime() - tp_).count();
  }

  double us_elapse() const { return ns_elapse() / 1000.0; }

  double ms_elapse() const { return us_elapse() / 1000.0; }

  double s_elapse() const { return ms_elapse() / 1000.0; }

 private:
  TimePoint tp_;
};

// Wrapper function
inline uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return tv.tv_usec + tv.tv_sec * 1000000;
}

// Even though CPU cycle does not give a precise human-readable time representation, 
// it helps in identifying the relative time duration 
inline uint64_t GetCPUCycle() {
  unsigned a, d;
  __asm __volatile("rdtsc" : "=a"(a), "=d"(d));
  return ((unsigned long)a) | (((unsigned long)d) << 32);
}

};  // namespace util