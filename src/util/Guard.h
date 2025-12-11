#pragma once

#include "util/Timer.h"

namespace util {

// A Guard class used to help delaying some user-specified operations
// e.g., One can use this Guard struct to delay the release of memory
// allocated from the heap when exiting some functions, which is helpful
// to prevent the memory-leak.
//
// The user must specify the callback function when creating a Guard
// object. The callback must have no argument and return value. One
// can use lambda capture feature to store the context needed to
// execute the callback function.
template <typename CallBack>
class Guard {
 public:
  Guard(CallBack cb) : cb_(cb) {}
  ~Guard() { cb_(); }

 private:
  CallBack cb_;
};

// This is basically the same as Guard but the callback must takes a
// timer argument. This class is used for time recording, i.e., sampling
// the execution time of a scope.
template <typename CallBack>
class LatencyGuard {
 public:
  LatencyGuard(CallBack cb) : cb_(cb), timer_() { timer_.reset(); }
  ~LatencyGuard() { cb_(&timer_); }

 private:
  CallBack cb_;
  Timer timer_;
};

};  // namespace util