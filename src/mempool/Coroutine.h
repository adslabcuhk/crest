// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#define BOOST_COROUTINES_NO_DEPRECATION_WARNING

#include <boost/coroutine/all.hpp>

namespace coro {
using coro_call_t = boost::coroutines::symmetric_coroutine<void>::call_type;

using coro_yield_t = boost::coroutines::symmetric_coroutine<void>::yield_type;

using coro_id_t = int;

// A Coroutine is a collection of Coroutine context implemented in Boost
// coroutine library. We use the symmetric coroutine as it is more readable.
enum class CoroutineStatus {
  kPending = 0,   // Pending for executing
  kWaitPoll = 1,  // Wait for RDMA request complete
  kWaitSleep = 2, // Sleeping
  kRun = 3,
  kFinish = 4,
};

struct Coroutine {
  Coroutine() : is_wait_poll(false) {}

  // Whether this coroutine is waiting for the response of an IO request
  // If true, this coroutine is removed from the active coroutine list.
  // It will be awaken only when the response returns
  bool is_wait_poll;

  CoroutineStatus status;

  // The timepoint this coroutine starts yielding
  uint64_t start_sleep_time;
  uint64_t sleep_dura;

  // My coroutine ID
  coro_id_t coro_id;

  // Registered coroutine function
  coro_call_t func;

  // Use pointer to accelerate yield. Otherwise, one needs a while loop
  // to yield the next coroutine that does not wait for network replies
  Coroutine *prev_coro, *next_coro;
};
}; // namespace coro