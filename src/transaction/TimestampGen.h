#pragma once
#include <atomic>

#include "common/Type.h"

class TimestampGenerator {
 public:
  virtual timestamp_t AcquireTimestamp() = 0;
};

class TimestampGeneratorImpl : public TimestampGenerator {
 public:
  TimestampGeneratorImpl(uint64_t start_ts) : ts_(start_ts) {}
  timestamp_t AcquireTimestamp() override { return ts_.fetch_add(1); }

  std::atomic<uint64_t> ts_;
};
