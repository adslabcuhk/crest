#pragma once

#include <stdarg.h>  // va_list

#include <chrono>
#include <cstdio>
#include <iomanip>
#include <iostream>
#include <ostream>  // std::ostream
#include <sstream>
#include <string>  // std::string

#include "util/Status.h"
#include "util/Timer.h"

namespace util {

#ifndef UTIL_LOG_LEVEL
#define UTIL_LOG_LEVEL util::INFO
#endif

enum LogLevel {
  EVERYTHING = 0,  // Print log information of all level
  DEBUG = 1,       // Debug information for development
  INFO = 2,        // Normal information when no exception occurs
  WARNNING = 3,    // Warning information
  ERROR = 4,       // information when some errors occurs
  FATAL = 5,       // Abort the current process after printing this log
};

// The color of information when printing messages, to better distinguish
// the information of different groups
enum LogColor {
  kTrailing = 0,  // This is necessary
  kNone = 1,
  kGreen = 2,
  kYellow = 3,
  kRed = 4,
  kBlue = 5,
};

static const char *color_to_ascii_str(LogColor color) {
  switch (color) {
    case kTrailing:
      return "\x1b[0m";
    case kGreen:
      return "\x1b[32m";
    case kYellow:
      return "\x1b[33m";
    case kRed:
      return "\x1b[31m";
    case kBlue:
      return "\x1b[34m";
    case kNone:
    default:
      return "";
  }
}

// A simple logger struct to print system-generated information. The user
// can choose the color to print the information when initializing the Logger
// object. Since log information may interfere with the system and causes
// performance degradation, the user can choose to disable the normal
// information logging and only allows printing error infors
//
// The user must specify a destination (a std::ostream) to write the log
// information, it would be std::cout by default.
class Logger {
  using TimePoint = decltype(std::chrono::steady_clock::now());

  // The number of space as separation between the time and the message
  static constexpr size_t kTimeMsgSep = 4;

  // Let the user decide which level to use for this logger
  static constexpr LogLevel EnabledLogLevel = LogLevel::INFO;

 public:
  Logger(LogColor color, std::ostream &os = std::cout) : os_(os), err_(false), timer_() {
    timer_.reset();
  }

  // Write the formated message to the logger and return the execution status.
  // If any error occurs during this process, the error code of this logger is
  // set and can not be used.
  Status WriteMessage(const char *file, int line, LogColor color, const char *fmt, ...) {
#if UTIL_DISABLE_LOGGER
    return Status::OK();
#else
    if (is_error()) [[unlikely]] {
      // Maybe handle the error
      Status s = HandleWriteStreamError();
      if (!s.ok()) [[unlikely]] {
        return s;
      }
    }

    static const int kPreBufSize = 256;
    char prebuf[kPreBufSize];
    char *write_buf = prebuf;

    va_list args;
    va_start(args, fmt);
    auto sz = std::vsnprintf(nullptr, 0, fmt, args);
    va_end(args);

    if (sz + 1 > kPreBufSize) {
      write_buf = new char[sz + 1];
    }

    va_start(args, fmt);
    std::vsnprintf(write_buf, sz + 1, fmt, args);
    va_end(args);

    std::string fpath = std::string(file);
    // drop the long directory path:
    auto it = fpath.rfind("/");
    if (it != std::string::npos) {
      fpath = fpath.substr(it + 1, std::string::npos);
    }

    // Per 100 ns
    std::stringstream ss;
    int64_t elapse = timer_.ns_elapse() / 100;
    ss << "[" << std::setw(10) << elapse << "]";
    ss << "[" << fpath << ":" << std::to_string(line) << "]";
    std::string hdr = ss.str();

    os_ << color_to_ascii_str(color) << hdr << " " << write_buf << color_to_ascii_str(kTrailing)
        << std::endl;

    // delete allocated buffer if necessary
    if (write_buf != prebuf) {
      delete write_buf;
    }

    if (os_.fail() || os_.bad()) {
      err_ = true;
      return Status(kNetworkError, "Logger WriteMessage failed when write into os");
    }

    return Status::OK();
#endif
  }

  // Log with default color
  Status WriteMessage(const char *file, int line, const char *fmt, ...) {
    return WriteMessage(file, line, color_, fmt);
  }

  // Handle the possible error state of the underlying ostream.
  Status HandleWriteStreamError() {
    os_.clear();
    os_.seekp(0);
    err_ = false;
    return Status::OK();
  }

  bool is_error() const { return err_; }

 private:
  std::ostream &os_;  // The stream to write
  bool err_;          // The log might be set in error state
  LogColor color_;    // default color for this logger stream
  util::Timer timer_;
};

inline Logger *LoggerInstance() {
  static Logger logger(kGreen);
  return &logger;
}

};  // namespace util

// Macros for loggers
#define LOG_DEBUG(fmt, ...)                                                            \
  if (UTIL_LOG_LEVEL <= util::DEBUG) {                                                 \
    util::LoggerInstance()->WriteMessage((char *)__FILE__, __LINE__, util::kBlue, fmt, \
                                         ##__VA_ARGS__);                               \
  }

#define LOG_INFO(fmt, ...)                                                              \
  if (UTIL_LOG_LEVEL <= util::INFO) {                                                   \
    util::LoggerInstance()->WriteMessage((char *)__FILE__, __LINE__, util::kGreen, fmt, \
                                         ##__VA_ARGS__);                                \
  }

#define LOG_WARN(fmt, ...)                                                               \
  if (UTIL_LOG_LEVEL <= util::WARNNING) {                                                \
    util::LoggerInstance()->WriteMessage((char *)__FILE__, __LINE__, util::kYellow, fmt, \
                                         ##__VA_ARGS__);                                 \
  }

#define LOG_ERROR(fmt, ...)                                                           \
  if (UTIL_LOG_LEVEL <= util::ERROR) {                                                \
    util::LoggerInstance()->WriteMessage((char *)__FILE__, __LINE__, util::kRed, fmt, \
                                         ##__VA_ARGS__);                              \
  }

#define LOG_FATAL(fmt, ...)                                                           \
  if (UTIL_LOG_LEVEL <= util::FATAL) {                                                \
    util::LoggerInstance()->WriteMessage((char *)__FILE__, __LINE__, util::kRed, fmt, \
                                         ##__VA_ARGS__);                              \
    abort();                                                                          \
  }
