#pragma once

#include <stdarg.h>  // va_list
#include <stdlib.h>  // free

#include <cstdio>
#include <cstring>
#include <ostream>  // std::ostream
#include <sstream>
#include <string>  // std::string

namespace util {

enum ErrorType {
  kOk = 0,        // No error
  kNetworkError,  // Network error
  kIOError,       // Disk error
  kOperationError,
  kMemError,
  kTxnError,
};

// A status class indicating the results of operations, inspired by
// leveldb::Status and btuil::Status (from brpc). The user may encapsulate
// the error message in this  Status class to indicate the results of
// operations, which is better than just returning the error code.
//
// The encapsulated message is stored in an std::string, which may harm
// the performance severely. The user can choose to disable this error
// message by setting the macro "STATUS_ENABLE_ERROR_MSG" to be 0, e.g.,
// setting the variable to be 0 in CMakeLists.txt
//
// A Status object is supposed to be immutable once created. So there is
// no synchronization problem towards a single Status object.
//
class Status {
// Enabling the error messages may slow down the system performance
#define STATUS_ENABLE_ERROR_MSG 1
#if STATUS_ENABLE_ERROR_MSG
  static constexpr bool enable_err_msg = true;
#else
  static constexpr bool enable_err_msg = false;
#endif

  static const char *error_code_str(ErrorType code) {
    switch (code) {
      case kOk:
        return "Ok";
      case kNetworkError:
        return "NetworkError";
      case kOperationError:
        return "OperationError";
      default:
        return "Not Supported Error Code";
    }
  }

 public:
  Status() : error_code_(kOk), err_no_(0), err_msg_() {}
  // Create a succ status, which is frequently used
  static Status OK() { return Status(); }

  Status(const Status &) = default;
  Status &operator=(const Status &) = default;
  ~Status() = default;

  // Create a failed status with formatted error messages
  Status(ErrorType code, int err_no, const char *fmt, ...)
      : error_code_(code), err_no_(err_no), err_msg_() {
    if (enable_err_msg) {
      va_list args;
      va_start(args, fmt);
      // calculate the size needed to store the formatted string, the extra
      // 1 is for the terminator '\0'
      auto sz = std::vsnprintf(nullptr, 0, fmt, args);
      va_end(args);
      err_msg_.resize(sz);
      // format the string:
      va_start(args, fmt);
      std::vsnprintf(const_cast<char *>(err_msg_.c_str()), sz + 1, fmt, args);
      va_end(args);
    }
  }

  Status(ErrorType code, int err_no, const std::string &msg)
      : error_code_(code), err_no_(err_no), err_msg_(msg) {}

  // Some operations do not invoke syscall and the err_no_ can be simply set
  // to be 0. The err_msg_ is an optional parameter.
  Status(ErrorType code, const char *fmt, ...) : Status(code, 0, fmt) {}

  // No error message output
  Status(ErrorType type, int err_no) : error_code_(type), err_no_(err_no), err_msg_() {}

  // fetchers
  bool ok() const { return error_code_ == kOk; }

  auto error_code() const { return error_code_; }

  int error_no() const { return err_no_; }

  const std::string &error_msg() const { return err_msg_; }

  // wrappers:
  bool IsNetworkError() const { return error_code_ == kNetworkError; }

  // debug information
  // this function is not recommended to be used
  std::string to_string() const {
    std::stringstream ss;
    ss << "[" << error_code_str(error_code_) << "]"
       << "[" << std::strerror(err_no_) << "]"
       << "[" << err_msg_.c_str() << "]";
    return ss.str();
  }

  std::ostream &operator<<(std::ostream &os) const { return os << this->to_string(); }

 private:
  ErrorType error_code_;
  int err_no_;           // errno from the syscall
  std::string err_msg_;  // error message if necessary
};

};  // namespace util