#pragma arguments

#include <cstdlib>
#include <cstring>

#include "Base/BenchTypes.h"
#include "SmallBank/SmallBankConstant.h"

namespace smallbank {
struct accounts_value_t {
  uint64_t cust_id;
  char name[ACCOUNTS_MAX_NAME_SIZE + 1];

  accounts_value_t() = default;
  accounts_value_t(const accounts_value_t& a) { std::memcpy(this, &a, sizeof(accounts_value_t)); }
  accounts_value_t& operator=(const accounts_value_t& a) {
    std::memcpy(this, &a, sizeof(accounts_value_t));
    return *this;
  }

  bool compare(const accounts_value_t& a) {
    return cust_id == a.cust_id && strcmp(name, a.name) == 0;
  }
};

struct savings_value_t {
  uint64_t cust_id;
  double bal;

  bool compare(const savings_value_t& s) { return cust_id == s.cust_id && bal == s.bal; }
};

struct checkings_value_t {
  uint64_t cust_id;
  double bal;

  bool compare(const checkings_value_t& c) { return cust_id == c.cust_id && bal == c.bal; }
};
};  // namespace smallbank