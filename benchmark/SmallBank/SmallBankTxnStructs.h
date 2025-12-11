#pragma once

#include <cstdint>

#include "Base/BenchTypes.h"
#include "SmallBank/SmallBankConstant.h"
#include "SmallBank/SmallBankContext.h"
#include "SmallBank/SmallBankTableStructs.h"
#include "common/Type.h"

namespace smallbank {
class AmalgamateParam : public TxnParam {
 public:
  AmalgamateParam() : TxnParam(smallbank::kAmalgamate) {}

  char* Serialize(char* dst) const override { return dst; }

  const char* Deserialize(const char* src) override { return src; }

 public:
  int64_t custid_0_;
  int64_t custid_1_;
};

class WriteCheckParam : public TxnParam {
 public:
  WriteCheckParam() : TxnParam(smallbank::kWriteCheck) {}

  char* Serialize(char* dst) const override { return dst; }

  const char* Deserialize(const char* src) override { return src; }

 public:
  int64_t custid_;
  double amount_;
};

class TransactSavingParam : public TxnParam {
 public:
  TransactSavingParam() : TxnParam(smallbank::kTransactSaving) {}

  char* Serialize(char* dst) const override { return dst; }

  const char* Deserialize(const char* src) override { return src; }

 public:
  int64_t custid_;
  double amount_;
};

class SendPaymentParam : public TxnParam {
 public:
  SendPaymentParam() : TxnParam(smallbank::kSendPayment) {}

  char* Serialize(char* dst) const override { return dst; }

  const char* Deserialize(const char* src) override { return src; }

 public:
  int64_t custid_0_;
  int64_t custid_1_;
  double amount_;
};

class DepositCheckingParam : public TxnParam {
 public:
  DepositCheckingParam() : TxnParam(smallbank::kDepositChecking) {}

  char* Serialize(char* dst) const override { return dst; }

  const char* Deserialize(const char* src) override { return src; }

 public:
  int64_t custid_;
  double amount_;
};

class BalanceParam : public TxnParam {
 public:
  BalanceParam() : TxnParam(smallbank::kBalance) {}

  char* Serialize(char* dst) const override { return dst; }

  const char* Deserialize(const char* src) override { return src; }

 public:
  int64_t custid_;
};

// Transaction execute results
class AmalgamateResult : public TxnResult {
 public:
  AmalgamateResult() : TxnResult(smallbank::kAmalgamate) {}

  char* Serialize(char* dst) const override { return dst; }

  const char* Deserialize(const char* src) override { return src; }
};

class WriteCheckResult : public TxnResult {
 public:
  WriteCheckResult() : TxnResult(smallbank::kWriteCheck) {}

  char* Serialize(char* dst) const override { return dst; }

  const char* Deserialize(const char* src) override { return src; }

 public:
  double saving_balance;
  double checking_balace;
};

class TransactSavingResult : public TxnResult {
 public:
  TransactSavingResult() : TxnResult(smallbank::kTransactSaving) {}

  char* Serialize(char* dst) const override { return dst; }

  const char* Deserialize(const char* src) override { return src; }
 public:
  double saving_balance;
};

class SendPaymentResult : public TxnResult {
 public:
  SendPaymentResult() : TxnResult(smallbank::kSendPayment) {}

  char* Serialize(char* dst) const override { return dst; }

  const char* Deserialize(const char* src) override { return src; }
};

class DepositCheckingResult : public TxnResult {
 public:
  DepositCheckingResult() : TxnResult(smallbank::kDepositChecking) {}

  char* Serialize(char* dst) const override { return dst; }

  const char* Deserialize(const char* src) override { return src; }
};

class BalanceResult : public TxnResult {
 public:
  BalanceResult() : TxnResult(smallbank::kBalance) {}

  char* Serialize(char* dst) const override { return dst; }

  const char* Deserialize(const char* src) override { return src; }
};

}  // namespace smallbank
