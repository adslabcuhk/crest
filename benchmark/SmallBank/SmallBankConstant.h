#pragma once

#include <string>

#include "Base/BenchTypes.h"
#include "common/Type.h"

namespace smallbank {

constexpr size_t SMALLBANK_TABLE_NUM = 3;
constexpr size_t SMALLBANK_TXN_NUM = 6;

// Tables definition:
constexpr TableId ACCOUNTS_TABLE = 0;
constexpr TableId SAVINGS_TABLE = 1;
constexpr TableId CHECKING_TABLE = 2;

// Columns definition:
// ACCOUNTS Table:
const ColumnId ACCOUNTS_CUST_ID = 0;
const ColumnId ACCOUNTS_NAME = 1;

// SAVIINGS Table:
const ColumnId SAVINGS_CUST_ID = 0;
const ColumnId SAVINGS_BALANCE = 1;

// Checkings Table:
const ColumnId CHECKINGS_CUST_ID = 0;
const ColumnId CHECKINGS_BALANCE = 1;

constexpr size_t ACCOUNTS_MAX_NAME_SIZE = 64;

const BenchTxnType kAmalgamate = 0;
const BenchTxnType kWriteCheck = 1;
const BenchTxnType kTransactSaving = 2;
const BenchTxnType kSendPayment = 3;
const BenchTxnType kDepositChecking = 4;
const BenchTxnType kBalance = 5;
const BenchTxnType kSmallBankTransactionTypeMax = 6;

constexpr char accounts_table_name[] = "Accounts ";

constexpr char savings_table_name[] = "Savings  ";

constexpr char checkings_table_name[] = "Checkings  ";

const int FREQUENCY_AMALGAMATE = 20;
const int FREQUENCY_WRITE_CHECK = 20;
const int FREQUENCY_TRANSACT_SAVINGS = 20;
const int FREQUENCY_SEND_PAYMENT = 25;
const int FREQUENCY_DEPOSIT_CHECKING = 15;
const int FREQUENCY_BALANCE = 0;

};  // namespace smallbank