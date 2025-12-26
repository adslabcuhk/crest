// Author: Ming Zhang
// Copyright (c) 2023

#include "smallbank/smallbank_txn.h"
#include "base/common.h"
#include "memstore/cvt.h"

/******************** The business logic (Transaction) start
 * ********************/

bool TxAmalgamate(SmallBank *smallbank_client, uint64_t *seed,
                  coro_yield_t &yield, tx_id_t tx_id, TXN *txn,
                  AmalgamateParam *param) {
  txn->Begin(tx_id, TXN_TYPE::kRWTxn, "TxAmalgamate");

  /* Transaction parameters */
  uint64_t acct_id_0 = param->custid_0_, acct_id_1 = param->custid_1_;
  // smallbank_client->get_two_accounts(seed, &acct_id_0, &acct_id_1);

  /* Read from accounts table for both accounts */
  smallbank_accounts_key_t acc_key_0, acc_key_1;
  acc_key_0.acct_id = acct_id_0;
  acc_key_1.acct_id = acct_id_1;
  auto acc_record_0 = std::make_shared<DataSetItem>(
      (table_id_t)SmallBankTableType::kAccountsTable,
      smallbank_account_val_t_size, acc_key_0.item_key, UserOP::kRead);
  auto acc_record_1 = std::make_shared<DataSetItem>(
      (table_id_t)SmallBankTableType::kAccountsTable,
      smallbank_account_val_t_size, acc_key_1.item_key, UserOP::kRead);
  txn->AddToReadOnlySet(acc_record_0);
  txn->AddToReadOnlySet(acc_record_1);

  /* Read from savings and checking tables for acct_id_0 */
  smallbank_savings_key_t sav_key_0;
  sav_key_0.acct_id = acct_id_0;
  auto sav_record_0 = std::make_shared<DataSetItem>(
      (table_id_t)SmallBankTableType::kSavingsTable,
      smallbank_savings_val_t_size, sav_key_0.item_key, UserOP::kUpdate);
  txn->AddToReadWriteSet(sav_record_0);

  smallbank_checking_key_t chk_key_0;
  chk_key_0.acct_id = acct_id_0;
  auto chk_record_0 = std::make_shared<DataSetItem>(
      (table_id_t)SmallBankTableType::kCheckingTable,
      smallbank_checking_val_t_size, chk_key_0.item_key, UserOP::kUpdate);
  txn->AddToReadWriteSet(chk_record_0);

  /* Read from checking account for acct_id_1 */
  smallbank_checking_key_t chk_key_1;
  chk_key_1.acct_id = acct_id_1;
  auto chk_record_1 = std::make_shared<DataSetItem>(
      (table_id_t)SmallBankTableType::kCheckingTable,
      smallbank_checking_val_t_size, chk_key_1.item_key, UserOP::kUpdate);
  txn->AddToReadWriteSet(chk_record_1);

  if (!txn->Execute(yield))
    return false;

  /* If we are here, execution succeeded and we have locks */
  smallbank_savings_val_t *sav_val_0 =
      (smallbank_savings_val_t *)sav_record_0->Value();
  smallbank_checking_val_t *chk_val_0 =
      (smallbank_checking_val_t *)chk_record_0->Value();
  smallbank_checking_val_t *chk_val_1 =
      (smallbank_checking_val_t *)chk_record_1->Value();
  if (sav_val_0->magic != smallbank_savings_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id
                    << "-" << txn->coro_id << "-" << tx_id;
  }
  if (chk_val_0->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id
                    << "-" << txn->coro_id << "-" << tx_id;
  }
  if (chk_val_1->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id
                    << "-" << txn->coro_id << "-" << tx_id;
  }

  /* Increase acct_id_1's kBalance and set acct_id_0's balances to 0 */
  chk_record_1->SetUpdate(smallbank_checking_val_bitmap::cbal, &chk_val_1->bal,
                          sizeof(chk_val_1->bal));
  chk_val_1->bal += (sav_val_0->bal + chk_val_0->bal);

  sav_record_0->SetUpdate(smallbank_savings_val_bitmap::sbal, &sav_val_0->bal,
                          sizeof(sav_val_0->bal));
  sav_val_0->bal = 0;

  chk_record_0->SetUpdate(smallbank_checking_val_bitmap::cbal, &chk_val_0->bal,
                          sizeof(chk_val_0->bal));
  chk_val_0->bal = 0;

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

/* Calculate the sum of saving and checking kBalance */
bool TxBalance(SmallBank *smallbank_client, uint64_t *seed, coro_yield_t &yield,
               tx_id_t tx_id, TXN *txn, BalanceParam *param) {
  txn->Begin(tx_id, TXN_TYPE::kROTxn, "TxBalance");

  /* Transaction parameters */
  uint64_t acct_id = param->custid_;

  /* Read from accounts tables */
  smallbank_accounts_key_t acc_key;
  acc_key.acct_id = acct_id;
  auto acc_record = std::make_shared<DataSetItem>(
      (table_id_t)SmallBankTableType::kAccountsTable,
      smallbank_account_val_t_size, acc_key.item_key, UserOP::kRead);
  txn->AddToReadOnlySet(acc_record);

  /* Read from savings and checking tables */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  auto sav_record = std::make_shared<DataSetItem>(
      (table_id_t)SmallBankTableType::kSavingsTable,
      smallbank_savings_val_t_size, sav_key.item_key, UserOP::kRead);
  txn->AddToReadOnlySet(sav_record);

  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  auto chk_record = std::make_shared<DataSetItem>(
      (table_id_t)SmallBankTableType::kCheckingTable,
      smallbank_checking_val_t_size, chk_key.item_key, UserOP::kRead);
  txn->AddToReadOnlySet(chk_record);

  if (!txn->Execute(yield))
    return false;

  smallbank_savings_val_t *sav_val =
      (smallbank_savings_val_t *)sav_record->Value();
  smallbank_checking_val_t *chk_val =
      (smallbank_checking_val_t *)chk_record->Value();
  if (sav_val->magic != smallbank_savings_magic) {
    RDMA_LOG(INFO) << "read value: " << sav_val;
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id
                    << "-" << txn->coro_id << "-" << tx_id;
  }
  if (chk_val->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id
                    << "-" << txn->coro_id << "-" << tx_id;
  }

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

/* Add $1.3 to acct_id's checking account */
bool TxDepositChecking(SmallBank *smallbank_client, uint64_t *seed,
                       coro_yield_t &yield, tx_id_t tx_id, TXN *txn,
                       DepositCheckingParam *param) {
  txn->Begin(tx_id, TXN_TYPE::kRWTxn, "TxDepositChecking");

  /* Transaction parameters */
  uint64_t acct_id = param->custid_;
  float amount = 1.3;

  /* Read from accounts table */
  smallbank_accounts_key_t acc_key;
  acc_key.acct_id = acct_id;
  auto acc_record = std::make_shared<DataSetItem>(
      (table_id_t)SmallBankTableType::kAccountsTable,
      smallbank_account_val_t_size, acc_key.item_key, UserOP::kRead);
  txn->AddToReadOnlySet(acc_record);

  /* Read from checking table */
  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  auto chk_record = std::make_shared<DataSetItem>(
      (table_id_t)SmallBankTableType::kCheckingTable,
      smallbank_checking_val_t_size, chk_key.item_key, UserOP::kUpdate);
  txn->AddToReadWriteSet(chk_record);

  if (!txn->Execute(yield))
    return false;

  /* If we are here, execution succeeded and we have a lock*/
  smallbank_checking_val_t *chk_val =
      (smallbank_checking_val_t *)chk_record->Value();
  if (chk_val->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id
                    << "-" << txn->coro_id << "-" << tx_id;
  }

  chk_record->SetUpdate(smallbank_checking_val_bitmap::cbal, &chk_val->bal,
                        sizeof(chk_val->bal));
  chk_val->bal += amount; /* Update checking kBalance */

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

/* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
bool TxSendPayment(SmallBank *smallbank_client, uint64_t *seed,
                   coro_yield_t &yield, tx_id_t tx_id, TXN *txn,
                   SendPaymentParam *param) {
  txn->Begin(tx_id, TXN_TYPE::kRWTxn, "TxSendPayment");

  /* Transaction parameters: send money from acct_id_0 to acct_id_1 */
  uint64_t acct_id_0 = param->custid_0_, acct_id_1 = param->custid_1_;
  // smallbank_client->get_two_accounts(seed, &acct_id_0, &acct_id_1);
  float amount = 5.0;

  /* Read from Accounts table*/
  smallbank_accounts_key_t acc_key_0, acc_key_1;
  acc_key_0.acct_id = acct_id_0;
  acc_key_1.acct_id = acct_id_1;
  auto acc_record_0 = std::make_shared<DataSetItem>(
      (table_id_t)SmallBankTableType::kAccountsTable,
      smallbank_account_val_t_size, acc_key_0.item_key, UserOP::kRead);
  auto acc_record_1 = std::make_shared<DataSetItem>(
      (table_id_t)SmallBankTableType::kAccountsTable,
      smallbank_account_val_t_size, acc_key_1.item_key, UserOP::kRead);
  txn->AddToReadOnlySet(acc_record_0);
  txn->AddToReadOnlySet(acc_record_1);

  /* Read from checking table */
  smallbank_checking_key_t chk_key_0;
  chk_key_0.acct_id = acct_id_0;
  auto chk_record_0 = std::make_shared<DataSetItem>(
      (table_id_t)SmallBankTableType::kCheckingTable,
      smallbank_checking_val_t_size, chk_key_0.item_key, UserOP::kUpdate);
  txn->AddToReadWriteSet(chk_record_0);

  /* Read from checking account for acct_id_1 */
  smallbank_checking_key_t chk_key_1;
  chk_key_1.acct_id = acct_id_1;
  auto chk_record_1 = std::make_shared<DataSetItem>(
      (table_id_t)SmallBankTableType::kCheckingTable,
      smallbank_checking_val_t_size, chk_key_1.item_key, UserOP::kUpdate);
  txn->AddToReadWriteSet(chk_record_1);

  if (!txn->Execute(yield))
    return false;

  /* if we are here, execution succeeded and we have locks */
  smallbank_checking_val_t *chk_val_0 =
      (smallbank_checking_val_t *)chk_record_0->Value();
  smallbank_checking_val_t *chk_val_1 =
      (smallbank_checking_val_t *)chk_record_1->Value();
  if (chk_val_0->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id
                    << "-" << txn->coro_id << "-" << tx_id;
  }
  if (chk_val_1->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id
                    << "-" << txn->coro_id << "-" << tx_id;
  }

  // if (chk_val_0->bal < amount) {
  //   txn->TxAbortReadWrite();
  //   return false;
  // }

  chk_record_0->SetUpdate(smallbank_checking_val_bitmap::cbal, &chk_val_0->bal,
                          sizeof(chk_val_0->bal));
  chk_val_0->bal -= amount; /* Debit */

  chk_record_1->SetUpdate(smallbank_checking_val_bitmap::cbal, &chk_val_1->bal,
                          sizeof(chk_val_1->bal));
  chk_val_1->bal += amount; /* Credit */

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

/* Add $20 to acct_id's saving's account */
bool TxTransactSaving(SmallBank *smallbank_client, uint64_t *seed,
                      coro_yield_t &yield, tx_id_t tx_id, TXN *txn,
                      TransactSavingParam *param) {
  txn->Begin(tx_id, TXN_TYPE::kRWTxn, "TxTransactSaving");

  /* Transaction parameters */
  uint64_t acct_id = param->custid_;
  // smallbank_client->get_account(seed, &acct_id);
  float amount = 20.20;

  /* Read from accounts table */
  smallbank_accounts_key_t acc_key;
  acc_key.acct_id = acct_id;
  auto acc_record = std::make_shared<DataSetItem>(
      (table_id_t)SmallBankTableType::kAccountsTable,
      smallbank_account_val_t_size, acc_key.item_key, UserOP::kRead);
  txn->AddToReadOnlySet(acc_record);

  /* Read from saving table */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  auto sav_record = std::make_shared<DataSetItem>(
      (table_id_t)SmallBankTableType::kSavingsTable,
      smallbank_savings_val_t_size, sav_key.item_key, UserOP::kUpdate);
  txn->AddToReadWriteSet(sav_record);

  if (!txn->Execute(yield))
    return false;

  /* If we are here, execution succeeded and we have a lock */
  smallbank_savings_val_t *sav_val =
      (smallbank_savings_val_t *)sav_record->Value();
  if (sav_val->magic != smallbank_savings_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id
                    << "-" << txn->coro_id << "-" << tx_id;
  }

  sav_record->SetUpdate(smallbank_savings_val_bitmap::sbal, &sav_val->bal,
                        sizeof(sav_val->bal));
  sav_val->bal += amount; /* Update saving kBalance */

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

/* Read saving and checking kBalance + update checking kBalance unconditionally
 */
bool TxWriteCheck(SmallBank *smallbank_client, uint64_t *seed,
                  coro_yield_t &yield, tx_id_t tx_id, TXN *txn,
                  WriteCheckParam *param) {
  txn->Begin(tx_id, TXN_TYPE::kRWTxn, "TxWriteCheck");

  /* Transaction parameters */
  uint64_t acct_id = param->custid_;
  // smallbank_client->get_account(seed, &acct_id);
  float amount = 5.0;

  /* Read from accounts table */
  smallbank_accounts_key_t acc_key;
  acc_key.acct_id = acct_id;
  auto acc_record = std::make_shared<DataSetItem>(
      (table_id_t)SmallBankTableType::kAccountsTable,
      smallbank_account_val_t_size, acc_key.item_key, UserOP::kRead);
  txn->AddToReadOnlySet(acc_record);

  /* Read from savings. Read checking record for update. */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  auto sav_record = std::make_shared<DataSetItem>(
      (table_id_t)SmallBankTableType::kSavingsTable,
      smallbank_savings_val_t_size, sav_key.item_key, UserOP::kRead);
  txn->AddToReadOnlySet(sav_record);

  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  auto chk_record = std::make_shared<DataSetItem>(
      (table_id_t)SmallBankTableType::kCheckingTable,
      smallbank_checking_val_t_size, chk_key.item_key, UserOP::kUpdate);
  txn->AddToReadWriteSet(chk_record);

  if (!txn->Execute(yield))
    return false;

  smallbank_savings_val_t *sav_val =
      (smallbank_savings_val_t *)sav_record->Value();
  smallbank_checking_val_t *chk_val =
      (smallbank_checking_val_t *)chk_record->Value();
  if (sav_val->magic != smallbank_savings_magic) {
    RDMA_LOG(INFO) << "read value: " << sav_val;
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id
                    << "-" << txn->coro_id << "-" << tx_id;
  }
  if (chk_val->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << txn->t_id
                    << "-" << txn->coro_id << "-" << tx_id;
  }

  chk_record->SetUpdate(smallbank_checking_val_bitmap::cbal, &chk_val->bal,
                        sizeof(chk_val->bal));

  if (sav_val->bal + chk_val->bal < amount) {
    chk_val->bal -= (amount + 1);
  } else {
    chk_val->bal -= amount;
  }

  bool commit_status = txn->Commit(yield);
  return commit_status;
}

/******************** The business logic (Transaction) end ********************/