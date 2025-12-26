// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <cstddef>

/*********************** For common **********************/
// Max data item size.
// 64: smallbank
// 40: tatp
// 664: tpcc
// 40: micro-benchmark

#define WORKLOAD_TPCC 1
#define WORKLOAD_SMALLBANK 0
#define WORKLOAD_MICRO 0
#define WORKLOAD_TATP 0

#if WORKLOAD_TPCC
const size_t MAX_ITEM_SIZE = 664;
#elif WORKLOAD_SMALLBANK
// The actual size of smallbank_accounts_val_t is 69, we round it up 
// to 72 to make it a multiple of 8
const size_t MAX_ITEM_SIZE = 128;
#elif WORKLOAD_MICRO
const size_t MAX_ITEM_SIZE = 232;
#elif WORKLOAD_TATP
const size_t MAX_ITEM_SIZE = 40;
#endif

#define THREAD_PER_CN 40

/*********************** For FORD **********************/
// 0: Read rw data without lock
// 1: Read+lock rw data
#define READ_LOCK 1

// 0: Seperately commit remote replicas
// 1: Coalescently commit remote replicas
#define COMMIT_TOGETHER 1

// 0: Disable reading read-only data from backups
// 1: Enable reading read-only data from backups
#define READ_BACKUP 0

// 0: No remote persistency guarantee
// 1: Full flush
// 2: Selective flush
#define RFLUSH 2

// 0: Wait if invisible
// 1: Abort if invisible
#define INV_ABORT 1

/*********************** For Localized opt **********************/
// Below are only for FORD with coalescent commit
// 0: Disable local lock
// 1: Enable locl lock
#define LOCAL_LOCK 0

// 0: Remote validation for RO set
// 1: Cache versions in local
#define LOCAL_VALIDATION 0

// Hash table parameters for localized validation
// For tatp
// 5
// 4
// 10000000

// For smallbank
// 2
// 1
// 100000

// For tpcc
// 11
// 72
// 100000

#define MAX_TABLE_NUM 11
#define SLOT_PER_BKT 72
#define NUM_BKT 100000

/*********************** For counterparts **********************/
// 0: Do not cache addrs in local. Default for FaRM
// 1: Cache addrs in local. Default for DrTM+h, Optmized for FaRM
#define USE_LOCAL_ADDR_CACHE 0

// 1: Locks block reads
// 0: Use FORD's machenism, i.e., visibility control to enable read locked data but not invisible data
// This is an **opposite** scheme compared with our visibility control, i.e., open this will close visibility, and close this will open visibility
#define LOCK_REFUSE_READ_RO 0
#define LOCK_REFUSE_READ_RW 0

/*********************** For micro-benchmarks **********************/
// 0: Does not wait lock, just abort (For end-to-end tests)
// 1: wait lock until resuming execution (For lock duration tests, remember set coroutine num as 2)
#define LOCK_WAIT 0

// 0: Does not busily wait the data to be visible, e.g., yield to another coroutine to execute the next tx (For end-to-end tests)
// 1: Busily wait the data to be visible (For visibility tests, remember set coroutine num as 2)
#define INV_BUSY_WAIT 0
