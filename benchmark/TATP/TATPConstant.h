#pragma once

#include <string>

#include "Base/BenchConfig.h"
#include "Base/BenchTypes.h"
#include "common/Type.h"

namespace tatp {
constexpr size_t TATP_TABLE_NUM = 5;
constexpr size_t TATP_TXN_NUM = 7;

// Table Definitions:
constexpr TableId SUBSCRIBER_TABLE = 0;
constexpr TableId SECONDARY_SUBSCRIBER_TABLE = 1;
constexpr TableId ACCESSINFO_TABLE = 2;
constexpr TableId SPECIAL_FACILITY_TABLE = 3;
constexpr TableId CALL_FORWARDING_TABLE = 4;

// Column definition:
// SUBSCRIBER Table:
const ColumnId S_SUBNUM = 0;
const ColumnId S_HEX = 1;
const ColumnId S_BYTES = 2;
const ColumnId S_BITS = 3;
const ColumnId S_MSC_LOCATION = 4;
const ColumnId S_VLR_LOCATION = 5;

// SECONDARY_SUBSCRIBER Table:
// The secondary subscriber table only has one column, which is the subscriber id
const ColumnId SEC_SUB_SID = 0;

// ACCESSINFO Table:
const ColumnId AI_DATA1 = 0;
const ColumnId AI_DATA2 = 1;
const ColumnId AI_DATA3 = 2;
const ColumnId AI_DATA4 = 3;

// Special Facility Table:
const ColumnId SF_IS_ACTIVE = 0;
const ColumnId SF_ERROR_CNTL = 1;
const ColumnId SF_DATA_A = 2;
const ColumnId SF_DATA_B = 3;

// CALL_FORWARDING Table:
const ColumnId CF_END_TIME = 0;
const ColumnId CF_NUMBERX = 1;

// Size of each table column: 
const size_t S_SUBNUM_SIZE = 8;
const size_t S_HEX_NUM = 5;
const size_t S_BYTES_NUM = 10;

// Transaction Types
const BenchTxnType kGetSubscriber = 0;
const BenchTxnType kGetNewDestination = 1;
const BenchTxnType kGetAccessData = 2;
const BenchTxnType kUpdateSubscriberData = 3;
const BenchTxnType kUpdateLocation = 4;
const BenchTxnType kInsertCallForwarding = 5;
const BenchTxnType kDeleteCallForwarding = 6;

const uint32_t MSC_LOCATION_DEFAULT_VALUE = 0xdeadbeef;

const uint32_t VLR_LOCATION_DEFAULT_VALUE = 0xbeefdead;

const uint32_t MAX_END_TIME = 24;

const short S_BITS_VALUE_DEFAULT = 0xdead;

// Transaction Table Names:
constexpr char subscriber_table_name[] = "Subscriber ";
constexpr char sec_subscriber_table_name[] = "Secondary Subscriber ";
constexpr char accessinfo_table_name[] = "AccessInfo ";
constexpr char specialfacility_table_name[] = "SpecialFacility ";
constexpr char callforwarding_table_name[] = "CallForwarding ";

const int FREQUENCY_GET_SUBSCRIBER = 36;
const int FREQUENCY_GET_ACCESS_DATA = 36;
const int FREQUENCY_GET_NEW_DESTINATION = 10;
const int FREQUENCY_UPDATE_SUBSCRIBER_DATA = 2;
const int FREQUENCY_UPDATE_LOCATION = 14;
const int FREQUENCY_INSERT_CALL_FORWARDING = 2;
const int FREQUENCY_DELETE_CALL_FORWARDING = 0;

const size_t AI_TYPE_PER_SUBSCRIBER = 4;
const size_t SF_TYPE_PER_SUBSCRIBER = 4;
const size_t STARTTIME_PER_FACILITY = 24;
};  // namespace tatp