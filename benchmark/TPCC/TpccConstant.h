#pragma once

#include <string>

#include "Base/BenchTypes.h"
#include "common/Type.h"

namespace tpcc {

constexpr size_t TPCC_TABLE_NUM = 9;
constexpr size_t TPCC_TXN_NUM = 5;

constexpr TableId WAREHOUSE_TABLE = 0;
constexpr TableId DISTRICT_TABLE = 1;
constexpr TableId CUSTOMER_TABLE = 2;
constexpr TableId HISTORY_TABLE = 3;
constexpr TableId NEW_ORDER_TABLE = 4;
constexpr TableId ORDER_TABLE = 5;
constexpr TableId ORDER_LINE_TABLE = 6;
constexpr TableId STOCK_TABLE = 7;
constexpr TableId ITEM_TABLE = 8;
constexpr TableId TPCC_TABLE_MAX = 9;

// Definition for column keys
// Warehouse Table:
const ColumnId W_TAX = 0;
const ColumnId W_YTD = 1;
const ColumnId W_NAME = 2;
const ColumnId W_STREET_1 = 3;
const ColumnId W_STREET_2 = 4;
const ColumnId W_CITY = 5;
const ColumnId W_STATE = 6;
const ColumnId W_ZIP = 7;

// District Table:
const ColumnId D_TAX = 0;
const ColumnId D_YTD = 1;
const ColumnId D_NEXT_OID = 2;
const ColumnId D_NAME = 3;
const ColumnId D_STREET_1 = 4;
const ColumnId D_STREET_2 = 5;
const ColumnId D_CITY = 6;
const ColumnId D_STATE = 7;
const ColumnId D_ZIP = 8;

// Customer Table:
const ColumnId C_BALANCE = 0;
const ColumnId C_YTD_PAYMENT = 1;
const ColumnId C_PAYMENT_CNT = 2;
const ColumnId C_DATA = 3;
const ColumnId C_FIRST = 4;
const ColumnId C_MIDDLE = 5;
const ColumnId C_LAST = 6;
const ColumnId C_STREET_1 = 7;
const ColumnId C_STREET_2 = 8;
const ColumnId C_CITY = 9;
const ColumnId C_STATE = 10;
const ColumnId C_ZIP = 11;
const ColumnId C_PHONE = 12;
const ColumnId C_SINCE = 13;
const ColumnId C_CREDIT = 14;
const ColumnId C_CREDIT_LIMIT = 15;
const ColumnId C_DISCOUNT = 16;
const ColumnId C_DELIVERY_CNT = 17;

// History Table:
const ColumnId H_AMOUNT = 0;
const ColumnId H_DATE = 1;
const ColumnId H_DATA = 2;

// New Order Table:
const ColumnId NO_W_ID = 0;
const ColumnId NO_D_ID = 0;
const ColumnId NO_O_ID = 0;

// Order Table:
const ColumnId O_C_ID = 0;
const ColumnId O_CARRIER_ID = 1;
const ColumnId O_OL_CNT = 2;
const ColumnId O_ALL_LOCAL = 3;
const ColumnId O_ENTRY_D = 4;

// Order Line Table:
const ColumnId OL_I_ID = 0;
const ColumnId OL_SUPPLY_W_ID = 1;
const ColumnId OL_QUANTITY = 2;
const ColumnId OL_AMOUNT = 3;
const ColumnId OL_DELIVERY_D = 4;
const ColumnId OL_DIST_INFO = 5;

// Item Table
const ColumnId I_IM_ID = 0;
const ColumnId I_PRICE = 1;
const ColumnId I_NAME = 2;
const ColumnId I_DATA = 3;

// Stock Table
const ColumnId S_QUANTITY = 0;
const ColumnId S_YTD = 1;
const ColumnId S_ORDER_CNT = 2;
const ColumnId S_REMOTE_CNT = 3;
const ColumnId S_DIST = 4;
const ColumnId S_DATA = 5;

// Stored procedure types
const BenchTxnType kNewOrder = 0;
const BenchTxnType kPayment = 1;
const BenchTxnType kDelivery = 2;
const BenchTxnType kOrderStatus = 3;
const BenchTxnType kStockLevel = 4;
const BenchTxnType kTPCCTransactionTypeMax = 5;

/**
 *    Table related constants configurations
 *    The following constants are copied from Cavalia's implementation:
 *        https://github.com/Cavalia/Cavalia
 */

// Item constants
// const int NUM_ITEMS = 1000; // origin : 100000
const int NUM_ITEMS = 100000;
const int MIN_IM = 1;
const int MAX_IM = 10000;
const double MIN_PRICE = 1.00;
const double MAX_PRICE = 100.00;
const int ITEM_NAME_SIZE_MIN = 14;
const int ITEM_NAME_SIZE_MAX = 24;
const int ITEM_DATA_SIZE_MIN = 26;
const int ITEM_DATA_SIZE_MAX = 50;

// ------------------------ Warehouse constants BEGIN

// Column Definition Constants
const int WAREHOUSE_NAME_SIZE_MIN = 6;
const int WAREHOUSE_NAME_SIZE_MAX = 10;

const int WAREHOUSE_STREET_SIZE_MIN = 10;
const int WAREHOUSE_STREET_SIZE_MAX = 20;

const int WAREHOUSE_CITY_SIZE_MIN = 10;
const int WAREHOUSE_CITY_SIZE_MAX = 20;

const int WAREHOUSE_STATE_SIZE = 2;

const int WAREHOUSE_ZIP_SIZE = 9;
const std::string WAREHOUSE_ZIP_SUFFIX = "11111";

// Column Values Constants
const double MIN_TAX = 0;
const double MAX_TAX = 0.2000;
const int TAX_DECIMALS = 4;
const double INITIAL_W_YTD = 300000.00;

//  Warehouse constants ENDS ------------------------

// ---------------------- DISTRICT TABLE CONSTANTS BEGIN

// Column Definition Constants
const int DISTRICT_NAME_SIZE_MIN = 6;
const int DISTRICT_NAME_SIZE_MAX = 10;

const int DISTRICT_STREET_SIZE_MIN = 10;
const int DISTRICT_STREET_SIZE_MAX = 20;

const int DISTRICT_CITY_SIZE_MIN = 10;
const int DISTRICT_CITY_SIZE_MAX = 20;

const int DISTRICT_STATE_SIZE = 2;

const int DISTRICT_ZIP_SIZE = 9;

// Column Values Constants
const int DISTRICTS_PER_WAREHOUSE = 10;

// different from Warehouse
const double INITIAL_D_YTD = 30000.00;
const std::string DISTRICT_ZIP_SUFFIX = "11111";

const int DISTRICT_INITIAL_NEXT_O_ID = 31;  // origin : 3001
const int DISTRICT_MAX_OID = 20000;
//  DISTRICT TABLE CONSTANTS ENDS ------------------------

// ---------------------- DISTRICT TABLE CONSTANTS BEGIN
// const int CUSTOMERS_PER_DISTRICT = 30; // origin : 3000

// Column Definition Constants
const int CUSTOMER_DATA_SIZE_MIN = 300;
const int CUSTOMER_DATA_SIZE_MAX = 500;

const int CUSTOMER_FIRST_SIZE_MIN = 8;
const int CUSTOMER_FIRST_SIZE_MAX = 16;

const int CUSTOMER_MIDDLE_SIZE = 2;

const int CUSTOMER_LAST_SIZE_MIN = 8;
const int CUSTOMER_LAST_SIZE_MAX = 16;

const int CUSTOMER_STREET_SIZE_MIN = 10;
const int CUSTOMER_STREET_SIZE_MAX = 20;

const int CUSTOMER_CITY_SIZE_MIN = 10;
const int CUSTOMER_CITY_SIZE_MAX = 20;

const int CUSTOMER_STATE_SIZE = 2;

const int CUSTOMER_ZIP_SIZE = 9;
const std::string CUSTOMER_ZIP_SUFFIX = "11111";

const int CUSTOMER_PHONE_SIZE = 16;

const int CUSTOMER_CREDIT_SIZE = 2;

// Column Values Constants
const std::string MIDDLE = "OE";
const int CUSTOMERS_PER_DISTRICT = 3000;
const double INITIAL_CREDIT_LIM = 50000.00;
const double MIN_DISCOUNT = 0.0000;
const double MAX_DISCOUNT = 0.5000;
const int DISCOUNT_DECIMALS = 4;
const double INITIAL_BALANCE = -10.00;
const double INITIAL_YTD_PAYMENT = 10.00;
const int INITIAL_PAYMENT_CNT = 1;
const int INITIAL_DELIVERY_CNT = 0;

const std::string GOOD_CREDIT = "GC";
const std::string BAD_CREDIT = "BC";
// ---------------------- DISTRICT TABLE CONSTANTS END

// ---------------------- HISTORY TABLE CONSTANTS BEGIN
// Column Definition constants
const int HISTORY_DATA_SIZE_MIN = 12;
const int HISTORY_DATA_SIZE_MAX = 24;

// Column Values constants
const double HISTORY_INITIAL_AMOUNT = 10.00;
// ---------------------- HISTORY TABLE CONSTANTS END

/* ---------------------- ORDER TABLE CONSTANTS BEGIN */
const int MIN_CARRIER_ID = 12;
const int MAX_CARRIER_ID = 24;

// HACK: This is not strictly correct, but it works
const int NULL_CARRIER_ID = 0;

// o_id < than this value, carrier != null, >= -> carrier == null
const int NULL_CARRIER_LOWER_BOUND = 2101;

// const int INITIAL_ORDERS_PER_DISTRICT = 30; // origin : 3000
const int INITIAL_ORDERS_PER_DISTRICT = 3000;

/* ---------------------- ORDER TABLE CONSTANTS END */

/* ---------------------- ORDER LINE TABLE CONSTANTS BEGIN */

const int ORDER_LINE_DIST_INFO_SIZE = 24;

const int MIN_OL_CNT = 5;
const int MAX_OL_CNT = 15;
const int MAX_OL_QUANTITY = 10;
const int INITIAL_QUANTITY = 5;
const double MIN_AMOUNT = 0.01;

// ---------------------- ORDER LINE TABLE CONSTANTS END

// Stock constants
const int MIN_QUANTITY = 10;
const int MAX_QUANTITY = 100;
const int DIST = 24;
// const int STOCK_PER_WAREHOUSE = 1000; //origin : 100000
const int STOCK_PER_WAREHOUSE = 100000;

const bool INITIAL_ALL_LOCAL = true;

// New order constants
// const int INITIAL_NEW_ORDERS_PER_DISTRICT = 9; // origin : 900
const int INITIAL_NEW_ORDERS_PER_DISTRICT = 900;

// Stock Table constants
const int STOCK_DIST_INFO_EACH = 24;
const int STOCK_DATA_SIZE = 50;

// TPC-C 2.4.3.4 (page 31) says this must be displayed when new order rolls back.
const std::string INVALID_ITEM_MESSAGE = "Item number is not valid";

// Used to generate stock level transactions
const int MIN_STOCK_LEVEL_THRESHOLD = 10;
const int MAX_STOCK_LEVEL_THRESHOLD = 20;

// Used to generate payment transactions
const double MIN_PAYMENT = 1.0;
const double MAX_PAYMENT = 5000.0;

// Indicates "brand" items and stock in i_data and s_data.
const std::string ORIGINAL_STRING = "ORIGINAL";

// Just make the VarChar fields of TPCC Tables readbale
constexpr int CONST_STREET_NAME_COUNT = 30;
constexpr const char* street_name[CONST_STREET_NAME_COUNT] = {
    "4th Street",        "6th Street",     "7th Street",       "8th Street",
    "9th Street",        "10th Street",    "River Road",       "Lake Drive",
    "Sunset Boulevard",  "Hillcrest Road", "Forest Drive",     "Orchard Road",
    "Washington Avenue", "Lincoln Street", "Jefferson Avenue", "Madison Avenue",
    "Adams Street",      "Franklin Road",  "Jackson Street",   "Grant Avenue",
    "Monroe Street",     "Harrison Road",  "Johnson Avenue",   "Wilson Street",
    "Roosevelt Avenue",  "Kennedy Drive",  "Reagan Boulevard", "Clinton Street",
    "Bush Street",       "Obama Avenue"};

constexpr int CONST_CITY_NAME_COUNT = 30;
constexpr const char* city_name[CONST_CITY_NAME_COUNT] = {
    "New York",    "Los Angeles", "Chicago",       "Houston",       "Phoenix",      "Philadelphia",
    "San Antonio", "San Diego",   "Dallas",        "San Jose",      "Austin",       "Jacksonville",
    "Fort Worth",  "Columbus",    "Charlotte",     "San Francisco", "Indianapolis", "Seattle",
    "Denver",      "Washington",  "Boston",        "El Paso",       "Detroit",      "Nashville",
    "Portland",    "Memphis",     "Oklahoma City", "Las Vegas",     "Louisville",   "Baltimore"};

constexpr int CONST_STATE_NAME_COUNT = 12;
constexpr const char* state_name[CONST_STATE_NAME_COUNT] = {"AB", "BC", "CD", "DE", "EF", "FG",
                                                            "GH", "HI", "IJ", "JK", "KL", "LM"};

constexpr int MAX_ORDERID_PER_DISTRICT = 20000;

constexpr int MAX_PAYMENT_CNT_PER_CUSTOMER = 1000;

constexpr char warehouse_table_name[] = "Warehouse ";

constexpr char district_table_name[] = "District  ";

constexpr char customer_table_name[] = "Customer  ";

constexpr char history_table_name[] = "History   ";

constexpr char neworder_table_name[] = "NewOrder  ";

constexpr char order_table_name[] = "Order     ";

constexpr char orderline_table_name[] = "OrderLine ";

constexpr char stock_table_name[] = "Stock     ";

constexpr char item_table_name[] = "Item      ";

constexpr int FREQUENCY_NEWORDER = 50;

constexpr int FREQUENCY_PAYMENT = 46;

constexpr int FREQUENCY_DELIVERY = 00;

constexpr int FREQUENCY_ORDER_STATUS = 04;

constexpr int FREQUENCY_STOCK_LEVEL = 00;
};  // namespace tpcc

static constexpr bool kReturnTxnResults = false;
