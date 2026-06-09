CREATE DATABASE BRONZE;
CREATE SCHEMA FINANCE;
CREATE SCHEMA UTILS;

USE DATABASE BRONZE;
USE SCHEMA FINANCE;

-- ===============================================
-- TABLE DEFINITIONS
-- ===============================================
--  Audit_job_log table
create or replace TABLE BRONZE.FINANCE.AUDIT_JOB_LOG (
	JOB_ID STRING,
	JOB_NAME STRING,
	LAYER_NAME STRING,
	SOURCE_OBJECT STRING,
	TARGET_OBJECT STRING,
	START_TIME TIMESTAMP_NTZ(9),
	END_TIME TIMESTAMP_NTZ(9),
	ROWS_PROCESSED NUMBER,
	ROWS_INSERTED NUMBER,
	ROWS_UPDATED NUMBER,
	ROWS_FAILED NUMBER,
	JOB_STATUS STRING,
	ERROR_MESSAGE STRING,
	LOAD_BATCH_ID STRING
);

-- customers 
CREATE OR REPLACE TABLE BRONZE.FINANCE.CUSTOMERS ( 
    customer_id STRING, 
    customer_type STRING, 
    first_name STRING, 
    last_name STRING, 
    organization_name STRING, 
    email STRING, 
    phone_number STRING, 
    tax_identifier STRING, 
    risk_profile STRING, 
    kyc_status STRING, 
    created_at TIMESTAMP, 
    updated_at TIMESTAMP,
    -- Metadata columns
    source_file_name STRING, 
    load_batch_id STRING,
    audit_job_id STRING, -- Which ETL job loaded this row
    file_row_number NUMBER, -- Which exact row inside CSV caused issue
    record_hash STRING, -- To detect changed rows, duplicates
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
); 

-- accounts 
CREATE OR REPLACE TABLE BRONZE.FINANCE.ACCOUNTS ( 
    account_id STRING, 
    customer_id STRING, 
    account_number STRING, 
    account_type STRING, 
    account_status STRING, 
    base_currency STRING, 
    opened_date DATE, 
    closed_date DATE, 
    advisor_code STRING, 
    created_at TIMESTAMP, 
    updated_at TIMESTAMP, 
    -- Metadata columns
    source_file_name STRING, 
    load_batch_id STRING,
    audit_job_id STRING,
    file_row_number NUMBER, 
    record_hash STRING, 
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
); 

-- securities 
CREATE OR REPLACE TABLE BRONZE.FINANCE.SECURITIES ( 
    security_id STRING, 
    security_symbol STRING, 
    security_name STRING, 
    isin_code STRING, 
    security_type STRING, 
    asset_class STRING, 
    exchange_code STRING, 
    currency STRING, 
    face_value NUMBER(18,4), 
    coupon_rate NUMBER(10,4), 
    maturity_date DATE, 
    created_at TIMESTAMP, 
    updated_at TIMESTAMP, 
    -- Metadata columns
    source_file_name STRING, 
    load_batch_id STRING,
    audit_job_id STRING,
    file_row_number NUMBER, 
    record_hash STRING, 
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
); 

-- trades 
CREATE OR REPLACE TABLE BRONZE.FINANCE.TRADES ( 
    trade_id STRING, 
    account_id STRING, 
    security_id STRING, 
    trade_date TIMESTAMP, 
    settlement_date DATE, 
    trade_type STRING, 
    quantity NUMBER(18,4), 
    trade_price NUMBER(18,6), 
    brokerage_rate NUMBER(10,6), 
    tax_rate NUMBER(10,6), 
    exchange_fee NUMBER(18,4), 
    trade_currency STRING, 
    trade_status STRING, 
    created_at TIMESTAMP, 
    updated_at TIMESTAMP, 
    -- Metadata columns
    source_file_name STRING, 
    load_batch_id STRING,
    audit_job_id STRING,
    file_row_number NUMBER, 
    record_hash STRING, 
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
); 

-- cash_transactions 
CREATE OR REPLACE TABLE BRONZE.FINANCE.CASH_TRANSACTIONS ( 
    cash_transaction_id STRING, 
    account_id STRING, 
    transaction_date TIMESTAMP, 
    transaction_type STRING, 
    amount NUMBER(18,4), 
    currency STRING, 
    reference_number STRING, 
    transaction_status STRING, 
    fee_amount NUMBER(18,4), 
    created_at TIMESTAMP, 
    updated_at TIMESTAMP, 
    -- Metadata columns
    source_file_name STRING, 
    load_batch_id STRING,
    audit_job_id STRING,
    file_row_number NUMBER, 
    record_hash STRING, 
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
); 

-- market_prices 
CREATE OR REPLACE TABLE BRONZE.FINANCE.MARKET_PRICES ( 
    price_id STRING, 
    security_id STRING, 
    price_date DATE, 
    open_price NUMBER(18,6), 
    high_price NUMBER(18,6), 
    low_price NUMBER(18,6), 
    close_price NUMBER(18,6), 
    adjusted_close_price NUMBER(18,6), 
    price_currency STRING, 
    fx_rate_to_base NUMBER(18,8), 
    created_at TIMESTAMP, 
    updated_at TIMESTAMP, 
    -- Metadata columns
    source_file_name STRING, 
    load_batch_id STRING,
    audit_job_id STRING,
    file_row_number NUMBER, 
    record_hash STRING, 
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
); 
-- to check the procedure call history: v_file_count
CREATE OR REPLACE TABLE BRONZE.UTILS.FILE_DETECTION_AUDIT
(
    TABLE_NAME STRING,
    FILE_COUNT NUMBER,
    STATUS STRING,
    RUN_TIME TIMESTAMP
);
-- Bronze Audit Log Table
CREATE OR REPLACE TABLE BRONZE.FINANCE.AUDIT_JOB_LOG ( -- 14 columns
    job_id STRING, 
    job_name STRING, 
    layer_name STRING, 
    source_object STRING, 
    target_object STRING, 
    start_time TIMESTAMP, 
    end_time TIMESTAMP, 
    rows_processed INT, 
    rows_inserted INT, 
    rows_updated INT, 
    rows_failed INT, 
    job_status STRING, 
    error_message STRING, 
    load_batch_id STRING 
); 

-- ETL_CONFIG TABLE to use the values in generic procedure: each table has a record here
CREATE OR REPLACE TABLE BRONZE.UTILS.ETL_CONFIG
(
    table_name STRING,
    stage_folder STRING,
    target_table STRING,
    file_format STRING,
    column_list STRING,
    select_list STRING,
    hash_columns STRING,
    job_name STRING,
    email_recipient STRING,
    active_flag STRING,
    business_key STRING
);
-- INSERT statement for CUSTOMERS TABLE
INSERT INTO BRONZE.UTILS.ETL_CONFIG
VALUES
(
    'CUSTOMERS',
    '@BRONZE.FINANCE.BRONZE_STAGE/Customers/',
    'BRONZE.FINANCE.CUSTOMERS',
    'BRONZE.UTILS.CSV_FILE_FORMAT',
    'customer_id,
     customer_type,
     first_name,
     last_name,
     organization_name,
     email,
     phone_number,
     tax_identifier,
     risk_profile,
     kyc_status,
     created_at,
     updated_at,
     source_file_name,
     load_batch_id,
     audit_job_id,
     file_row_number,
     record_hash,
     load_timestamp',
    't.$1,
     t.$2,
     t.$3,
     t.$4,
     t.$5,
     t.$6,
     t.$7,
     t.$8,
     t.$9,
     t.$10,
     TO_TIMESTAMP_NTZ(t.$11,''MM/DD/YYYY HH24:MI''),
     TO_TIMESTAMP_NTZ(t.$12,''MM/DD/YYYY HH24:MI''),
     METADATA$FILENAME,
     ''DYNAMIC_BATCH_ID'',
     ''DYNAMIC_JOB_ID'',
     METADATA$FILE_ROW_NUMBER,
     MD5(CONCAT(t.$1,t.$11,t.$12)),
     CURRENT_TIMESTAMP()',
    't.$1,t.$11,t.$12',
    'LOAD_BRONZE_CUSTOMERS',
    'kgirija@defteam.co',
    'Y',
    'CUSTOMER_ID'
);
-- INSERT for ACCOUNTS TABLE
INSERT INTO BRONZE.UTILS.ETL_CONFIG
VALUES
(
    'ACCOUNTS',
    '@BRONZE.FINANCE.BRONZE_STAGE/Accounts/',
    'BRONZE.FINANCE.ACCOUNTS',
    'BRONZE.UTILS.CSV_FILE_FORMAT',
    'account_id,
     customer_id,
     account_number,
     account_type,
     account_status,
     base_currency,
     opened_date,
     closed_date,
     advisor_code,
     created_at,
     updated_at,
     source_file_name, 
     load_batch_id,
     audit_job_id,
     file_row_number,
     record_hash, 
     load_timestamp',
    't.$1,
     t.$2,
     t.$3,
     t.$4,
     t.$5,
     t.$6,
     TO_TIMESTAMP_NTZ(t.$7,''MM/DD/YYYY''),
     TO_TIMESTAMP_NTZ(t.$8,''MM/DD/YYYY''), 
     t.$9,
     TO_TIMESTAMP_NTZ(t.$10,''MM/DD/YYYY HH24:MI''),
     TO_TIMESTAMP_NTZ(t.$11,''MM/DD/YYYY HH24:MI''),
     METADATA$FILENAME,
     ''DYNAMIC_BATCH_ID'',
     ''DYNAMIC_JOB_ID'',
     METADATA$FILE_ROW_NUMBER,
     MD5(CONCAT(t.$1,t.$10,t.$11)),
     CURRENT_TIMESTAMP()',
    't.$1,t.$10,t.$11',
    'LOAD_BRONZE_ACCOUNTS',
    'kgirija@defteam.co',
    'Y',
    'ACCOUNT_ID'
);
-- INSERT for CASH_TRANSACTIONS TABLE
INSERT INTO BRONZE.UTILS.ETL_CONFIG
VALUES
(
    'CASH_TRANSACTIONS',
    '@BRONZE.FINANCE.BRONZE_STAGE/CashTransactions/',
    'BRONZE.FINANCE.CASH_TRANSACTIONS',
    'BRONZE.UTILS.CSV_FILE_FORMAT',
    'cash_transaction_id,
     account_id,
     transaction_date,
     transaction_type,
     amount,
     currency,
     reference_number,
     transaction_status,
     fee_amount,
     created_at,
     updated_at,
     source_file_name, 
     load_batch_id,
     audit_job_id,
     file_row_number,
     record_hash, 
     load_timestamp',
    't.$1,
     t.$2,
     TO_TIMESTAMP_NTZ(t.$3,''MM/DD/YYYY HH24:MI''),
     t.$4,
     t.$5,
     t.$6,
     t.$7,
     t.$8,
     t.$9,
     TO_TIMESTAMP_NTZ(t.$10,''MM/DD/YYYY HH24:MI''),
     TO_TIMESTAMP_NTZ(t.$11,''MM/DD/YYYY HH24:MI''),
     METADATA$FILENAME,
     ''DYNAMIC_BATCH_ID'',
     ''DYNAMIC_JOB_ID'',
     METADATA$FILE_ROW_NUMBER,
     MD5(CONCAT(t.$1,t.$10,t.$11)),
     CURRENT_TIMESTAMP()',
    't.$1,t.$10,t.$11',
    'LOAD_BRONZE_CASH_TRANSACTIONS',
    'kgirija@defteam.co',
    'Y',
    'CASH_TRANSACTION_ID'
);
-- INSERT for MARKET_PRICES TABLE
INSERT INTO BRONZE.UTILS.ETL_CONFIG
VALUES
(
    'MARKET_PRICES',
    '@BRONZE.FINANCE.BRONZE_STAGE/MarketPrices/',
    'BRONZE.FINANCE.MARKET_PRICES',
    'BRONZE.UTILS.CSV_FILE_FORMAT',
    'price_id,
     security_id,
     price_date,
     open_price,
     high_price,
     low_price,
     close_price,
     adjusted_close_price,
     price_currency,
     fx_rate_to_base,
     created_at,
     updated_at,
     source_file_name, 
     load_batch_id,
     audit_job_id,
     file_row_number,
     record_hash, 
     load_timestamp',
    't.$1,
     t.$2,
     t.$3,
     t.$4,
     t.$5,
     t.$6,
     t.$7,
     t.$8,
     t.$9,
     t.$10,
     TO_TIMESTAMP_NTZ(t.$11,''MM/DD/YYYY HH24:MI''),
     TO_TIMESTAMP_NTZ(t.$12,''MM/DD/YYYY HH24:MI''),
     METADATA$FILENAME,
     ''DYNAMIC_BATCH_ID'',
     ''DYNAMIC_JOB_ID'',
     METADATA$FILE_ROW_NUMBER,
     MD5(CONCAT(t.$1,t.$11,t.$12)),
     CURRENT_TIMESTAMP()',
    't.$1,t.$11,t.$12',
    'LOAD_BRONZE_MARKET_PRICES',
    'kgirija@defteam.co',
    'Y',
    'PRICE_ID'
);
-- INSERT for SECURITIES TABLE
INSERT INTO BRONZE.UTILS.ETL_CONFIG
VALUES
(
    'SECURITIES',
    '@BRONZE.FINANCE.BRONZE_STAGE/Securities/',
    'BRONZE.FINANCE.SECURITIES',
    'BRONZE.UTILS.CSV_FILE_FORMAT',
    'security_id,
     security_symbol,
     security_name,
     isin_code,
     security_type,
     asset_class,
     exchange_code,
     currency,
     face_value,
     coupon_rate,
     maturity_date,
     created_at,
     updated_at,
     source_file_name, 
     load_batch_id,
     audit_job_id,
     file_row_number,
     record_hash, 
     load_timestamp',
    't.$1,
     t.$2,
     t.$3,
     t.$4,
     t.$5,
     t.$6,
     t.$7,
     t.$8,
     t.$9,
     t.$10,
     TRY_TO_TIMESTAMP_NTZ(t.$11,''MM/DD/YYYY''),
     TO_TIMESTAMP_NTZ(t.$12,''MM/DD/YYYY HH24:MI''),
     TO_TIMESTAMP_NTZ(t.$13,''MM/DD/YYYY HH24:MI''),
     METADATA$FILENAME,
     ''DYNAMIC_BATCH_ID'',
     ''DYNAMIC_JOB_ID'',
     METADATA$FILE_ROW_NUMBER,
     MD5(CONCAT(t.$1,t.$12,t.$13)),
     CURRENT_TIMESTAMP()',
    't.$1,t.$12,t.$13',
    'LOAD_BRONZE_SECURITIES',
    'kgirija@defteam.co',
    'Y',
    'SECURITY_ID'
);
-- INSERT for TRADES TABLE
INSERT INTO BRONZE.UTILS.ETL_CONFIG
VALUES
(
    'TRADES',
    '@BRONZE.FINANCE.BRONZE_STAGE/Trades/',
    'BRONZE.FINANCE.TRADES',
    'BRONZE.UTILS.CSV_FILE_FORMAT',
    'trade_id,
     account_id,
     security_id,
     trade_date,
     settlement_date,
     trade_type,
     quantity,
     trade_price,
     brokerage_rate,
     tax_rate,
     exchange_fee,
     trade_currency,
     trade_status,
     created_at,
     updated_at,
     source_file_name, 
     load_batch_id,
     audit_job_id,
     file_row_number,
     record_hash, 
     load_timestamp',
    't.$1,
     t.$2,
     t.$3,
     TO_TIMESTAMP_NTZ(t.$4,''MM/DD/YYYY HH24:MI''),
     TO_TIMESTAMP_NTZ(t.$5,''MM/DD/YYYY''),
     t.$6,
     t.$7,
     t.$8,
     t.$9,
     t.$10,
     t.$11,
     t.$12,
     t.$13,
     TO_TIMESTAMP_NTZ(t.$14,''MM/DD/YYYY HH24:MI''),
     TO_TIMESTAMP_NTZ(t.$15,''MM/DD/YYYY HH24:MI''),
     METADATA$FILENAME,
     ''DYNAMIC_BATCH_ID'',
     ''DYNAMIC_JOB_ID'',
     METADATA$FILE_ROW_NUMBER,
     MD5(CONCAT(t.$1,t.$14,t.$15)),
     CURRENT_TIMESTAMP()',
    't.$1,t.$14,t.$15',
    'LOAD_BRONZE_TRADES',
    'kgirija@defteam.co',
    'Y',
    'TRADE_ID'
);
-- ======================
-- FILE FORMAT OBJECT
-- ======================
CREATE OR REPLACE FILE FORMAT Bronze.Utils.csv_file_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TRIM_SPACE = TRUE
EMPTY_FIELD_AS_NULL = TRUE
NULL_IF = ('NULL', 'null', '')
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- ==============================
-- NAMED STAGE & DIRECTORY TABLE
-- ==============================
CREATE OR REPLACE STAGE BRONZE.FINANCE.BRONZE_STAGE
DIRECTORY = (ENABLE = TRUE)
FILE_FORMAT = BRONZE.UTILS.csv_file_format;

CREATE OR REPLACE STREAM BRONZE.FINANCE.STR_BRONZE_STAGE
ON DIRECTORY(@BRONZE.FINANCE.BRONZE_STAGE);
-- ==============
-- STREAMS
-- ==============
CREATE OR REPLACE STREAM BRONZE.FINANCE.Bronze_Customers_Stream 
ON TABLE BRONZE.FINANCE.CUSTOMERS;

CREATE OR REPLACE STREAM BRONZE.FINANCE.Bronze_Accounts_Stream 
ON TABLE BRONZE.FINANCE.ACCOUNTS;

CREATE OR REPLACE STREAM BRONZE.FINANCE.Bronze_CashTransactions_Stream 
ON TABLE BRONZE.FINANCE.CASH_TRANSACTIONS;

CREATE OR REPLACE STREAM BRONZE.FINANCE.Bronze_MarketPrices_Stream 
ON TABLE BRONZE.FINANCE.MARKET_PRICES;

CREATE OR REPLACE STREAM BRONZE.FINANCE.Bronze_Securities_Stream 
ON TABLE BRONZE.FINANCE.SECURITIES;

CREATE OR REPLACE STREAM BRONZE.FINANCE.Bronze_Trades_Stream 
ON TABLE BRONZE.FINANCE.TRADES;

-- ===============================
-- EMAIL NOTIFICATION INTEGRATION
-- ===============================
CREATE OR REPLACE NOTIFICATION INTEGRATION finance_email_notification
TYPE = EMAIL
ENABLED = TRUE
ALLOWED_RECIPIENTS = ('kgirija@defteam.co'); -- can provide any number of emails here, this will be the superset for SYSTEM$SEND_EMAIL procedure

-- ============================================================
-- TASK (for all tables): can create a new task for each table
-- =============================================================
CREATE OR REPLACE TASK BRONZE.FINANCE.BRONZE_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.FINANCE.STR_BRONZE_STAGE')
AS
BEGIN
    CALL BRONZE.UTILS.SP_DETECT_NEW_FILES('CUSTOMERS');
    CALL BRONZE.UTILS.SP_DETECT_NEW_FILES('ACCOUNTS');
    CALL BRONZE.UTILS.SP_DETECT_NEW_FILES('CASH_TRANSACTIONS');
    CALL BRONZE.UTILS.SP_DETECT_NEW_FILES('MARKET_PRICES');
    CALL BRONZE.UTILS.SP_DETECT_NEW_FILES('TRADES');
    CALL BRONZE.UTILS.SP_DETECT_NEW_FILES('SECURITIES');
END;

ALTER TASK BRONZE.FINANCE.BRONZE_TASK RESUME;

-- ALTER TASK BRONZE.FINANCE.BRONZE_TASK SUSPEND;