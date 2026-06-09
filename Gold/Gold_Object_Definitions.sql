CREATE DATABASE IF NOT EXISTS GOLD;
USE DATABASE GOLD;

CREATE SCHEMA IF NOT EXISTS FINANCE;
USE SCHEMA FINANCE;

CREATE SCHEMA IF NOT EXISTS UTILS;

-- ==============================
-- Table Definitions
-- ==============================
-- DIM_CUSTOMER table
CREATE TABLE gold.finance.dim_customer ( 
    customer_sk INT AUTOINCREMENT, 
    customer_id STRING, 
    customer_type STRING, 
    customer_display_name STRING, 
    email STRING, 
    phone_number STRING, 
    tax_identifier STRING, 
    risk_profile STRING, 
    kyc_status STRING, 
    effective_start_date DATE, 
    effective_end_date DATE, 
    is_current BOOLEAN, 
    record_hash STRING 
); 
-- DIM_ACCOUNT table
CREATE TABLE gold.finance.dim_account ( 
    account_sk INT AUTOINCREMENT, 
    account_id STRING, 
    customer_id STRING, 
    account_number STRING, 
    account_type STRING, 
    account_status STRING, 
    base_currency STRING, 
    opened_date DATE, 
    closed_date DATE, 
    advisor_code STRING, 
    effective_start_date DATE, 
    effective_end_date DATE, 
    is_current BOOLEAN, 
    record_hash STRING 
); 
-- DIM_SEDURITY table
CREATE TABLE gold.finance.dim_security ( 
    security_sk INT AUTOINCREMENT, 
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
    effective_start_date DATE, 
    effective_end_date DATE, 
    is_current BOOLEAN, 
    record_hash STRING 
); 
-- FACT_TRADE table
CREATE TABLE gold.finance.fact_trade ( 
    trade_sk INT AUTOINCREMENT, 
    trade_id STRING, 
    account_sk INT, 
    customer_sk INT, 
    security_sk INT, 
    trade_date DATE, 
    settlement_date DATE, 
    trade_type STRING, 
    quantity NUMBER(18,4), 
    trade_price NUMBER(18,6), 
    gross_trade_amount NUMBER(18,4), 
    brokerage_amount NUMBER(18,4), 
    tax_amount NUMBER(18,4), 
    exchange_fee NUMBER(18,4), 
    net_trade_amount NUMBER(18,4), 
    signed_quantity NUMBER(18,4), 
    trade_currency STRING, 
    trade_status STRING, 
    load_timestamp TIMESTAMP 
); 
-- FACT_CASH_TRANSACTION table
CREATE TABLE gold.finance.fact_cash_transaction ( 
    cash_transaction_sk INT AUTOINCREMENT, 
    cash_transaction_id STRING, 
    account_sk INT, 
    customer_sk INT, 
    transaction_date DATE, 
    transaction_type STRING, 
    amount NUMBER(18,4), 
    fee_amount NUMBER(18,4), 
    net_cash_amount NUMBER(18,4), 
    signed_cash_flow NUMBER(18,4), 
    currency STRING, 
    transaction_status STRING, 
    load_timestamp TIMESTAMP 
); 
-- FACT_PORTFOLIO_POSITION
CREATE TABLE gold.finance.fact_portfolio_position ( 
    portfolio_position_sk INT AUTOINCREMENT, 
    account_sk INT, 
    customer_sk INT, 
    security_sk INT, 
    position_date DATE, 
    total_buy_quantity NUMBER(18,4), 
    total_sell_quantity NUMBER(18,4), 
    net_quantity NUMBER(18,4), 
    average_buy_price NUMBER(18,6), 
    cost_basis_amount NUMBER(18,4), 
    market_price NUMBER(18,6), 
    market_value NUMBER(18,4), 
    unrealized_gain_loss NUMBER(18,4), 
    unrealized_gain_loss_pct NUMBER(10,4), 
    portfolio_weight_pct NUMBER(10,4), 
    base_currency STRING, 
    load_timestamp TIMESTAMP 
); 

-- AUDIT_JOB_LOG table
create or replace TABLE GOLD.FINANCE.AUDIT_JOB_LOG (
	JOB_ID VARCHAR(255),
	JOB_NAME VARCHAR(255),
	LAYER_NAME VARCHAR(255),
	SOURCE_OBJECT VARCHAR(255),
	TARGET_OBJECT VARCHAR(255),
	START_TIME TIMESTAMP_NTZ(9),
	END_TIME TIMESTAMP_NTZ(9),
	ROWS_PROCESSED NUMBER(38,0),
	ROWS_INSERTED NUMBER(38,0),
	ROWS_UPDATED NUMBER(38,0),
	ROWS_FAILED NUMBER(38,0),
	JOB_STATUS VARCHAR(255),
	ERROR_MESSAGE VARCHAR(1000),
	CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);

-- ================================
-- Notebook Project
-- ================================
CREATE OR REPLACE NOTEBOOK PROJECT GOLD.FINANCE.DIM_CUSTOMER_PROJECT
  FROM 'snow://workspace/USER$.PUBLIC."Finance_Assessment"/versions/last'
  COMMENT = 'Notebook project for DIM_CUSTOMER Gold layer load';

-- ================================
-- Tasks to execute notebooks
-- ================================
CREATE OR REPLACE TASK GOLD.FINANCE.DIM_CUSTOMER_LOAD
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('SILVER.FINANCE.SILVER_CUSTOMERS_STREAM')
AS
EXECUTE NOTEBOOK PROJECT GOLD.FINANCE.DIM_CUSTOMER_PROJECT
  MAIN_FILE = 'GOLD/dim_customer.ipynb'
  COMPUTE_POOL = 'SYSTEM_COMPUTE_POOL_CPU'
  QUERY_WAREHOUSE = 'COMPUTE_WH'
  RUNTIME = 'V2.2-CPU-PY3.11';
-- EXECUTE NOTEBOOK PROJECT GOLD.FINANCE.DIM_CUSTOMER_PROJECT
--   MAIN_FILE = 'GOLD/dim_customer.ipynb'
--   COMPUTE_POOL = 'COMPUTE_POOL'
--   QUERY_WAREHOUSE = 'COMPUTE_WH'
--   RUNTIME = 'V2.2-CPU-PY3.11';
--   -- EXECUTE NOTEBOOK PROJECT GOLD.FINANCE.DIM_CUSTOMER_PROJECT
  --   MAIN_FILE = 'GOLD/dim_customer.ipynb'
  --   COMPUTE_POOL = 'SYSTEM_COMPUTE_POOL_CPU'
  --   QUERY_WAREHOUSE = 'COMPUTE_WH'
  --   RUNTIME = '2.5-CPU-PY3.12';

ALTER TASK GOLD.FINANCE.DIM_CUSTOMER_LOAD RESUME;
-- Error:  Notebook resource not found: failed to get image URL: not_found: (505153) No notebook runtime environment found for label: 2.5-CPU-PY3.12:vnext

ALTER TASK GOLD.FINANCE.DIM_CUSTOMER_LOAD SUSPEND;

show tasks;

UPDATE gold.finance.DIM_CUSTOMER
SET EMAIL='erine.white@allen.com'
where customer_sk = 2;

select count(*) from SILVER.FINANCE.SILVER_CUSTOMERS_STREAM;