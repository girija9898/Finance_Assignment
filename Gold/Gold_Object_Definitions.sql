CREATE DATABASE IF NOT EXISTS GOLD;
USE DATABASE GOLD;

CREATE SCHEMA IF NOT EXISTS FINANCE;
CREATE SCHEMA IF NOT EXISTS UTILS;

USE SCHEMA FINANCE;

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
-- FACT_MARKET_PRICE
create or replace TABLE gold.FINANCE.FACT_MARKET_PRICE (
    market_price_sk INT AUTOINCREMENT, 
    PRICE_ID VARCHAR(255),
    SECURITY_SK VARCHAR(255),
    POSITION_DATE DATE,
    OPEN_PRICE NUMBER(18,6),
    HIGH_PRICE NUMBER(18,6),
    LOW_PRICE NUMBER(18,6),
    CLOSE_PRICE NUMBER(18,6),
    ADJUSTED_CLOSE_PRICE NUMBER(18,6),
    PRICE_CURRENCY VARCHAR(255),
    FX_RATE_TO_BASE NUMBER(18,8),
    LOAD_TIMESTAMP TIMESTAMP
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

