-- Databricks notebook source
-- DBTITLE 1,Medallion CDC Pipeline - Standard Delta Operations
-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Lakeflow Medallion Architecture - Standard Delta Table Operations
-- MAGIC 
-- MAGIC This notebook converts the Spark Declarative Pipeline (SDP) into standard Delta table operations
-- MAGIC that can be scheduled as a **Job notebook task**.
-- MAGIC 
-- MAGIC **Architecture:**
-- MAGIC - **Bronze**: Read from source Delta tables with Change Data Feed, add metadata columns
-- MAGIC - **Silver**: SCD Type 2 (accounts, customers) and MERGE (transactions) with data quality checks
-- MAGIC - **Gold**: Aggregated customer transaction profile
-- MAGIC 
-- MAGIC **Source Tables:** `pipeline_demo.bronze.src_accounts`, `pipeline_demo.bronze.src_customer`, `pipeline_demo.bronze.src_acct_tx`
-- MAGIC 
-- MAGIC **Target Catalog:** `lf_demo_av` | **Schemas:** `bronze`, `silver`, `gold`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setup - Create Schemas

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS lf_demo_av.bronze;
CREATE SCHEMA IF NOT EXISTS lf_demo_av.silver;
CREATE SCHEMA IF NOT EXISTS lf_demo_av.gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## BRONZE LAYER - Raw Data Ingestion with Change Data Feed
-- MAGIC 
-- MAGIC Reads from source Delta tables using Change Data Feed columns (`_change_type`, `_commit_version`, `_commit_timestamp`).
-- MAGIC Adds ingestion metadata columns and stores as Delta tables with CDF enabled.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Bronze: Accounts

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS lf_demo_av.bronze.src_accounts_bronze (
  id BIGINT,
  acct_id STRING,
  acct_name STRING,
  acct_number BIGINT,
  acct_type STRING,
  balance BIGINT,
  acct_status STRING,
  open_date TIMESTAMP,
  closed_date TIMESTAMP,
  margin_enabled STRING,
  risk_tolerance STRING,
  created_date TIMESTAMP,
  ingestion_timestamp TIMESTAMP,
  source_file STRING,
  operation_type STRING,
  commit_version BIGINT,
  commit_timestamp TIMESTAMP
)
CLUSTER BY AUTO;

-- COMMAND ----------

-- Insert new changes from source using Change Data Feed
INSERT INTO lf_demo_av.bronze.src_accounts_bronze
SELECT
  id,
  acct_id,
  acct_name,
  acct_number,
  acct_type,
  balance,
  acct_status,
  open_date,
  closed_date,
  margin_enabled,
  risk_tolerance,
  created_date,
  current_timestamp() AS ingestion_timestamp,
  'accounts' AS source_file,
  _change_type AS operation_type,
  _commit_version AS commit_version,
  _commit_timestamp AS commit_timestamp
FROM table_changes('pipeline_demo.bronze.src_accounts', 0)
WHERE _change_type IN ('insert', 'update_postimage', 'delete');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Bronze: Customers

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS lf_demo_av.bronze.src_customer_bronze (
  id BIGINT,
  customer_id BIGINT,
  first_name STRING,
  last_name STRING,
  email STRING,
  phone_number STRING,
  acct_id STRING,
  state_cd STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  country_cd STRING,
  created_at TIMESTAMP,
  ingestion_timestamp TIMESTAMP,
  source_file STRING,
  operation_type STRING,
  commit_version BIGINT,
  commit_timestamp TIMESTAMP
)
CLUSTER BY AUTO;

-- COMMAND ----------

-- Insert new changes from source using Change Data Feed
INSERT INTO lf_demo_av.bronze.src_customer_bronze
SELECT
  id,
  customer_id,
  first_name,
  last_name,
  email,
  phone_number,
  acct_id,
  state_cd,
  CAST(latitude AS DOUBLE) AS latitude,
  CAST(longitude AS DOUBLE) AS longitude,
  country_cd,
  created_at,
  current_timestamp() AS ingestion_timestamp,
  'customer' AS source_file,
  _change_type AS operation_type,
  _commit_version AS commit_version,
  _commit_timestamp AS commit_timestamp
FROM table_changes('pipeline_demo.bronze.src_customer', 0)
WHERE _change_type IN ('insert', 'update_postimage', 'delete');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Bronze: Transactions

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS lf_demo_av.bronze.src_acct_tx_bronze (
  id BIGINT,
  transaction_id STRING,
  acct_id STRING,
  ticker_symbol STRING,
  transaction_type STRING,
  quantity BIGINT,
  price_per_share DOUBLE,
  total_amt DOUBLE,
  transaction_date TIMESTAMP,
  order_type STRING,
  trade_status STRING,
  total_holdings BIGINT,
  created_date TIMESTAMP,
  ingestion_timestamp TIMESTAMP,
  source_file STRING,
  operation_type STRING,
  commit_version BIGINT,
  commit_timestamp TIMESTAMP
)
CLUSTER BY AUTO;

-- COMMAND ----------

-- Insert new changes from source using Change Data Feed
INSERT INTO lf_demo_av.bronze.src_acct_tx_bronze
SELECT
  id,
  transaction_id,
  acct_id,
  ticker_symbol,
  transaction_type,
  quantity,
  price_per_share,
  total_amt,
  transaction_date,
  order_type,
  trade_status,
  total_holdings,
  created_date,
  current_timestamp() AS ingestion_timestamp,
  'transactions' AS source_file,
  _change_type AS operation_type,
  _commit_version AS commit_version,
  _commit_timestamp AS commit_timestamp
FROM table_changes('pipeline_demo.bronze.src_acct_tx', 0)
WHERE _change_type IN ('insert', 'update_postimage', 'delete');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## SILVER LAYER - Cleaned & Validated Data with SCD Type 2
-- MAGIC 
-- MAGIC Implements:
-- MAGIC - **SCD Type 2** for `accounts_silver` and `customers_silver` (tracks history with `__START_AT` / `__END_AT`)
-- MAGIC - **MERGE INTO** for `transactions_silver` (upsert with delete handling)
-- MAGIC - **Data quality checks** via WHERE filters on incoming data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Silver: Accounts (SCD Type 2)
-- MAGIC 
-- MAGIC **Data Quality Expectations:**
-- MAGIC - `acct_id IS NOT NULL`
-- MAGIC - `balance >= 0`
-- MAGIC - `acct_status IS NOT NULL`

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS lf_demo_av.silver.accounts_silver (
  id BIGINT,
  acct_id STRING NOT NULL,
  acct_name STRING,
  acct_number BIGINT,
  acct_type STRING,
  balance BIGINT,
  acct_status STRING NOT NULL,
  open_date TIMESTAMP,
  closed_date TIMESTAMP,
  margin_enabled STRING,
  risk_tolerance STRING,
  __START_AT TIMESTAMP NOT NULL,
  __END_AT TIMESTAMP
)
CLUSTER BY AUTO;

-- COMMAND ----------

-- SCD Type 2 MERGE: Close existing records and insert new versions
-- Step 1: Identify changes from bronze (with data quality filter)
MERGE INTO lf_demo_av.silver.accounts_silver AS target
USING (
  SELECT
    id,
    acct_id,
    acct_name,
    acct_number,
    acct_type,
    balance,
    acct_status,
    open_date,
    closed_date,
    margin_enabled,
    risk_tolerance,
    operation_type,
    commit_timestamp
  FROM lf_demo_av.bronze.src_accounts_bronze
  -- Data Quality Expectations
  WHERE acct_id IS NOT NULL
    AND balance >= 0
    AND acct_status IS NOT NULL
) AS source
ON target.acct_id = source.acct_id AND target.__END_AT IS NULL
WHEN MATCHED AND source.operation_type = 'delete' THEN
  -- Close record for deletes
  UPDATE SET __END_AT = source.commit_timestamp
WHEN MATCHED AND source.operation_type IN ('update_postimage', 'insert') THEN
  -- Close current record (new version will be inserted below)
  UPDATE SET __END_AT = source.commit_timestamp
WHEN NOT MATCHED AND source.operation_type != 'delete' THEN
  -- Insert new record
  INSERT (id, acct_id, acct_name, acct_number, acct_type, balance, acct_status, open_date, closed_date, margin_enabled, risk_tolerance, __START_AT, __END_AT)
  VALUES (source.id, source.acct_id, source.acct_name, source.acct_number, source.acct_type, source.balance, source.acct_status, source.open_date, source.closed_date, source.margin_enabled, source.risk_tolerance, source.commit_timestamp, NULL);

-- COMMAND ----------

-- Step 2: Insert new versions for updated records (SCD Type 2 new row)
INSERT INTO lf_demo_av.silver.accounts_silver
SELECT
  b.id,
  b.acct_id,
  b.acct_name,
  b.acct_number,
  b.acct_type,
  b.balance,
  b.acct_status,
  b.open_date,
  b.closed_date,
  b.margin_enabled,
  b.risk_tolerance,
  b.commit_timestamp AS __START_AT,
  NULL AS __END_AT
FROM lf_demo_av.bronze.src_accounts_bronze b
INNER JOIN lf_demo_av.silver.accounts_silver s
  ON b.acct_id = s.acct_id
  AND s.__END_AT = b.commit_timestamp  -- Match records we just closed
WHERE b.operation_type = 'update_postimage'
  AND b.acct_id IS NOT NULL
  AND b.balance >= 0
  AND b.acct_status IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Silver: Customers (SCD Type 2)
-- MAGIC 
-- MAGIC **Data Quality Expectations:**
-- MAGIC - `customer_id IS NOT NULL`
-- MAGIC - `acct_id IS NOT NULL`
-- MAGIC - `state_cd IS NOT NULL AND length(state_cd) = 2`
-- MAGIC - `longitude IS NOT NULL AND latitude IS NOT NULL`

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS lf_demo_av.silver.customers_silver (
  id BIGINT,
  customer_id BIGINT NOT NULL,
  first_name STRING,
  last_name STRING,
  email STRING,
  phone_number STRING,
  acct_id STRING NOT NULL,
  state_cd STRING NOT NULL,
  latitude DOUBLE,
  longitude DOUBLE,
  country_cd STRING,
  __START_AT TIMESTAMP NOT NULL,
  __END_AT TIMESTAMP
)
CLUSTER BY AUTO;

-- COMMAND ----------

-- SCD Type 2 MERGE: Close existing records and insert new versions
MERGE INTO lf_demo_av.silver.customers_silver AS target
USING (
  SELECT
    id,
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    acct_id,
    state_cd,
    latitude,
    longitude,
    country_cd,
    operation_type,
    commit_timestamp
  FROM lf_demo_av.bronze.src_customer_bronze
  -- Data Quality Expectations
  WHERE customer_id IS NOT NULL
    AND acct_id IS NOT NULL
    AND state_cd IS NOT NULL AND length(state_cd) = 2
    AND longitude IS NOT NULL AND latitude IS NOT NULL
) AS source
ON target.customer_id = source.customer_id AND target.__END_AT IS NULL
WHEN MATCHED AND source.operation_type = 'delete' THEN
  UPDATE SET __END_AT = source.commit_timestamp
WHEN MATCHED AND source.operation_type IN ('update_postimage', 'insert') THEN
  UPDATE SET __END_AT = source.commit_timestamp
WHEN NOT MATCHED AND source.operation_type != 'delete' THEN
  INSERT (id, customer_id, first_name, last_name, email, phone_number, acct_id, state_cd, latitude, longitude, country_cd, __START_AT, __END_AT)
  VALUES (source.id, source.customer_id, source.first_name, source.last_name, source.email, source.phone_number, source.acct_id, source.state_cd, source.latitude, source.longitude, source.country_cd, source.commit_timestamp, NULL);

-- COMMAND ----------

-- Step 2: Insert new versions for updated records (SCD Type 2 new row)
INSERT INTO lf_demo_av.silver.customers_silver
SELECT
  b.id,
  b.customer_id,
  b.first_name,
  b.last_name,
  b.email,
  b.phone_number,
  b.acct_id,
  b.state_cd,
  b.latitude,
  b.longitude,
  b.country_cd,
  b.commit_timestamp AS __START_AT,
  NULL AS __END_AT
FROM lf_demo_av.bronze.src_customer_bronze b
INNER JOIN lf_demo_av.silver.customers_silver s
  ON b.customer_id = s.customer_id
  AND s.__END_AT = b.commit_timestamp
WHERE b.operation_type = 'update_postimage'
  AND b.customer_id IS NOT NULL
  AND b.acct_id IS NOT NULL
  AND b.state_cd IS NOT NULL AND length(b.state_cd) = 2
  AND b.longitude IS NOT NULL AND b.latitude IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Silver: Transactions (MERGE with SCD Type 2)
-- MAGIC 
-- MAGIC **Data Quality Expectations:**
-- MAGIC - `transaction_id IS NOT NULL`
-- MAGIC - `acct_id IS NOT NULL`
-- MAGIC - `total_amt IS NOT NULL AND total_amt > 0`
-- MAGIC - `transaction_date IS NOT NULL`

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS lf_demo_av.silver.transactions_silver (
  id BIGINT,
  transaction_id STRING NOT NULL,
  acct_id STRING NOT NULL,
  ticker_symbol STRING,
  transaction_type STRING,
  quantity BIGINT,
  price_per_share DOUBLE,
  total_amt DOUBLE,
  transaction_date TIMESTAMP NOT NULL,
  order_type STRING,
  trade_status STRING,
  total_holdings BIGINT,
  __START_AT TIMESTAMP NOT NULL,
  __END_AT TIMESTAMP
)
CLUSTER BY AUTO;

-- COMMAND ----------

-- SCD Type 2 MERGE for transactions
MERGE INTO lf_demo_av.silver.transactions_silver AS target
USING (
  SELECT
    id,
    transaction_id,
    acct_id,
    ticker_symbol,
    transaction_type,
    quantity,
    price_per_share,
    total_amt,
    transaction_date,
    order_type,
    trade_status,
    total_holdings,
    operation_type,
    commit_timestamp
  FROM lf_demo_av.bronze.src_acct_tx_bronze
  -- Data Quality Expectations
  WHERE transaction_id IS NOT NULL
    AND acct_id IS NOT NULL
    AND total_amt IS NOT NULL AND total_amt > 0.0
    AND transaction_date IS NOT NULL
) AS source
ON target.transaction_id = source.transaction_id AND target.__END_AT IS NULL
WHEN MATCHED AND source.operation_type = 'delete' THEN
  UPDATE SET __END_AT = source.commit_timestamp
WHEN MATCHED AND source.operation_type IN ('update_postimage', 'insert') THEN
  UPDATE SET __END_AT = source.commit_timestamp
WHEN NOT MATCHED AND source.operation_type != 'delete' THEN
  INSERT (id, transaction_id, acct_id, ticker_symbol, transaction_type, quantity, price_per_share, total_amt, transaction_date, order_type, trade_status, total_holdings, __START_AT, __END_AT)
  VALUES (source.id, source.transaction_id, source.acct_id, source.ticker_symbol, source.transaction_type, source.quantity, source.price_per_share, source.total_amt, source.transaction_date, source.order_type, source.trade_status, source.total_holdings, source.commit_timestamp, NULL);

-- COMMAND ----------

-- Step 2: Insert new versions for updated transactions
INSERT INTO lf_demo_av.silver.transactions_silver
SELECT
  b.id,
  b.transaction_id,
  b.acct_id,
  b.ticker_symbol,
  b.transaction_type,
  b.quantity,
  b.price_per_share,
  b.total_amt,
  b.transaction_date,
  b.order_type,
  b.trade_status,
  b.total_holdings,
  b.commit_timestamp AS __START_AT,
  NULL AS __END_AT
FROM lf_demo_av.bronze.src_acct_tx_bronze b
INNER JOIN lf_demo_av.silver.transactions_silver s
  ON b.transaction_id = s.transaction_id
  AND s.__END_AT = b.commit_timestamp
WHERE b.operation_type = 'update_postimage'
  AND b.transaction_id IS NOT NULL
  AND b.acct_id IS NOT NULL
  AND b.total_amt IS NOT NULL AND b.total_amt > 0.0
  AND b.transaction_date IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## GOLD LAYER - Business-Ready Analytics
-- MAGIC 
-- MAGIC Joins accounts, customers (current records only, `__END_AT IS NULL`), and transactions
-- MAGIC to create an aggregated customer transaction profile.
-- MAGIC 
-- MAGIC **Data Quality Expectations:**
-- MAGIC - `customer_id IS NOT NULL`
-- MAGIC - `acct_id IS NOT NULL`
-- MAGIC - `total_transactions > 0`
-- MAGIC - `total_transactions_value > 0`
-- MAGIC - `first_transaction_date <= latest_transaction_date`

-- COMMAND ----------

CREATE OR REPLACE TABLE lf_demo_av.gold.cust_tran_profile
CLUSTER BY AUTO
AS
SELECT
  src_customer.customer_id,
  src_accounts.acct_id,
  src_accounts.acct_status,
  src_accounts.balance,
  COUNT(*) AS total_transactions,
  CAST(SUM(src_acct_tx.total_amt) AS DOUBLE) AS total_transactions_value,
  MIN(src_acct_tx.transaction_date) AS first_transaction_date,
  MAX(src_acct_tx.transaction_date) AS latest_transaction_date,
  COUNT(DISTINCT src_acct_tx.ticker_symbol) AS count_distinct_tickers_traded,
  current_timestamp() AS profile_created_at
FROM lf_demo_av.silver.accounts_silver src_accounts
INNER JOIN lf_demo_av.silver.customers_silver src_customer
  ON src_accounts.acct_id = src_customer.acct_id
INNER JOIN lf_demo_av.silver.transactions_silver src_acct_tx
  ON src_accounts.acct_id = src_acct_tx.acct_id
-- Only current records from SCD Type 2 tables
WHERE src_accounts.__END_AT IS NULL
  AND src_customer.__END_AT IS NULL
-- Data Quality Checks
  AND src_customer.customer_id IS NOT NULL
  AND src_accounts.acct_id IS NOT NULL
GROUP BY ALL
-- Post-aggregation quality filter
HAVING total_transactions > 0
  AND total_transactions_value > 0.0
  AND MIN(src_acct_tx.transaction_date) <= MAX(src_acct_tx.transaction_date);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Validation: Row Counts

-- COMMAND ----------

SELECT 'bronze.src_accounts_bronze' AS table_name, COUNT(*) AS row_count FROM lf_demo_av.bronze.src_accounts_bronze
UNION ALL
SELECT 'bronze.src_customer_bronze', COUNT(*) FROM lf_demo_av.bronze.src_customer_bronze
UNION ALL
SELECT 'bronze.src_acct_tx_bronze', COUNT(*) FROM lf_demo_av.bronze.src_acct_tx_bronze
UNION ALL
SELECT 'silver.accounts_silver', COUNT(*) FROM lf_demo_av.silver.accounts_silver
UNION ALL
SELECT 'silver.customers_silver', COUNT(*) FROM lf_demo_av.silver.customers_silver
UNION ALL
SELECT 'silver.transactions_silver', COUNT(*) FROM lf_demo_av.silver.transactions_silver
UNION ALL
SELECT 'gold.cust_tran_profile', COUNT(*) FROM lf_demo_av.gold.cust_tran_profile;
