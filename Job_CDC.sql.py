# Databricks notebook source
# DBTITLE 1,Medallion CDC Pipeline - Standard Delta Operations
# MAGIC %sql
# MAGIC -- Databricks notebook source
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC # Lakeflow Medallion Architecture - Standard Delta Table Operations
# MAGIC -- MAGIC 
# MAGIC -- MAGIC This notebook converts the Spark Declarative Pipeline (SDP) into standard Delta table operations
# MAGIC -- MAGIC that can be scheduled as a **Job notebook task**.
# MAGIC -- MAGIC 
# MAGIC -- MAGIC **Architecture:**
# MAGIC -- MAGIC - **Bronze**: Read from source Delta tables with Change Data Feed, add metadata columns
# MAGIC -- MAGIC - **Silver**: SCD Type 2 (accounts, customers) and MERGE (transactions) with data quality checks
# MAGIC -- MAGIC - **Gold**: Aggregated customer transaction profile
# MAGIC -- MAGIC 
# MAGIC -- MAGIC **Source Tables:** `pipeline_demo.bronze.src_accounts`, `pipeline_demo.bronze.src_customer`, `pipeline_demo.bronze.src_acct_tx`
# MAGIC -- MAGIC 
# MAGIC -- MAGIC **Target Catalog:** `lf_demo_av` | **Schemas:** `bronze`, `silver`, `gold`
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC ## Setup - Create Schemas
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS lf_demo_av.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS lf_demo_av.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS lf_demo_av.gold;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC ---
# MAGIC -- MAGIC ## BRONZE LAYER - Raw Data Ingestion with Change Data Feed
# MAGIC -- MAGIC 
# MAGIC -- MAGIC Reads from source Delta tables using Change Data Feed columns (`_change_type`, `_commit_version`, `_commit_timestamp`).
# MAGIC -- MAGIC Adds ingestion metadata columns and stores as Delta tables with CDF enabled.
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC ### Bronze: Accounts
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS lf_demo_av.bronze.src_accounts_bronze (
# MAGIC   id BIGINT,
# MAGIC   acct_id STRING,
# MAGIC   acct_name STRING,
# MAGIC   acct_number BIGINT,
# MAGIC   acct_type STRING,
# MAGIC   balance BIGINT,
# MAGIC   acct_status STRING,
# MAGIC   open_date TIMESTAMP,
# MAGIC   closed_date TIMESTAMP,
# MAGIC   margin_enabled STRING,
# MAGIC   risk_tolerance STRING,
# MAGIC   created_date TIMESTAMP,
# MAGIC   ingestion_timestamp TIMESTAMP,
# MAGIC   source_file STRING,
# MAGIC   operation_type STRING,
# MAGIC   commit_version BIGINT,
# MAGIC   commit_timestamp TIMESTAMP
# MAGIC )
# MAGIC CLUSTER BY AUTO;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- Insert new changes from source using Change Data Feed
# MAGIC INSERT INTO lf_demo_av.bronze.src_accounts_bronze
# MAGIC SELECT
# MAGIC   id,
# MAGIC   acct_id,
# MAGIC   acct_name,
# MAGIC   acct_number,
# MAGIC   acct_type,
# MAGIC   balance,
# MAGIC   acct_status,
# MAGIC   open_date,
# MAGIC   closed_date,
# MAGIC   margin_enabled,
# MAGIC   risk_tolerance,
# MAGIC   created_date,
# MAGIC   current_timestamp() AS ingestion_timestamp,
# MAGIC   'accounts' AS source_file,
# MAGIC   _change_type AS operation_type,
# MAGIC   _commit_version AS commit_version,
# MAGIC   _commit_timestamp AS commit_timestamp
# MAGIC FROM table_changes('pipeline_demo.bronze.src_accounts', 0)
# MAGIC WHERE _change_type IN ('insert', 'update_postimage', 'delete');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC ### Bronze: Customers
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS lf_demo_av.bronze.src_customer_bronze (
# MAGIC   id BIGINT,
# MAGIC   customer_id BIGINT,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   email STRING,
# MAGIC   phone_number STRING,
# MAGIC   acct_id STRING,
# MAGIC   state_cd STRING,
# MAGIC   latitude DOUBLE,
# MAGIC   longitude DOUBLE,
# MAGIC   country_cd STRING,
# MAGIC   created_at TIMESTAMP,
# MAGIC   ingestion_timestamp TIMESTAMP,
# MAGIC   source_file STRING,
# MAGIC   operation_type STRING,
# MAGIC   commit_version BIGINT,
# MAGIC   commit_timestamp TIMESTAMP
# MAGIC )
# MAGIC CLUSTER BY AUTO;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- Insert new changes from source using Change Data Feed
# MAGIC INSERT INTO lf_demo_av.bronze.src_customer_bronze
# MAGIC SELECT
# MAGIC   id,
# MAGIC   customer_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   email,
# MAGIC   phone_number,
# MAGIC   acct_id,
# MAGIC   state_cd,
# MAGIC   CAST(latitude AS DOUBLE) AS latitude,
# MAGIC   CAST(longitude AS DOUBLE) AS longitude,
# MAGIC   country_cd,
# MAGIC   created_at,
# MAGIC   current_timestamp() AS ingestion_timestamp,
# MAGIC   'customer' AS source_file,
# MAGIC   _change_type AS operation_type,
# MAGIC   _commit_version AS commit_version,
# MAGIC   _commit_timestamp AS commit_timestamp
# MAGIC FROM table_changes('pipeline_demo.bronze.src_customer', 0)
# MAGIC WHERE _change_type IN ('insert', 'update_postimage', 'delete');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC ### Bronze: Transactions
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS lf_demo_av.bronze.src_acct_tx_bronze (
# MAGIC   id BIGINT,
# MAGIC   transaction_id STRING,
# MAGIC   acct_id STRING,
# MAGIC   ticker_symbol STRING,
# MAGIC   transaction_type STRING,
# MAGIC   quantity BIGINT,
# MAGIC   price_per_share DOUBLE,
# MAGIC   total_amt DOUBLE,
# MAGIC   transaction_date TIMESTAMP,
# MAGIC   order_type STRING,
# MAGIC   trade_status STRING,
# MAGIC   total_holdings BIGINT,
# MAGIC   created_date TIMESTAMP,
# MAGIC   ingestion_timestamp TIMESTAMP,
# MAGIC   source_file STRING,
# MAGIC   operation_type STRING,
# MAGIC   commit_version BIGINT,
# MAGIC   commit_timestamp TIMESTAMP
# MAGIC )
# MAGIC CLUSTER BY AUTO;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- Insert new changes from source using Change Data Feed
# MAGIC INSERT INTO lf_demo_av.bronze.src_acct_tx_bronze
# MAGIC SELECT
# MAGIC   id,
# MAGIC   transaction_id,
# MAGIC   acct_id,
# MAGIC   ticker_symbol,
# MAGIC   transaction_type,
# MAGIC   quantity,
# MAGIC   price_per_share,
# MAGIC   total_amt,
# MAGIC   transaction_date,
# MAGIC   order_type,
# MAGIC   trade_status,
# MAGIC   total_holdings,
# MAGIC   created_date,
# MAGIC   current_timestamp() AS ingestion_timestamp,
# MAGIC   'transactions' AS source_file,
# MAGIC   _change_type AS operation_type,
# MAGIC   _commit_version AS commit_version,
# MAGIC   _commit_timestamp AS commit_timestamp
# MAGIC FROM table_changes('pipeline_demo.bronze.src_acct_tx', 0)
# MAGIC WHERE _change_type IN ('insert', 'update_postimage', 'delete');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC ---
# MAGIC -- MAGIC ## SILVER LAYER - Cleaned & Validated Data with SCD Type 2
# MAGIC -- MAGIC 
# MAGIC -- MAGIC Implements:
# MAGIC -- MAGIC - **SCD Type 2** for `accounts_silver` and `customers_silver` (tracks history with `__START_AT` / `__END_AT`)
# MAGIC -- MAGIC - **MERGE INTO** for `transactions_silver` (upsert with delete handling)
# MAGIC -- MAGIC - **Data quality checks** via WHERE filters on incoming data
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC ### Silver: Accounts (SCD Type 2)
# MAGIC -- MAGIC 
# MAGIC -- MAGIC **Data Quality Expectations:**
# MAGIC -- MAGIC - `acct_id IS NOT NULL`
# MAGIC -- MAGIC - `balance >= 0`
# MAGIC -- MAGIC - `acct_status IS NOT NULL`
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS lf_demo_av.silver.accounts_silver (
# MAGIC   id BIGINT,
# MAGIC   acct_id STRING NOT NULL,
# MAGIC   acct_name STRING,
# MAGIC   acct_number BIGINT,
# MAGIC   acct_type STRING,
# MAGIC   balance BIGINT,
# MAGIC   acct_status STRING NOT NULL,
# MAGIC   open_date TIMESTAMP,
# MAGIC   closed_date TIMESTAMP,
# MAGIC   margin_enabled STRING,
# MAGIC   risk_tolerance STRING,
# MAGIC   __START_AT TIMESTAMP NOT NULL,
# MAGIC   __END_AT TIMESTAMP
# MAGIC )
# MAGIC CLUSTER BY AUTO;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- SCD Type 2 MERGE: Close existing records and insert new versions
# MAGIC -- Step 1: Identify changes from bronze (with data quality filter)
# MAGIC MERGE INTO lf_demo_av.silver.accounts_silver AS target
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     id,
# MAGIC     acct_id,
# MAGIC     acct_name,
# MAGIC     acct_number,
# MAGIC     acct_type,
# MAGIC     balance,
# MAGIC     acct_status,
# MAGIC     open_date,
# MAGIC     closed_date,
# MAGIC     margin_enabled,
# MAGIC     risk_tolerance,
# MAGIC     operation_type,
# MAGIC     commit_timestamp
# MAGIC   FROM lf_demo_av.bronze.src_accounts_bronze
# MAGIC   -- Data Quality Expectations
# MAGIC   WHERE acct_id IS NOT NULL
# MAGIC     AND balance >= 0
# MAGIC     AND acct_status IS NOT NULL
# MAGIC ) AS source
# MAGIC ON target.acct_id = source.acct_id AND target.__END_AT IS NULL
# MAGIC WHEN MATCHED AND source.operation_type = 'delete' THEN
# MAGIC   -- Close record for deletes
# MAGIC   UPDATE SET __END_AT = source.commit_timestamp
# MAGIC WHEN MATCHED AND source.operation_type IN ('update_postimage', 'insert') THEN
# MAGIC   -- Close current record (new version will be inserted below)
# MAGIC   UPDATE SET __END_AT = source.commit_timestamp
# MAGIC WHEN NOT MATCHED AND source.operation_type != 'delete' THEN
# MAGIC   -- Insert new record
# MAGIC   INSERT (id, acct_id, acct_name, acct_number, acct_type, balance, acct_status, open_date, closed_date, margin_enabled, risk_tolerance, __START_AT, __END_AT)
# MAGIC   VALUES (source.id, source.acct_id, source.acct_name, source.acct_number, source.acct_type, source.balance, source.acct_status, source.open_date, source.closed_date, source.margin_enabled, source.risk_tolerance, source.commit_timestamp, NULL);
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- Step 2: Insert new versions for updated records (SCD Type 2 new row)
# MAGIC INSERT INTO lf_demo_av.silver.accounts_silver
# MAGIC SELECT
# MAGIC   b.id,
# MAGIC   b.acct_id,
# MAGIC   b.acct_name,
# MAGIC   b.acct_number,
# MAGIC   b.acct_type,
# MAGIC   b.balance,
# MAGIC   b.acct_status,
# MAGIC   b.open_date,
# MAGIC   b.closed_date,
# MAGIC   b.margin_enabled,
# MAGIC   b.risk_tolerance,
# MAGIC   b.commit_timestamp AS __START_AT,
# MAGIC   NULL AS __END_AT
# MAGIC FROM lf_demo_av.bronze.src_accounts_bronze b
# MAGIC INNER JOIN lf_demo_av.silver.accounts_silver s
# MAGIC   ON b.acct_id = s.acct_id
# MAGIC   AND s.__END_AT = b.commit_timestamp  -- Match records we just closed
# MAGIC WHERE b.operation_type = 'update_postimage'
# MAGIC   AND b.acct_id IS NOT NULL
# MAGIC   AND b.balance >= 0
# MAGIC   AND b.acct_status IS NOT NULL;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC ### Silver: Customers (SCD Type 2)
# MAGIC -- MAGIC 
# MAGIC -- MAGIC **Data Quality Expectations:**
# MAGIC -- MAGIC - `customer_id IS NOT NULL`
# MAGIC -- MAGIC - `acct_id IS NOT NULL`
# MAGIC -- MAGIC - `state_cd IS NOT NULL AND length(state_cd) = 2`
# MAGIC -- MAGIC - `longitude IS NOT NULL AND latitude IS NOT NULL`
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS lf_demo_av.silver.customers_silver (
# MAGIC   id BIGINT,
# MAGIC   customer_id BIGINT NOT NULL,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   email STRING,
# MAGIC   phone_number STRING,
# MAGIC   acct_id STRING NOT NULL,
# MAGIC   state_cd STRING NOT NULL,
# MAGIC   latitude DOUBLE,
# MAGIC   longitude DOUBLE,
# MAGIC   country_cd STRING,
# MAGIC   __START_AT TIMESTAMP NOT NULL,
# MAGIC   __END_AT TIMESTAMP
# MAGIC )
# MAGIC CLUSTER BY AUTO;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- SCD Type 2 MERGE: Close existing records and insert new versions
# MAGIC MERGE INTO lf_demo_av.silver.customers_silver AS target
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     id,
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     email,
# MAGIC     phone_number,
# MAGIC     acct_id,
# MAGIC     state_cd,
# MAGIC     latitude,
# MAGIC     longitude,
# MAGIC     country_cd,
# MAGIC     operation_type,
# MAGIC     commit_timestamp
# MAGIC   FROM lf_demo_av.bronze.src_customer_bronze
# MAGIC   -- Data Quality Expectations
# MAGIC   WHERE customer_id IS NOT NULL
# MAGIC     AND acct_id IS NOT NULL
# MAGIC     AND state_cd IS NOT NULL AND length(state_cd) = 2
# MAGIC     AND longitude IS NOT NULL AND latitude IS NOT NULL
# MAGIC ) AS source
# MAGIC ON target.customer_id = source.customer_id AND target.__END_AT IS NULL
# MAGIC WHEN MATCHED AND source.operation_type = 'delete' THEN
# MAGIC   UPDATE SET __END_AT = source.commit_timestamp
# MAGIC WHEN MATCHED AND source.operation_type IN ('update_postimage', 'insert') THEN
# MAGIC   UPDATE SET __END_AT = source.commit_timestamp
# MAGIC WHEN NOT MATCHED AND source.operation_type != 'delete' THEN
# MAGIC   INSERT (id, customer_id, first_name, last_name, email, phone_number, acct_id, state_cd, latitude, longitude, country_cd, __START_AT, __END_AT)
# MAGIC   VALUES (source.id, source.customer_id, source.first_name, source.last_name, source.email, source.phone_number, source.acct_id, source.state_cd, source.latitude, source.longitude, source.country_cd, source.commit_timestamp, NULL);
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- Step 2: Insert new versions for updated records (SCD Type 2 new row)
# MAGIC INSERT INTO lf_demo_av.silver.customers_silver
# MAGIC SELECT
# MAGIC   b.id,
# MAGIC   b.customer_id,
# MAGIC   b.first_name,
# MAGIC   b.last_name,
# MAGIC   b.email,
# MAGIC   b.phone_number,
# MAGIC   b.acct_id,
# MAGIC   b.state_cd,
# MAGIC   b.latitude,
# MAGIC   b.longitude,
# MAGIC   b.country_cd,
# MAGIC   b.commit_timestamp AS __START_AT,
# MAGIC   NULL AS __END_AT
# MAGIC FROM lf_demo_av.bronze.src_customer_bronze b
# MAGIC INNER JOIN lf_demo_av.silver.customers_silver s
# MAGIC   ON b.customer_id = s.customer_id
# MAGIC   AND s.__END_AT = b.commit_timestamp
# MAGIC WHERE b.operation_type = 'update_postimage'
# MAGIC   AND b.customer_id IS NOT NULL
# MAGIC   AND b.acct_id IS NOT NULL
# MAGIC   AND b.state_cd IS NOT NULL AND length(b.state_cd) = 2
# MAGIC   AND b.longitude IS NOT NULL AND b.latitude IS NOT NULL;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC ### Silver: Transactions (MERGE with SCD Type 2)
# MAGIC -- MAGIC 
# MAGIC -- MAGIC **Data Quality Expectations:**
# MAGIC -- MAGIC - `transaction_id IS NOT NULL`
# MAGIC -- MAGIC - `acct_id IS NOT NULL`
# MAGIC -- MAGIC - `total_amt IS NOT NULL AND total_amt > 0`
# MAGIC -- MAGIC - `transaction_date IS NOT NULL`
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS lf_demo_av.silver.transactions_silver (
# MAGIC   id BIGINT,
# MAGIC   transaction_id STRING NOT NULL,
# MAGIC   acct_id STRING NOT NULL,
# MAGIC   ticker_symbol STRING,
# MAGIC   transaction_type STRING,
# MAGIC   quantity BIGINT,
# MAGIC   price_per_share DOUBLE,
# MAGIC   total_amt DOUBLE,
# MAGIC   transaction_date TIMESTAMP NOT NULL,
# MAGIC   order_type STRING,
# MAGIC   trade_status STRING,
# MAGIC   total_holdings BIGINT,
# MAGIC   __START_AT TIMESTAMP NOT NULL,
# MAGIC   __END_AT TIMESTAMP
# MAGIC )
# MAGIC CLUSTER BY AUTO;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- SCD Type 2 MERGE for transactions
# MAGIC MERGE INTO lf_demo_av.silver.transactions_silver AS target
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     id,
# MAGIC     transaction_id,
# MAGIC     acct_id,
# MAGIC     ticker_symbol,
# MAGIC     transaction_type,
# MAGIC     quantity,
# MAGIC     price_per_share,
# MAGIC     total_amt,
# MAGIC     transaction_date,
# MAGIC     order_type,
# MAGIC     trade_status,
# MAGIC     total_holdings,
# MAGIC     operation_type,
# MAGIC     commit_timestamp
# MAGIC   FROM lf_demo_av.bronze.src_acct_tx_bronze
# MAGIC   -- Data Quality Expectations
# MAGIC   WHERE transaction_id IS NOT NULL
# MAGIC     AND acct_id IS NOT NULL
# MAGIC     AND total_amt IS NOT NULL AND total_amt > 0.0
# MAGIC     AND transaction_date IS NOT NULL
# MAGIC ) AS source
# MAGIC ON target.transaction_id = source.transaction_id AND target.__END_AT IS NULL
# MAGIC WHEN MATCHED AND source.operation_type = 'delete' THEN
# MAGIC   UPDATE SET __END_AT = source.commit_timestamp
# MAGIC WHEN MATCHED AND source.operation_type IN ('update_postimage', 'insert') THEN
# MAGIC   UPDATE SET __END_AT = source.commit_timestamp
# MAGIC WHEN NOT MATCHED AND source.operation_type != 'delete' THEN
# MAGIC   INSERT (id, transaction_id, acct_id, ticker_symbol, transaction_type, quantity, price_per_share, total_amt, transaction_date, order_type, trade_status, total_holdings, __START_AT, __END_AT)
# MAGIC   VALUES (source.id, source.transaction_id, source.acct_id, source.ticker_symbol, source.transaction_type, source.quantity, source.price_per_share, source.total_amt, source.transaction_date, source.order_type, source.trade_status, source.total_holdings, source.commit_timestamp, NULL);
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- Step 2: Insert new versions for updated transactions
# MAGIC INSERT INTO lf_demo_av.silver.transactions_silver
# MAGIC SELECT
# MAGIC   b.id,
# MAGIC   b.transaction_id,
# MAGIC   b.acct_id,
# MAGIC   b.ticker_symbol,
# MAGIC   b.transaction_type,
# MAGIC   b.quantity,
# MAGIC   b.price_per_share,
# MAGIC   b.total_amt,
# MAGIC   b.transaction_date,
# MAGIC   b.order_type,
# MAGIC   b.trade_status,
# MAGIC   b.total_holdings,
# MAGIC   b.commit_timestamp AS __START_AT,
# MAGIC   NULL AS __END_AT
# MAGIC FROM lf_demo_av.bronze.src_acct_tx_bronze b
# MAGIC INNER JOIN lf_demo_av.silver.transactions_silver s
# MAGIC   ON b.transaction_id = s.transaction_id
# MAGIC   AND s.__END_AT = b.commit_timestamp
# MAGIC WHERE b.operation_type = 'update_postimage'
# MAGIC   AND b.transaction_id IS NOT NULL
# MAGIC   AND b.acct_id IS NOT NULL
# MAGIC   AND b.total_amt IS NOT NULL AND b.total_amt > 0.0
# MAGIC   AND b.transaction_date IS NOT NULL;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC ---
# MAGIC -- MAGIC ## GOLD LAYER - Business-Ready Analytics
# MAGIC -- MAGIC 
# MAGIC -- MAGIC Joins accounts, customers (current records only, `__END_AT IS NULL`), and transactions
# MAGIC -- MAGIC to create an aggregated customer transaction profile.
# MAGIC -- MAGIC 
# MAGIC -- MAGIC **Data Quality Expectations:**
# MAGIC -- MAGIC - `customer_id IS NOT NULL`
# MAGIC -- MAGIC - `acct_id IS NOT NULL`
# MAGIC -- MAGIC - `total_transactions > 0`
# MAGIC -- MAGIC - `total_transactions_value > 0`
# MAGIC -- MAGIC - `first_transaction_date <= latest_transaction_date`
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE OR REPLACE TABLE lf_demo_av.gold.cust_tran_profile
# MAGIC CLUSTER BY AUTO
# MAGIC AS
# MAGIC SELECT
# MAGIC   src_customer.customer_id,
# MAGIC   src_accounts.acct_id,
# MAGIC   src_accounts.acct_status,
# MAGIC   src_accounts.balance,
# MAGIC   COUNT(*) AS total_transactions,
# MAGIC   CAST(SUM(src_acct_tx.total_amt) AS DOUBLE) AS total_transactions_value,
# MAGIC   MIN(src_acct_tx.transaction_date) AS first_transaction_date,
# MAGIC   MAX(src_acct_tx.transaction_date) AS latest_transaction_date,
# MAGIC   COUNT(DISTINCT src_acct_tx.ticker_symbol) AS count_distinct_tickers_traded,
# MAGIC   current_timestamp() AS profile_created_at
# MAGIC FROM lf_demo_av.silver.accounts_silver src_accounts
# MAGIC INNER JOIN lf_demo_av.silver.customers_silver src_customer
# MAGIC   ON src_accounts.acct_id = src_customer.acct_id
# MAGIC INNER JOIN lf_demo_av.silver.transactions_silver src_acct_tx
# MAGIC   ON src_accounts.acct_id = src_acct_tx.acct_id
# MAGIC -- Only current records from SCD Type 2 tables
# MAGIC WHERE src_accounts.__END_AT IS NULL
# MAGIC   AND src_customer.__END_AT IS NULL
# MAGIC -- Data Quality Checks
# MAGIC   AND src_customer.customer_id IS NOT NULL
# MAGIC   AND src_accounts.acct_id IS NOT NULL
# MAGIC GROUP BY ALL
# MAGIC -- Post-aggregation quality filter
# MAGIC HAVING total_transactions > 0
# MAGIC   AND total_transactions_value > 0.0
# MAGIC   AND MIN(src_acct_tx.transaction_date) <= MAX(src_acct_tx.transaction_date);
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC ---
# MAGIC -- MAGIC ## Validation: Row Counts
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC SELECT 'bronze.src_accounts_bronze' AS table_name, COUNT(*) AS row_count FROM lf_demo_av.bronze.src_accounts_bronze
# MAGIC UNION ALL
# MAGIC SELECT 'bronze.src_customer_bronze', COUNT(*) FROM lf_demo_av.bronze.src_customer_bronze
# MAGIC UNION ALL
# MAGIC SELECT 'bronze.src_acct_tx_bronze', COUNT(*) FROM lf_demo_av.bronze.src_acct_tx_bronze
# MAGIC UNION ALL
# MAGIC SELECT 'silver.accounts_silver', COUNT(*) FROM lf_demo_av.silver.accounts_silver
# MAGIC UNION ALL
# MAGIC SELECT 'silver.customers_silver', COUNT(*) FROM lf_demo_av.silver.customers_silver
# MAGIC UNION ALL
# MAGIC SELECT 'silver.transactions_silver', COUNT(*) FROM lf_demo_av.silver.transactions_silver
# MAGIC UNION ALL
# MAGIC SELECT 'gold.cust_tran_profile', COUNT(*) FROM lf_demo_av.gold.cust_tran_profile;
