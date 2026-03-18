-- =============================================================================
-- Lakeflow Declarative Pipeline (LDP) - Medallion Architecture (SQL Version)
-- Financial Services Data Pipeline with Bronze, Silver, and Gold Layers
--
-- This pipeline processes financial account and transaction data through a medallion architecture:
-- - Bronze: Raw data ingestion with Change Data Feed
-- - Silver: Cleaned, validated data with SCD Type 2 for accounts/customers/transactions
-- - Gold: Business-ready analytics tables with materialized views
--
-- Target Catalog: pipeline_demo
-- Default Schema: gold
-- Source Schema: bronze (src_accounts, src_customer, src_acct_tx)
-- Silver Schema: silver (with SCD Type 2 and streaming)
-- Gold Schema: gold (materialized views for analytics)
-- =============================================================================

-- =============================================================================
-- BRONZE LAYER - Raw Data Ingestion with Change Data Feed
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE src_accounts_bronze_v1
COMMENT "Bronze layer for accounts data with CDC incremental loading from latest version"
TBLPROPERTIES (
  "delta.enableChangeDataFeed" = "true",
  "delta.autoOptimize.optimizeWrite" = "true",
  "delta.autoOptimize.autoCompact" = "true"
)
AS
SELECT
  * EXCEPT (_change_type, _commit_version, _commit_timestamp),
  current_timestamp() AS ingestion_timestamp,
  'accounts' AS source_file,
  _change_type AS operation_type,
  _commit_version AS commit_version,
  _commit_timestamp AS commit_timestamp
FROM STREAM pipeline_demo.bronze.src_accounts WITH (readChangeFeed);


CREATE OR REFRESH STREAMING TABLE src_customer_bronze_v1
COMMENT "Bronze layer for customer data with CDC incremental loading from latest version"
TBLPROPERTIES (
  "delta.enableChangeDataFeed" = "true",
  "delta.autoOptimize.optimizeWrite" = "true",
  "delta.autoOptimize.autoCompact" = "true"
)
AS
SELECT
  * EXCEPT (_change_type, _commit_version, _commit_timestamp, longitude, latitude),
  CAST(longitude AS DOUBLE) AS longitude,
  CAST(latitude AS DOUBLE) AS latitude,
  current_timestamp() AS ingestion_timestamp,
  'customer' AS source_file,
  _change_type AS operation_type,
  _commit_version AS commit_version,
  _commit_timestamp AS commit_timestamp
FROM STREAM pipeline_demo.bronze.src_customer WITH (readChangeFeed);


CREATE OR REFRESH STREAMING TABLE src_acct_tx_bronze_v1
COMMENT "Bronze layer for transaction data with CDC incremental loading from latest version"
TBLPROPERTIES (
  "delta.enableChangeDataFeed" = "true",
  "delta.autoOptimize.optimizeWrite" = "true",
  "delta.autoOptimize.autoCompact" = "true"
)
AS
SELECT
  * EXCEPT (_change_type, _commit_version, _commit_timestamp),
  current_timestamp() AS ingestion_timestamp,
  'transactions' AS source_file,
  _change_type AS operation_type,
  _commit_version AS commit_version,
  _commit_timestamp AS commit_timestamp
FROM STREAM pipeline_demo.bronze.src_acct_tx WITH (readChangeFeed);


-- =============================================================================
-- SILVER LAYER - Cleaned and Validated Data with SCD Type 2 and CDC
-- =============================================================================

-- =============================================================================
-- ACCOUNTS SILVER - AUTO CDC with SCD Type 2
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE accounts_silver_v1
(
  CONSTRAINT valid_acct_id EXPECT (acct_id IS NOT NULL),
  CONSTRAINT valid_balance EXPECT (balance >= 0.0),
  CONSTRAINT valid_acct_status EXPECT (acct_status IS NOT NULL)
)
COMMENT "SCD Type 2 streaming table for account data with AUTO CDC for INSERT and UPDATE operations"
TBLPROPERTIES (
  "delta.enableChangeDataFeed" = "true",
  "delta.autoOptimize.optimizeWrite" = "true",
  "delta.autoOptimize.autoCompact" = "true"
);

-- Apply changes with AUTO CDC - handles INSERT, UPDATE, and DELETE operations
-- APPLY AS DELETE: marks records as deleted in SCD Type 2 when operation_type = 'delete'
CREATE FLOW accounts_cdc_flow_v1 AS AUTO CDC INTO accounts_silver_v1
FROM STREAM(src_accounts_bronze_v1)
KEYS (acct_id)
APPLY AS DELETE WHEN operation_type = 'delete'
SEQUENCE BY commit_timestamp
COLUMNS * EXCEPT (operation_type, commit_version, commit_timestamp, source_file, created_date, ingestion_timestamp)
STORED AS SCD TYPE 2;


-- =============================================================================
-- CUSTOMERS SILVER - AUTO CDC with SCD Type 2
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE customers_silver_v1
(
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL),
  CONSTRAINT valid_acct_id EXPECT (acct_id IS NOT NULL),
  CONSTRAINT valid_state_cd EXPECT (state_cd IS NOT NULL AND length(state_cd) = 2),
  CONSTRAINT valid_coordinates EXPECT (longitude IS NOT NULL AND latitude IS NOT NULL)
)
COMMENT "SCD Type 2 streaming table for customer data with AUTO CDC for INSERT and UPDATE operations"
TBLPROPERTIES (
  "delta.enableChangeDataFeed" = "true",
  "delta.autoOptimize.optimizeWrite" = "true",
  "delta.autoOptimize.autoCompact" = "true"
);

-- Apply changes with AUTO CDC - handles INSERT, UPDATE, and DELETE operations
-- APPLY AS DELETE: marks records as deleted in SCD Type 2 when operation_type = 'delete'
CREATE FLOW customers_cdc_flow_v1 AS AUTO CDC INTO customers_silver_v1
FROM STREAM(src_customer_bronze_v1)
KEYS (customer_id)
APPLY AS DELETE WHEN operation_type = 'delete'
SEQUENCE BY commit_timestamp
COLUMNS * EXCEPT (operation_type, commit_version, commit_timestamp, source_file, created_at, ingestion_timestamp)
STORED AS SCD TYPE 2;


-- =============================================================================
-- TRANSACTIONS SILVER - AUTO CDC with SCD Type 2
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE transactions_silver_v1
(
  CONSTRAINT valid_transaction_id EXPECT (transaction_id IS NOT NULL),
  CONSTRAINT valid_acct_id EXPECT (acct_id IS NOT NULL),
  CONSTRAINT valid_total_amt EXPECT (total_amt IS NOT NULL AND total_amt > 0.0),
  CONSTRAINT valid_transaction_date EXPECT (transaction_date IS NOT NULL)
)
COMMENT "SCD Type 2 streaming table for transaction data with AUTO CDC for INSERT and UPDATE operations"
TBLPROPERTIES (
  "delta.enableChangeDataFeed" = "true",
  "delta.autoOptimize.optimizeWrite" = "true",
  "delta.autoOptimize.autoCompact" = "true"
);

-- Apply changes with AUTO CDC - handles INSERT, UPDATE, and DELETE operations
-- APPLY AS DELETE: marks records as deleted in SCD Type 2 when operation_type = 'delete'
CREATE FLOW transactions_cdc_flow_v1 AS AUTO CDC INTO transactions_silver_v1
FROM STREAM(src_acct_tx_bronze_v1)
KEYS (transaction_id)
APPLY AS DELETE WHEN operation_type = 'delete'
SEQUENCE BY commit_timestamp
COLUMNS * EXCEPT (operation_type, commit_version, commit_timestamp, source_file, created_date, ingestion_timestamp)
STORED AS SCD TYPE 2;


-- =============================================================================
-- GOLD LAYER - Business-Ready Analytics Materialized Views
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW cust_tran_profile_v1
(
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL),
  CONSTRAINT valid_acct_id EXPECT (acct_id IS NOT NULL),
  CONSTRAINT positive_transaction_count EXPECT (total_transactions > 0),
  CONSTRAINT positive_transaction_value EXPECT (total_transactions_value > 0.0),
  CONSTRAINT valid_date_range EXPECT (first_transaction_date <= latest_transaction_date)
)
COMMENT "Customer transaction profile materialized view for analytics and BI"
TBLPROPERTIES (
  "delta.autoOptimize.optimizeWrite" = "true",
  "delta.autoOptimize.autoCompact" = "true"
)
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
FROM accounts_silver_v1 AS src_accounts
INNER JOIN customers_silver_v1 AS src_customer
  ON src_accounts.acct_id = src_customer.acct_id
INNER JOIN transactions_silver_v1 AS src_acct_tx
  ON src_accounts.acct_id = src_acct_tx.acct_id
WHERE src_accounts.__END_AT IS NULL
  AND src_customer.__END_AT IS NULL
GROUP BY ALL;
