# MAGIC %md
# MAGIC In this demo, we will read build a lakeflow pipeline that will read from Delta tables and process the data through medallion architecture to create materialized views for business consumption and analytics. 
# MAGIC By the end of this demo, you will be able to:
# MAGIC
# MAGIC * Build Lakeflow declarative pipeline
# MAGIC * Have pipeline read data from a Delta table and convert it into streaming tables
# MAGIC * Build silver layer of data sources with data quality expectations. Implement SCD Type 2 for master data. Load data in append mode for transaction data
# MAGIC * Create materialized views for business consumption and analytics by aggregating data from silver layer
# MAGIC * Run pipeline and visualize the data using Databricks SQL and Catalog explorer
# MAGIC * Visually explore statistics for each table and performance stats for each hop
# MAGIC

"""
Lakeflow Declarative Pipeline (LDP) - Medallion Architecture
Financial Services Data Pipeline with Bronze, Silver, and Gold Layers

This pipeline processes financial account and transaction data through a medallion architecture:
- Bronze: Raw data (already exists in lf_av_demo.bronze)
- Silver: Cleaned, validated data with SCD Type 2 for accounts/customers
- Gold: Business-ready analytics tables with materialized views

Target Catalog: lf_demo_av
Default Schema: silver
Source Schema: bronze (src_accounts, src_customer, src_acct_tx)
Silver Schema: silver (with SCD Type 2 and streaming)
Gold Schema: gold (materialized views for analytics)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# =============================================================================
# BRONZE LAYER - Raw Data Ingestion with Change Data Feed
# =============================================================================

@dp.table(
    name="src_accounts_bronze",
    comment="Bronze layer for accounts data with CDC incremental loading from latest version",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def src_accounts_bronze():
    """
    Bronze layer for accounts - CDC incremental loading starting from latest version
    """
    return (
        spark.readStream
        .option("readChangeFeed", "true")
     #   .option("startingVersion", "latest")
        .table("pipeline_demo.bronze.src_accounts")
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.lit("accounts"))
        .withColumn("operation_type", F.col("_change_type"))
        .withColumn("commit_version", F.col("_commit_version"))
        .withColumn("commit_timestamp", F.col("_commit_timestamp"))
        #.withColumn("balance", F.col("balance").cast(DoubleType()))
        .drop("_change_type","_commit_version", "_commit_timestamp")
    )


@dp.table(
    name="src_customer_bronze",
    comment="Bronze layer for customer data with CDC incremental loading from latest version",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def src_customer_bronze():
    """
    Bronze layer for customers - CDC incremental loading starting from latest version
    """
    return (
        spark.readStream
        .option("readChangeFeed", "true")
        #.option("startingVersion", "latest")
        .table("pipeline_demo.bronze.src_customer")
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.lit("customer"))
        .withColumn("operation_type", F.col("_change_type"))
        .withColumn("commit_version", F.col("_commit_version"))
        .withColumn("commit_timestamp", F.col("_commit_timestamp"))
        .withColumn("longitude", F.col("longitude").cast(DoubleType()))
        .withColumn("latitude", F.col("latitude").cast(DoubleType()))
        .drop("_change_type","_commit_version", "_commit_timestamp")
    )

@dp.table(
    name="src_acct_tx_bronze",
    comment="Bronze layer for transaction data with CDC incremental loading from latest version",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def src_acct_tx_bronze():
    """
    Bronze layer for transactions - CDC incremental loading starting from latest version
    """
    return (
        spark.readStream
        .option("readChangeFeed", "true")
        #.option("startingVersion", "latest")
        #.option("startingVersion", "3")
        .table("pipeline_demo.bronze.src_acct_tx")
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.lit("transactions"))
        .withColumn("operation_type", F.col("_change_type"))
        .withColumn("commit_version", F.col("_commit_version"))
        .withColumn("commit_timestamp", F.col("_commit_timestamp"))
        .drop("_change_type","_commit_version", "_commit_timestamp")
    )

# =============================================================================
# SILVER LAYER - Cleaned and Validated Data with SCD Type 2 and CDC
# =============================================================================

# =============================================================================
# ACCOUNTS SILVER - AUTO CDC with SCD Type 2
# =============================================================================

# Create the streaming table for accounts
dp.create_streaming_table(
    name="accounts_silver",
    comment="SCD Type 2 streaming table for account data with AUTO CDC for INSERT and UPDATE operations",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    expect_all={
        "valid_acct_id": "acct_id IS NOT NULL",
        "valid_balance": "balance >= 0.0",
        "valid_acct_status": "acct_status IS NOT NULL"
    }
)

# Apply changes with AUTO CDC - handles INSERT, UPDATE, and DELETE operations
# apply_as_deletes: marks records as deleted in SCD Type 2 when operation_type = 'delete'
dp.create_auto_cdc_flow(
    target="accounts_silver",
    source="src_accounts_bronze",
    keys=["acct_id"],
    sequence_by="commit_timestamp",
    ignore_null_updates=False,
    apply_as_deletes="operation_type = 'delete'",  # Handle DELETE operations in SCD Type 2
    except_column_list=["operation_type", "commit_version", "commit_timestamp", "source_file", "created_date", "ingestion_timestamp"],
    stored_as_scd_type=2
)

#=============================================================================
#CUSTOMERS SILVER - AUTO CDC with SCD Type 2
#=============================================================================

# Create the streaming table for customers
dp.create_streaming_table(
    name="customers_silver",
    comment="SCD Type 2 streaming table for customer data with AUTO CDC for INSERT and UPDATE operations",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    expect_all={
        "valid_customer_id": "customer_id IS NOT NULL",
        "valid_acct_id": "acct_id IS NOT NULL",
        "valid_state_cd": "state_cd IS NOT NULL AND length(state_cd) = 2",
        "valid_coordinates": "longitude IS NOT NULL AND latitude IS NOT NULL"
    }
)

# Apply changes with AUTO CDC - handles INSERT, UPDATE, and DELETE operations
# apply_as_deletes: marks records as deleted in SCD Type 2 when operation_type = 'delete'
dp.create_auto_cdc_flow(
    target="customers_silver",
    source="src_customer_bronze",
    keys=["customer_id"],
    sequence_by="commit_timestamp",
    ignore_null_updates=False,
    apply_as_deletes="operation_type = 'delete'",  # Handle DELETE operations in SCD Type 2
    except_column_list=["operation_type", "commit_version", "commit_timestamp", "source_file", "created_at", "ingestion_timestamp"],
    stored_as_scd_type=2
)

#transaction table

dp.create_streaming_table(
    name="transactions_silver",
    comment="SCD Type 2 streaming table for transaction data with AUTO CDC for INSERT and UPDATE operations",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    expect_all={
        "valid_transaction_id": "transaction_id IS NOT NULL",
        "valid_acct_id": "acct_id IS NOT NULL", 
        "valid_total_amt": "total_amt IS NOT NULL AND total_amt > 0.0",
        "valid_transaction_date": "transaction_date IS NOT NULL",
    }
)

# Apply changes with AUTO CDC - handles INSERT, UPDATE, and DELETE operations
# apply_as_deletes: marks records as deleted in SCD Type 2 when operation_type = 'delete'
dp.create_auto_cdc_flow(
    target="transactions_silver",
    source="src_acct_tx_bronze",
    keys=["transaction_id"],
    sequence_by="commit_timestamp",
    ignore_null_updates=False,
    apply_as_deletes="operation_type = 'delete'",  # Handle DELETE operations in SCD Type 2
    except_column_list=["operation_type", "commit_version", "commit_timestamp", "source_file", "created_date", "ingestion_timestamp"],
    stored_as_scd_type=2
)

# =============================================================================
# GOLD LAYER - Business-Ready Analytics Materialized Views
# =============================================================================

@dp.materialized_view(
    name="cust_tran_profile",
    comment="Customer transaction profile materialized view for analytics and BI",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dp.expect_all({
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_acct_id": "acct_id IS NOT NULL",
    "positive_transaction_count": "total_transactions > 0",
    "positive_transaction_value": "total_transactions_value > 0.0",
    "valid_date_range": "first_transaction_date <= latest_transaction_date"
})
def cust_tran_profile():
    """
    Gold layer materialized view: Customer Transaction Profile
    Implements the exact SQL query from requirements
    Note: SCD Type 2 uses __END_AT IS NULL to filter for current records
    """
    accounts = spark.read.table("accounts_silver").filter(F.col("__END_AT").isNull())
    customers = spark.read.table("customers_silver").filter(F.col("__END_AT").isNull())
    transactions = spark.read.table("transactions_silver")
    
    return (
        accounts.alias("src_accounts")
        .join(customers.alias("src_customer"), 
              F.col("src_accounts.acct_id") == F.col("src_customer.acct_id"), "inner")
        .join(transactions.alias("src_acct_tx"), 
              F.col("src_accounts.acct_id") == F.col("src_acct_tx.acct_id"), "inner")
        .groupBy(
            F.col("src_customer.customer_id"),
            F.col("src_accounts.acct_id"),
            F.col("src_accounts.acct_status"),
            F.col("src_accounts.balance")
        )
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("src_acct_tx.total_amt").cast(DoubleType()).alias("total_transactions_value"),
            F.min("src_acct_tx.transaction_date").alias("first_transaction_date"),
            F.max("src_acct_tx.transaction_date").alias("latest_transaction_date"),
            F.countDistinct("src_acct_tx.ticker_symbol").alias("count_distinct_tickers_traded")
        )
        .withColumn("profile_created_at", F.current_timestamp())
    )
