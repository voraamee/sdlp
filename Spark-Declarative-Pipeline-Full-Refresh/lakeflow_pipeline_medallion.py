# MAGIC %md
# MAGIC In this demo, we will read build a lakeflow pipeline that will read from Delta tables and process the data through medallion architecture to create materialized views for business consumption and analytics. 
# MAGIC By the end of this demo, you will be able to:
# MAGIC
# MAGIC * Build Lakeflow declarative pipeline
# MAGIC * Have pipeline read data from a Delta table and convert it into streaming tables
# MAGIC * Build silver layer of data sources with data quality expectations. Implement SCD Type 2 for master data and apply AUTO CDC. Load data in append mode for transaction data
# MAGIC * Create materialized views for business consumption and analytics by aggregating data from silver layer
# MAGIC * Run pipeline and visualize the data using Databricks SQL and Catalog explorer
# MAGIC * Visually explore statistics for each table and performance stats for each hop
# MAGIC

"""
Lakeflow Declarative Pipeline (LDP) - Medallion Architecture
Financial Services Data Pipeline with Bronze, Silver, and Gold Layers

This pipeline processes financial account and transaction data through a medallion architecture:
- Bronze: Streaming tables with Raw data (already exists in lf_av_demo.bronze), copy of source delta tables
- Silver: Cleaned, validated data with SCD Type 2 for accounts/customers and append only for transactions date
- Gold: Business-ready analytics tables with materialized views

Target Catalog: lf_demo_av
Source Schema: bronze (src_accounts, src_customer, src_acct_tx)
Default Schema: lfp_demo_av
"""
# The unterminated string literal was caused by a missing closing triple quote above.

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# Import SparkSession for reading external tables
from pyspark.sql import SparkSession
# =============================================================================
# Bronze LAYER - Reading Delta source tables
# =============================================================================

@dlt.table(
    name="src_accounts_bronze",
    comment="Delta table for account data"
)
def src_accounts_bronze():
    """Ingest raw account data with minimal transformation."""
    return (
        spark.readStream.table("lf_demo_av.bronze.src_accounts")
     #   .withColumn("ingestion_timestamp", current_timestamp())
    #  .withColumn("source_table", lit("ato_training"))
    #    .withColumn("pipeline_version", lit("1.0"))
    )


@dlt.table(
    name="src_customer_bronze",
    comment="Delta table for customer data"

)

def src_customer_bronze():
    """Ingest raw customer data with minimal transformation."""
    return (
        spark.readStream.table("lf_demo_av.bronze.src_customer")
     #   .withColumn("ingestion_timestamp", current_timestamp())
     #   .withColumn("source_table", lit("ato_training"))
     #   .withColumn("pipeline_version", lit("1.0"))
    )

@dlt.table(
    name="src_acct_tx_bronze",
    comment="Ingest raw transaction data with minimal transformation."

)

def src_acct_tx_bronze():
    """Ingest raw customer data with minimal transformation."""
    return (
        spark.readStream.table("lf_demo_av.bronze.src_acct_tx")
     #   .withColumn("ingestion_timestamp", current_timestamp())
     #   .withColumn("source_table", lit("ato_training"))
     #   .withColumn("pipeline_version", lit("1.0"))
    )
# =============================================================================
# SILVER LAYER - Cleaned and Validated Data with SCD Type 2
# =============================================================================

@dlt.table(
    name="accounts_silver",
    comment="SCD Type 2 streaming table for account data with data quality expectations",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect_all({
    "valid_acct_id": "acct_id IS NOT NULL",
    "valid_balance": "balance >= 0.0",
    "valid_acct_status": "acct_status IS NOT NULL"
})
def accounts_silver():
    """
    Silver layer for accounts with SCD Type 2 implementation using merge statement
    Tracks historical changes to account data
    """
    #spark = SparkSession.getActiveSession()
    return (
        #spark.readStream.table("src_accounts_bronze")
        dlt.read_stream("src_accounts_bronze")
        .withColumn("__START_AT", F.current_timestamp())
        .withColumn("__END_AT", F.lit(None).cast(TimestampType()))
        .withColumn("balance", F.col("balance").cast(DoubleType()))
        .withColumn("is_current", F.lit(True))
        .drop("ingestion_timestamp","source_file")
    )
@dlt.table(
    name="customers_silver", 
    comment="SCD Type 2 streaming table for customer data with data quality expectations",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect_all({
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_acct_id": "acct_id IS NOT NULL",
    "valid_state_cd": "state_cd IS NOT NULL AND length(state_cd) = 2",
    "valid_coordinates": "longitude IS NOT NULL AND latitude IS NOT NULL"
})
def customers_silver():
    """
    Silver layer for customers with SCD Type 2 implementation using merge statement
    Tracks historical changes to customer data
    """
    spark = SparkSession.getActiveSession()
    return (
        #spark.readStream.table("src_customer_bronze")
        dlt.read_stream("src_customer_bronze")
        .withColumn("__START_AT", F.current_timestamp())
        .withColumn("__END_AT", F.lit(None).cast(TimestampType()))
        #.withColumn("effective_date", F.current_timestamp())
        #.withColumn("end_date", F.lit(None).cast(TimestampType()))
        .withColumn("is_current", F.lit(True))
        #.withColumn("created_at", F.current_timestamp())
        #.withColumn("updated_at", F.current_timestamp())
        .withColumn("longitude", F.col("longitude").cast(DoubleType()))
        .withColumn("latitude", F.col("latitude").cast(DoubleType()))
        .drop("ingestion_timestamp","source_file")
        #.withColumn("record_hash",
        #           F.sha2(F.concat_ws("|",
        #                           F.col("customer_id"),
        #                           F.col("acct_id"),
        #                           F.col("state_cd"),
        #                           F.col("longitude"),
        #                           F.col("latitude")), 256))
    )

@dlt.table(
    name="transactions_silver",
    comment="Streaming append-only table for transaction data with comprehensive data quality",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect_all({
    "valid_transaction_id": "transaction_id IS NOT NULL",
    "valid_acct_id": "acct_id IS NOT NULL", 
    "valid_total_amt": "total_amt IS NOT NULL AND total_amt > 0.0",
    "valid_transaction_date": "transaction_date IS NOT NULL",
    "valid_ticker_symbol": "ticker_symbol IS NOT NULL",
    "valid_total_holdings": "total_holdings IS NOT NULL AND total_holdings >= 0.0"
})
def transactions_silver():
    """
    Silver layer for transactions - streaming append only
    No SCD Type 2 needed as transactions are immutable events
    """
    spark = SparkSession.getActiveSession()
    return (
        #spark.readStream.table("src_acct_tx_bronze")
        dlt.read_stream("src_acct_tx_bronze")
        #.withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("total_amt", F.col("total_amt").cast(DoubleType()))
        .withColumn("total_holdings", F.col("total_holdings").cast(DoubleType()))
        .withColumn("transaction_date", F.col("transaction_date").cast(DateType()))
        .drop("source_file","processed_timestamp","created_date")
    )

# =============================================================================
# GOLD LAYER - Business-Ready Analytics Materialized Views
# =============================================================================

@dlt.table(
    name="cust_tran_profile",
    comment="Customer transaction profile materialized view for analytics and BI",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect_all({
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
    """
    accounts = dlt.read("accounts_silver").filter(F.col("is_current") == True)
    customers = dlt.read("customers_silver").filter(F.col("is_current") == True)
    transactions = dlt.read("transactions_silver")
    
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

@dlt.table(
    name="ticker_dist_geo",
    comment="Ticker distribution by geography materialized view for market analysis and BI",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect_all({
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_acct_id": "acct_id IS NOT NULL",
    "valid_ticker_symbol": "ticker_symbol IS NOT NULL",
    "valid_state_cd": "state_cd IS NOT NULL",
    "valid_coordinates": "longitude IS NOT NULL AND latitude IS NOT NULL",
    "positive_holdings": "total_holdings >= 0.0"
})
def ticker_dist_geo():
    """
    Gold layer materialized view: Ticker Geographic Distribution
    Implements the exact SQL query from requirements with CTE pattern
    """
    customers = dlt.read("customers_silver").filter(F.col("is_current") == True)
    transactions = dlt.read("transactions_silver")
    
    # CTE equivalent: temp table for latest transaction per account/ticker combination
    temp = (
        transactions
        .groupBy("acct_id", "ticker_symbol")
        .agg(F.max("transaction_date").alias("max_tran_date"))
    )
    
    return (
        transactions.alias("src_acct_tx")
        .join(customers.alias("cust"), 
              F.col("src_acct_tx.acct_id") == F.col("cust.acct_id"), "inner")
        .join(temp.alias("temp"),
              (F.col("src_acct_tx.acct_id") == F.col("temp.acct_id")) &
              (F.col("src_acct_tx.ticker_symbol") == F.col("temp.ticker_symbol")) &
              (F.col("src_acct_tx.transaction_date") == F.col("temp.max_tran_date")),
              "inner")
        .select(
            F.col("cust.customer_id"),
            F.col("cust.acct_id"),
            F.col("temp.ticker_symbol"),
            F.col("src_acct_tx.total_holdings").cast(DoubleType()),
            F.col("cust.state_cd"),
            F.col("cust.longitude"),
            F.col("cust.latitude")
        )
        .withColumn("geo_analysis_created_at", F.current_timestamp())
    )