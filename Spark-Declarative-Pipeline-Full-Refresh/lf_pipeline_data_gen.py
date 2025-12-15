# Databricks notebook source
# MAGIC %pip install dbldatagen

# COMMAND ----------

from dbldatagen import DataGenerator
from pyspark.sql.functions import expr, col, when
import uuid
import pyspark.sql.functions as F

# COMMAND ----------

ROW_COUNT = 10000
PARTITIONS = 4               
SEED = 1000000000

# COMMAND ----------

# MAGIC %sql
# MAGIC USE pipeline_demo.bronze_new

# COMMAND ----------

account_type = ['individual', 'joint','trust','custodial']
account_status = ['']
margin_trade = ['yes','no','pending','restricted']
risk_tol = ['low','medium','high']
states = [
    ('CA', 34.0522, -118.2437),
    ('NY', 40.7128,  -74.0060),
    ('TX', 29.7604,  -95.3698),
    ('VA', 38.8951,  -77.0369),
    ('IL', 41.8781,  -87.6298),
    ('FL', 27.9944,  -81.7603),
    ('WA', 47.7511, -120.7401),
    ('MA', 42.4072,  -71.3824),
    ('CO', 39.5501, -105.7821),
    ('AZ', 33.4484, -112.0740),
    ('AL', 32.8067, -86.7911),
    ('AR', 34.9697, -92.3731),
    ('CT', 41.5978, -72.7554),
    ('DE', 39.3185, -75.5071),
    ('GA', 33.0406, -83.6431),
    ('ID', 43.6150, -116.2311),
    ('IA', 41.5978, -93.09),
    ('KS', 39.0119, -98.4852),
    ('KY', 37.6681, -84.219),
    ('LA', 30.4636, -91.1893),
    ('MD', 39.1619, -76.5765),
    ('ME', 45.2538, -69.4454),
    ('MI', 42.7297, -84.654),
    ('MN', 46.7296, -94.6859),
    ('MO', 39.0119, -94.57),
    ('MS', 32.3182, -90.2072),
    ('NC', 35.7721, -79.0143),
    ('ND', 47.5080, -100.9341),
    ('NE', 41.1495, -98.2680),
    ('NH', 43.1630, -70.8433),
    ('NM', 34.5224, -105.8074),
    ('NV', 36.1627, -115.1628),
    ('OH', 40.4173, -82.9071),
    ('OK', 35.4676, -97.5164),
    ('OR', 45.5753, -122.8296),
    ('PA', 40.4406, -79.9959),
    ('RI', 41.8240, -71.4128),
    ('SC', 33.7323, -80.4592),
    ('SD', 44.3679, -100.3366),
    ('TN', 35.7664, -86.8024),
    ('UT', 39.3209, -111.0937),
    ('VT', 44.2656, -72.5778),
    ('WI', 43.0721, -89.3801),
    ('WV', 38.6495, -80.0081),
    ('WY', 42.7275, -107.2368)
]
state_codes = [s[0] for s in states]
lat_case = "CASE " + " ".join([f"WHEN state_cd = '{s[0]}' THEN {s[1]}" for s in states]) + " END + (rand() / 50 - 0.01)"
lon_case = "CASE " + " ".join([f"WHEN state_cd = '{s[0]}' THEN {s[2]}" for s in states]) + " END + (rand() / 50 - 0.01)"
country_code = ['US']
acct_status = ['open']


transaction_probabilities = [0.001, 0.1]
tran_type=['buy','sell','deposit','withdrawal','fee','split','dividend','interest']
ticker_symbol = ['BLK','SCHW','MS','GS','AMP','TROW','NTRS','STT','UBS','BAC','JPM']
order_type = ['market', 'limit', 'stop', 'stop_limit','fill_or_kill', 'immediate_or_cancel', 'day','trailing_stop']
trade_status=['open', 'filled', 'canceled', 'rejected','expired']

# COMMAND ----------

df_new= spark.sql("select ticker from users.amee_vora.ticker_data")

# COMMAND ----------

df_new_list = df_new.select("ticker").toPandas()["ticker"].tolist()

# COMMAND ----------

type(df_new_list)
display(df_new_list)


# COMMAND ----------

acct_rows = 1000
SEED = 1

acct_spec = (
    DataGenerator(spark, rows=acct_rows, partitions=4, randomSeed=SEED)
    .withIdOutput()
    .withColumn("acct_id","string",values=[f"00{i}A000000U5DtWAOV" for i in range(1,1001)])
    .withColumn("acct_name", "string", values=[f"acct_{i}" for i in range(1, 1001)], random=True)
    .withColumn("acct_number", "integer", minValue=1000000000, maxValue=2000000000, random=True)
    .withColumn("acct_type", "string", values=account_type, random=True)
    .withColumn("balance", "double", minValue=500000.0, maxValue=2500000.0, random=True)
    .withColumn("acct_status", values=acct_status)
    .withColumn("open_date", "timestamp", begin="2020-01-01 00:00:00", end="2021-12-31 23:59:59", random=True)
    .withColumn("closed_date", "timestamp", begin="2080-01-01 00:00:00", end="2800-12-31 23:59:59", random=True)
    .withColumn("margin_enabled", "string", values=margin_trade, random=True)
    .withColumn("risk_tolerance", "string", values=risk_tol, random=True)
    .withColumn("created_date", "timestamp", begin="2022-01-01 00:00:00", end="2024-12-31 23:59:59", random=True)
    
)

accounts_df = acct_spec.build()
accounts_df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("src_accounts")


# COMMAND ----------

cust_rows = 1000
SEED = 1

cust_spec = (
    DataGenerator(spark, rows=cust_rows, partitions=4, randomSeed=SEED)
    .withIdOutput()
    .withColumn("customer_id", "string", expr="concat('1000000',id)")
    .withColumn("first_name", "string", values=[f"customer_{i}" for i in range(1, 1001)], random=True)
    .withColumn("last_name", "string", values=[f"custlastname_{i}" for i in range(1, 1001)], random=True)
    .withColumn("email", "string", expr="concat('user', cast(id as string), '@demo.com')")
    .withColumn("phone_number", "string", expr="concat('+1-555-', lpad(cast(id%10000 as string), 4, '0'))")
    .withColumn("acct_id","string",values=[f"00{i}A000000U5DtWAOV" for i in range(1,1001)])
    .withColumn("state_cd", "string", values=state_codes, random=True)
    .withColumn("latitude", "double",expr=lat_case,
          baseColumn="state_cd")
    .withColumn(
          "longitude", "double",
          expr=lon_case,
          baseColumn="state_cd")
    .withColumn("country_cd", values=country_code)
    .withColumn("created_at", "timestamp", begin="2022-01-01 00:00:00", end="2024-12-31 23:59:59", random=True)
)

cust_df = cust_spec.build()
cust_df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("src_customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Validation to make sure all accounts have customer.
# MAGIC
# MAGIC select acct_id from src_accounts
# MAGIC MINUS
# MAGIC select acct_id from src_customer;
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

tran_rows = 8000
SEED = 1

tran_spec = (
    DataGenerator(spark, rows=tran_rows, partitions=4, randomSeed=SEED)
    .withIdOutput()
    .withColumn("transaction_id", "string", expr="concat('1110000',id)")
    .withColumn("acct_id","string",values=[f"00{i}A000000U5DtWAOV" for i in range(1,1001)], random=True)
    .withColumn("ticker_symbol", "string", values=df_new_list, random=True)
    .withColumn("transaction_type", "string", values=tran_type, random=True)
    .withColumn("quantity", "double", minValue=10.0, maxValue=20000.0, random=True)
    .withColumn("price_per_share", "double", minValue=50.0, maxValue=1000.0, random=True)
    .withColumn("total_amt", "double", expr="quantity * price_per_share")
    .withColumn("transaction_date", "timestamp", begin="2022-01-01 00:00:00", end="2022-12-31 23:59:59", random=True)
    .withColumn("order_type", "string", values=order_type, random=True)
    .withColumn("trade_status", "string", values=trade_status, random=True)
    .withColumn("created_date", "timestamp", begin="2022-01-01 00:00:00", end="2022-12-31 23:59:59", random=True)
    
)

tran_df = tran_spec.build()
tran_df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("src_acct_tx")

# COMMAND ----------

# DBTITLE 1,transaction
tran_rows = 2000
SEED = 1

tran_spec = (
    DataGenerator(spark, rows=tran_rows, partitions=4, randomSeed=SEED)
    .withIdOutput()
    .withColumn("transaction_id", "string", expr="concat('1110000',id)")
    .withColumn("acct_id","string",values=[f"00{i}A000000U5DtWAOV" for i in range(100,120)], random=True)
    .withColumn("ticker_symbol", "string", values=df_new_list, random=True)
    .withColumn("transaction_type", "string", values=tran_type, random=True)
    .withColumn("quantity", "double", minValue=10.0, maxValue=20000.0, random=True)
    .withColumn("price_per_share", "double", minValue=50.0, maxValue=1000.0, random=True)
    .withColumn("total_amt", "double", expr="quantity * price_per_share")
    .withColumn("transaction_date", "timestamp", begin="2022-01-01 00:00:00", end="2022-12-31 23:59:59", random=True)
    .withColumn("order_type", "string", values=order_type, random=True)
    .withColumn("trade_status", "string", values=trade_status, random=True)
    .withColumn("created_date", "timestamp", begin="2022-01-01 00:00:00", end="2022-12-31 23:59:59", random=True)
    
)

tran_df = tran_spec.build()
tran_df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("src_acct_tx")

# COMMAND ----------

# DBTITLE 1,Validation Commands
# MAGIC %sql
# MAGIC with temp_transaction as (
# MAGIC select acct_id, count(1) as cnt from src_acct_transaction group by acct_id)
# MAGIC select min(cnt), max(cnt) from temp_transaction
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select acct_id from src_accounts
# MAGIC MINUS
# MAGIC select acct_id from src_acct_tx

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select acct_id from src_acct_tx
# MAGIC MINUS
# MAGIC select acct_id from src_accounts
# MAGIC
# MAGIC
