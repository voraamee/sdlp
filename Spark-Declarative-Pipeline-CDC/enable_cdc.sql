-- Enable Change Data Feed on source tables
ALTER TABLE pipeline_demo.bronze.src_accounts SET TBLPROPERTIES (delta.enableChangeDataFeed=true);
ALTER TABLE pipeline_demo.bronze.src_customer SET TBLPROPERTIES (delta.enableChangeDataFeed=true);  
ALTER TABLE pipeline_demo.bronze.src_acct_tx SET TBLPROPERTIES (delta.enableChangeDataFeed=true);

-- Verify CDC is enabled
DESCRIBE DETAIL pipeline_demo.bronze.src_accounts;
DESCRIBE DETAIL pipeline_demo.bronze.src_customer;
DESCRIBE DETAIL pipeline_demo.bronze.src_acct_tx;