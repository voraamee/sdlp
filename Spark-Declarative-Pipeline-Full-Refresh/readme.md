# Data Engineering - Spark Declarative Pipeline
Builds a spark declarative pipeline that proceses data through the medallion architecture.

#Details
-Step 1: Reads account, customer and transaction delta tables in your defined catalog and schema
-Step 2: Converts source delta tables to create streaming tables in bronze layer
-Step 3: Reads streaming tables in bronze layer and curates and applies SCD2 Type 2 format to create tables in silver layer
-Step 4: Aggregates data from silver layer to build two materialized views in Gold layer: customer profile and ticker distribution by geography

#Files
-**`lf_pipeline_data_gen`** - Source data generation
-**`lakeflow_pipeline_medallion.py`** - Script to execute lakeflow pipeline

#Installation
-Git clone URL the lab material within a folder of your choice in your local
-Import lf_pipeline_data_gen notebook and lakeflow_pipeline_medallion.py in your Databricks workspace
-Create a catalog / schema as used in the script
	-catalog - lf_demo_av
	-schema - bronze
	Note - If you provide your own catalog and schema name, then please update the two code artificats listed above with the updated catalog and schema name
-Provide access on catalog to the users of the workshop
-Source Data Generation
	-Execute cell by cell or run all cells in notebook lf_pipeline_data_gen to generate source data. 
        -If this does not work please upload the CSV files provided in dataset folder
        -lf_pipeline_data_ge notebook has validation commands towards the end, once the data is loaded to source tables perform validation using the commands provided
-Pipeline Execution
	-Execute lakeflow_pipeline_medallion.py through UI in Databricks workspace
        -Once the pipeline completes successfully you should see the following datasets:
        -Account, Customer and Transation Streaming tables
        -Account, Customer and Transaction streaming tables with effective start and effective end date
        -Customer profile and Ticker distribution by geo materialized views
        -Take a look at the pipeline graph in the UI. 
        -Understand data quality stats under Tables tab presented at the bottom and Pipeline performance states under performance tab below
        -Explore additional features like pipeline environment, how to use Databrciks assistant,variables, version history 
