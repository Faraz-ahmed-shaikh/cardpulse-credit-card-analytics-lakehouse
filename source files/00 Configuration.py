# Databricks notebook source
# MAGIC %md
# MAGIC ### This Notebook is used to do Configurations and set PATHs for project

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.types import *
import logging
logging.basicConfig(
    level= logging.INFO,
    format = "%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# Here we are creating metadata table, this table will keep track of all the last processed timestamp by each pipeline, the creation will be done only 1 time.
table_name = "workspace.default.cardpulse_pipeline_metadata"

if not spark.catalog.tableExists(table_name):

    schema = StructType([
        StructField('pipeline_name', StringType(), False),
        StructField('table_name', StringType(), False),
        StructField('last_processed_timestamp', TimestampType(), True)
    ])

    pipeline_metadata = spark.createDataFrame([
        Row(pipeline_name='ingest_to_bronze', table_name='bronze_merchants', last_processed_timestamp=None),
        Row(pipeline_name='ingest_to_bronze', table_name='bronze_customers', last_processed_timestamp=None),
        Row(pipeline_name='ingest_to_bronze', table_name='bronze_cards', last_processed_timestamp=None),
        Row(pipeline_name='ingest_to_bronze', table_name='bronze_transactions', last_processed_timestamp=None),

        Row(pipeline_name='bronze_to_silver', table_name='silver_merchants', last_processed_timestamp=None),
        Row(pipeline_name='bronze_to_silver', table_name='silver_customers', last_processed_timestamp=None),
        Row(pipeline_name='bronze_to_silver', table_name='silver_cards', last_processed_timestamp=None),
        Row(pipeline_name='bronze_to_silver', table_name='silver_transactions', last_processed_timestamp=None),

        Row(pipeline_name='silver_to_gold', table_name='customers_spend_summary', last_processed_timestamp=None),
        Row(pipeline_name='silver_to_gold', table_name='portfolio_risk_summary', last_processed_timestamp=None),
        Row(pipeline_name='silver_to_gold', table_name='customer_segmentation_summary', last_processed_timestamp=None)
    ], schema=schema)

    pipeline_metadata.write.mode("overwrite").saveAsTable(table_name)

    logger.info("Metadata table created")

else:
    logger.info("Metadata table already exists. Skipping creation.")

# For Ingestation
MERCHANTS_CSV_PATH = '/Volumes/workspace/default/my_files/merchants.csv'
NEONDB_URL = 'postgresql://username@password/neondb?sslmode=require&channel_binding=require'

API_BASE_URL = 'https://cardpulse-api-obzy.onrender.com'

# For Bronze Cleaning
METADATA_PATH = 'workspace.default.cardpulse_pipeline_metadata'
BRONZE_MERCHANTS_PATH = '/Volumes/workspace/default/cardpulse/bronze/merchants'
BRONZE_CUSTOMERS_PATH = '/Volumes/workspace/default/cardpulse/bronze/customers'
BRONZE_CARDS_PATH = '/Volumes/workspace/default/cardpulse/bronze/cards'
BRONZE_TRANSACTIONS_PATH = '/Volumes/workspace/default/cardpulse/bronze/transactions'

# For Silver Processing
SILVER_MERCHANTS_PATH = '/Volumes/workspace/default/cardpulse/silver/merchants'
SILVER_CUSTOMERS_PATH = '/Volumes/workspace/default/cardpulse/silver/customers'
SILVER_CARDS_PATH = '/Volumes/workspace/default/cardpulse/silver/cards'
SILVER_TRANSACTIONS_PATH = '/Volumes/workspace/default/cardpulse/silver/transactions'

# For Gold Transformation
GOLD_SPEND_SUMMARY_PATH = '/Volumes/workspace/default/cardpulse/gold/customers_spend_summary'
GOLD_PORTFOLIO_RISK_SUMMARY = "/Volumes/workspace/default/cardpulse/gold/portfolio_risk_summary/"
GOLD_CUSTOMER_SEGMENTATION = "/Volumes/workspace/default/cardpulse/gold/customer_segmentation_summary/"