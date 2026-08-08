# Databricks notebook source
import pandas as pd
import requests 
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pyspark.sql.functions import to_date
import time
import logging
logging.basicConfig(
    level= logging.INFO,
    format = "%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %run "/Workspace/Users/shaikhfaraz0401@gmail.com/CardPulse: Credit Card Analytics Lakehouse/00 Configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Building Moduldar Function for API-Ingestation 

# COMMAND ----------

# A Modular Function for getting last_run of a perticular pipeline
def load_last_run(pipeline_name, table_name):
    try:
        metadata = spark.read.table('workspace.default.cardpulse_pipeline_metadata')
        row = metadata.filter((col('pipeline_name') == pipeline_name) & (col('table_name') == table_name)).select('last_processed_timestamp').first()
        last_run = row[0] if row else None
        return last_run
    
    except Exception as e:
        logger.exception("Metadata loading failed.")
        raise

# COMMAND ----------

# A Modular Function which will be used to fetch data from API 
def fetch_data_from_api(endpoint, last_run):
    try:
        limit = 5000
        offset = 0
        all_data = []
        logger.info(f"Starting API ingestion for {endpoint}...")
        while True:
            params = {
                "limit": limit,
                "offset": offset
            }
            if last_run is not None:
                params["updated_after"] = last_run

            logger.info(f"Fetching {endpoint} page with offset={offset}")
            MAX_RETRIES = 3
            for attempt in range(MAX_RETRIES):
                try:

                    response = requests.get(
                        API_BASE_URL + endpoint,
                        params=params,
                        timeout=120
                    )

                    response.raise_for_status()

                    result = response.json()

                    break

                except (
                    requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout
                ) as e:

                    logger.warning(
                        f"Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}"
                    )

                    if attempt < MAX_RETRIES - 1:
                        time.sleep(5)
                    else:
                        raise

            all_data.extend(result["data"])

            if result["count"] < limit:
                break

            offset += limit

            time.sleep(2)

        logger.info(f"{len(all_data)} rows fetched successfully.")

        if len(all_data) == 0:
            logger.info("No new rows found.")
            return None

        df = spark.createDataFrame(all_data)

        return df

    except Exception as e:
        logger.exception("Fetching data failed.")
        raise

# COMMAND ----------

# A Modular funtion which will be used to update the pipeline's metadata
def pipeline_metadata_update(ingested_data, pipeline_name, table_name):
    try:
        last_processed_timestamp = ingested_data.agg(max('updated_at').cast('timestamp')).first()[0] #(first finding last processed timestamp)

        new_metadata = spark.createDataFrame([
            Row(pipeline_name = pipeline_name, table_name = table_name, last_processed_timestamp = last_processed_timestamp)
        ]) # createing a temp metadata to be used as source

        metadata = DeltaTable.forName(spark, 'workspace.default.cardpulse_pipeline_metadata') # calling our original metadata table  

        # Performing merge to update the last_processed_timestamp
        metadata.alias('target').merge(new_metadata.alias('source'), 'target.pipeline_name = source.pipeline_name and target.table_name = source.table_name').whenMatchedUpdate(set={'last_processed_timestamp':'source.last_processed_timestamp'}).whenNotMatchedInsert(values={'pipeline_name':'source.pipeline_name', 'table_name':'source.table_name', 'last_processed_timestamp':'source.last_processed_timestamp'}).execute() 

    except Exception as e:
        logger.exception("Updating Metadata failed.")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function for Ingesting our data

# COMMAND ----------

def ingest_merchants_to_bronze(merchants_csv_path):
    try:
        logger.info("Merchants ingestion started...")
        # Frist we will load the merchants table from DBFS
        ingested_merchants = spark.read.format('csv').load(merchants_csv_path, inferSchema=True, header=True)

        # we will append this data [as it is ingestation layer so the marge-based saving will work from bronze to silver part]
        ingested_merchants.write.mode('overwrite').format('delta').save('/Volumes/workspace/default/cardpulse/bronze/merchants')     
            
        logger.info(f'{ingested_merchants.count()} Rows ingested and saved to bronze/merchants')

    except Exception as e:
        logger.exception("Merchants ingestion failed.")
        raise

# COMMAND ----------

def ingest_customers_to_bronze():
    try:
        logger.info("Customers ingestion started...")
        # using our load_last_run funtion to get last_run of this pipeline
        last_run = load_last_run('ingest_to_bronze', 'bronze_customers')
        
        #  using our fetch_data_from_api to fetch customers data from api
        ingested_customers = fetch_data_from_api('/customers', last_run)
        if ingested_customers is None:
            logger.info("No new customer records, thus exiting the pipeline.")
            return # Early exit
        
        # we will append this data [as it is ingestation layer so the marge-based saving will work from bronze to silver part]
        ingested_customers.write.mode('append').format('delta').save('/Volumes/workspace/default/cardpulse/bronze/customers')
        
        pipeline_metadata_update(ingested_customers, 'ingest_to_bronze', 'bronze_customers')

        logger.info(f'{ingested_customers.count()} Rows ingested and saved to bronze/customers')

    except Exception as e:
        logger.exception("Customers ingestion failed.")
        raise

# COMMAND ----------

def ingest_cards_to_bronze():
    try:
        logger.info("Cards ingestion started...")
        # using our load_last_run funtion to get last_run of this pipeline
        last_run = load_last_run('ingest_to_bronze', 'bronze_cards')
        
        #  using our fetch_data_from_api to fetch customers data from api
        ingested_cards = fetch_data_from_api('/cards', last_run)
        if ingested_cards is None:
            logger.info("No new cards records, thus exiting the pipeline.")
            return # Early exit
        
        # we will append this data [as it is ingestation layer so the marge-based saving will work from bronze to silver part]
        ingested_cards.write.mode('append').format('delta').save('/Volumes/workspace/default/cardpulse/bronze/cards')

        pipeline_metadata_update(ingested_cards, 'ingest_to_bronze', 'bronze_cards')

        logger.info(f'{ingested_cards.count()} Rows ingested and saved to bronze/cards')

    except Exception as e:
        logger.exception("Cards ingestion failed.")
        raise

# COMMAND ----------

def ingest_transactions_to_bronze():
    try:
        logger.info("Transtions ingestion started...")
        # using our load_last_run funtion to get last_run of this pipeline
        last_run = load_last_run('ingest_to_bronze', 'bronze_transactions')
        
        #  using our fetch_data_from_api to fetch customers data from api
        ingested_transaction = fetch_data_from_api('/transactions', last_run)
        if ingested_transaction is None:
            logger.info("No new transactions, thus exiting the pipeline.")
            return # Early exit
        
        # we will append this data [as it is ingestation layer so the marge-based saving will work from bronze to silver part]
        ingested_transaction.write.mode('append').format('delta').save('/Volumes/workspace/default/cardpulse/bronze/transactions')

        pipeline_metadata_update(ingested_transaction, 'ingest_to_bronze', 'bronze_transactions')

        logger.info(f'{ingested_transaction.count()} Rows ingested and saved to bronze/transactions')

    except Exception as e:
        logger.exception("Transactions ingestion failed.")
        raise

# COMMAND ----------

def ingest_to_bronze_orchestration_pipeline():
    try: 
        logger.info("Ingest to Bronze Orchestration Pipeline Started...")
        ingest_merchants_to_bronze(MERCHANTS_CSV_PATH)
        ingest_customers_to_bronze()
        ingest_cards_to_bronze()
        ingest_transactions_to_bronze()
        logger.info("Orchestration pipeline completed sucessfully :)")
    
    except Exception as e:
        logger.exception("Orchestration pipeline failed.")
        raise

# COMMAND ----------

ingest_to_bronze_orchestration_pipeline()