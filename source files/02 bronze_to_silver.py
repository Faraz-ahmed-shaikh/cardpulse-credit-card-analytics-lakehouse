# Databricks notebook source
from pyspark.sql import Window
from pyspark.sql.functions import *
from delta.tables import DeltaTable
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
# MAGIC #### Moduldar Functions which will be used by each processing pipelines

# COMMAND ----------

# This is a modular function for loading last_run of each pipeline's table for incremantal loading
def load_last_run(pipeline_name, table_name):
    try:
        # First we will load exisiting metadata to get last_run for that table
        metadata = spark.read.table('workspace.default.cardpulse_pipeline_metadata')
        # Now we will select the last_run of that particular pipeline's table
        last_run_row = metadata.filter((col("pipeline_name") == pipeline_name) & (col("table_name") == table_name)).select("last_processed_timestamp").first() 
        last_run = last_run_row[0] if last_run_row is not None and last_run_row[0] is not None else None
        return last_run

    except Exception as e:
        logger.exception(f"Loading last run for {pipeline_name, table_name} failed")
        raise

# COMMAND ----------

# A Modular Function for Loading data from bronze following Full Load in 1st Run and Incremenatal Load in next Runs
def load_data_from_bronze(bronze_table_path, last_run):
    try:
        loaded_data = spark.read.format('delta').load(bronze_table_path)
        if last_run is None:
            new_rows = loaded_data # If the last_run is None basically when the pipeline is running for first time, then it will be fully loaded
        else:
            new_rows = loaded_data.filter(col("updated_at")>last_run)
        return new_rows

    except Exception as e:
        logger.exception(f"Loading data from {bronze_table_path} failed")
        raise

# COMMAND ----------

# This is a [modular] function which will receive rules and cleaned data, it will just validate only by making 'is_valid' column
def validation_engine(cleaned_df,rules, row_count, cleaned_row_count):
    try:
        combined_rules = rules[0]
        for rule in rules[1:]:
            combined_rules = combined_rules & rule # This is how our combined_rules will look like: [rule1 & rule2 & rule3 & rule....]
        validated_df = cleaned_df.withColumn('is_valid', combined_rules)
        good_rows = validated_df.filter(col('is_valid')==True).drop(col('is_valid'))
        bad_rows = validated_df.filter(col('is_valid')==False).drop(col('is_valid'))
        # Report
        passed_count = good_rows.count()
        falied_count = bad_rows.count()
        success_rate = (passed_count / cleaned_row_count * 100) if cleaned_row_count > 0 else 0.0
        failure_rate = (falied_count / cleaned_row_count * 100) if cleaned_row_count > 0 else 0.0

        logger.info(f"""
        ===================================
            DATA QUALITY & LINAGE REPORT
        ===================================

        New Rows loaded from bronze : {row_count}
        Rows Removed While Cleaning : {row_count - cleaned_row_count}
        Rows Entered in Validation : {cleaned_row_count}
        Rows Passed in Validation & Loaded into Silver  : {passed_count}
        Rows Failed in Validation & Quarantined  : {falied_count}

        Success Rate                : {success_rate:.2f}%
        Failure Rate                : {failure_rate:.2f}%

        ===================================""")
    
        return good_rows, bad_rows

    except Exception as e:
        logger.exception(f"Validating {cleaned_df} failed.")
        raise

# COMMAND ----------

# A Modular function which we will be used to update pipline's metadata
def update_metadata(good_rows, pipeline_name, table_name):
    try:
        # first processing the max_timestamp of good_df
        last_processed_timestamp = good_rows.agg(max(col('updated_at').cast('timestamp'))).first()[0]
        # A temp df to use as source
        new_df = spark.createDataFrame(
            [Row(pipeline_name=pipeline_name, table_name=table_name, last_processed_timestamp = last_processed_timestamp)]
        )
        # calling existing data for target
        metadata = DeltaTable.forName(spark, 'workspace.default.cardpulse_pipeline_metadata')
        # updating metadata using merge
        metadata.alias('target').merge(new_df.alias('source'),'target.pipeline_name = source.pipeline_name AND target.table_name = source.table_name').whenMatchedUpdate(set={'last_processed_timestamp': 'source.last_processed_timestamp'}).whenNotMatchedInsert(values={'pipeline_name':'source.pipeline_name', 'table_name':'source.table_name','last_processed_timestamp': 'source.last_processed_timestamp'}).execute()

    except Exception as e:
        logger.exception(f"Updating metadata for {pipeline_name, table_name} failed.")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline specific functions

# COMMAND ----------

# MAGIC %md
# MAGIC #### Functions for Merchants

# COMMAND ----------

# This is the [non-modular] cleaning function for merchants
def clean_merchants(loaded_merchants):
    # Droping Duplicates based on merchant_id
    merchants = loaded_merchants.dropDuplicates(["merchant_id"])

    # fixing standarization issue like titlecasing and removing whitespace 
    merchants = merchants.withColumns({
        'merchant_id': trim(col('merchant_id')),
        'merchant_name': initcap(trim(col("merchant_name"))),
        'merchant_category': initcap(trim(col("merchant_category"))),
        'country': initcap(trim(col("country"))),
        'mcc_code': trim(col("mcc_code")),
        'merchant_city': initcap(trim(col("merchant_city")))
    })

    # renaming country with merchants_country 
    merchants = merchants.withColumnRenamed("country", "merchant_home_country")

    # adding a timestamp column to identify when this was processed for silver
    merchants = merchants.withColumns({'silver_processed_at': current_timestamp(), 'data_source':lit('bronze')})

    return merchants

# COMMAND ----------

def process_merchants(BRONZE_MERCHANTS_PATH):
    try:
        logger.info('Processing Merchants for Silver Started..')
        # First we will load our data, as merchants is small static file, it will be full load at every run
        loaded_merchants = spark.read.format('delta').load(BRONZE_MERCHANTS_PATH)
        
        row_count = loaded_merchants.count()
        logger.info(f'{row_count} Rows loaded from bronze merchants for silver processing')
        
        # Cleaning Function for Merchants
        cleaned_merchants = clean_merchants(loaded_merchants)
        cleaned_merchants_count = cleaned_merchants.count()
        
        # Using Validation Engine for Merchants
        rules = [
            col('merchant_id').isNotNull(),
            col('merchant_name').isNotNull()
        ] # rows should pass all these rules to be saved in silver 
        merchants_good_rows, merchants_bad_rows = validation_engine(cleaned_merchants,rules, row_count, cleaned_merchants_count)
        
        # Adding reasons for quarantine
        merchants_bad_rows = merchants_bad_rows.withColumn(
            "failed_reason",
            when(col("merchant_id").isNull() , "merchant_id_missing")
            .when(col("merchant_name").isNull(), "merchant_name_missing")
            .otherwise("multiple_rules_failed")
        )
        
        # Saving bad rows [which failed to satisfy the rules] in Quarantine layer
        merchants_bad_rows.write.mode('overwrite').format('delta').save('/Volumes/workspace/default/cardpulse/quarantine/merchants')

        # Now saving good rows into silver [we are doing full load and overwrite in every run]
        merchants_good_rows.write.mode('overwrite').format('delta').save('/Volumes/workspace/default/cardpulse/silver/merchants')
        logger.info(f'{merchants_good_rows.count()} Sucessfully saved in Silver layer')

    
    except Exception as e:
        logger.exception("Processing Merchants failed.")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC #### Functions for Customers

# COMMAND ----------

def clean_customers(new_customers):
    try:
        # Dedup on customer_id
        window_spec = Window.partitionBy("customer_id").orderBy(desc("updated_at"))
        new_customers = (
            new_customers
            .withColumn("row_num", row_number().over(window_spec))
            .filter("row_num = 1")
            .drop("row_num")
        )

        # Strip whitespace from all string columns, and Title case: customer_name, city, occupation, customer_status, risk_grade
        new_customers = new_customers.withColumns({
            'city': initcap(trim(col('city'))),
            'customer_id': trim(col('customer_id')),
            'customer_name': initcap(trim(col('customer_name'))),
            'customer_status': initcap(trim(col('customer_status'))),
            'income': trim(col('income')),
            'occupation': initcap(trim(col('occupation'))), 
            'risk_grade': initcap(trim(col('risk_grade')))
        })

        # Cast joining_date string → date, Cast updated_at string → timestamp
        new_customers = new_customers.withColumns({
            'joining_date': col("joining_date").cast('date'),
            'updated_at': col("updated_at").cast('timestamp'),
        })

        # Adding metadata columns
        new_customers = new_customers.withColumns({'silver_processed_at': current_timestamp(), 'data_source':lit('bronze')})

        # Reorder columns 
        new_customers = new_customers.select('customer_id', 'customer_name', 'customer_status', 'income', 'total_cards_issued', 'joining_date', 'city', 'occupation', 'risk_grade',  'updated_at', 'silver_processed_at', 'data_source')

        # Renameing Columns
        new_customers = new_customers.withColumnsRenamed({"city": "customer_city", "income":"income_bucket"})
        
        return new_customers 

    except Exception as e:
        logger.exception(f"Cleaning Customers failed")
        raise

# COMMAND ----------

def process_customers(BRONZE_CUSTOMERS_PATH, pipeline_name, table_name):
    try:
        logger.info('Processing Customers started...')
        # getting the last run for this pipeline and table 
        last_run = load_last_run(pipeline_name, table_name) 
        
        # loading new rows after there last run (full load if last_run is none)
        new_customers = load_data_from_bronze(BRONZE_CUSTOMERS_PATH, last_run) 
        
        row_count = new_customers.count()
        logger.info(f'Total {row_count} rows loaded')
        if row_count == 0:
            print('There are no new rows to process')
            return # early exit
        
        # cleaning customers 
        cleaned_customers = clean_customers(new_customers)
        cleaned_customers_count = cleaned_customers.count()

        # Now validating customers
        rules = [
            col('customer_id').isNotNull(),
            col('customer_name').isNotNull()
        ]
        good_rows, bad_rows = validation_engine(cleaned_customers, rules, row_count, cleaned_customers_count) 

        # Saving bad rows [which failed to satisfy the rules] in Quarantine layer
        bad_rows = bad_rows.withColumn(
            "failed_reason",
            when(col("customer_id").isNull() , "customer_id_missing")
            .when(col("customer_name").isNull(), "customer_name_missing")
            .otherwise("multiple_rules_failed")
        ) # Adding reasons for quarantine
        bad_rows.write.mode('append').format('delta').save('/Volumes/workspace/default/cardpulse/quarantine/customers')

        # If the pipeline is running for first time, then entier good rows will be saved!
        if last_run is None:
            good_rows = good_rows.withColumns({
                'valid_from': col('updated_at'),
                'valid_till': lit(None),
                'is_current': lit(True)
            })
            good_rows.write.mode('overwrite').format('delta').save('/Volumes/workspace/default/cardpulse/silver/customers')
            update_metadata(good_rows, pipeline_name, table_name)
            logger.info("Initial load completed")
            return
        
        # If it is after 1st run, then rows will be classified into actual new customers, type 1 update and type 2 update
        # loading existing silver (active)
        existing_customers = spark.read.format('delta').load('/Volumes/workspace/default/cardpulse/silver/customers')
        active_customers = existing_customers.filter(col('is_current')==True)

        # joining new_customers with existing one for classification
        comparison = good_rows.alias('new').join(active_customers.alias('old'),col('new.customer_id') == col('old.customer_id'), how='left')

        # Scenario 1: New customer (who's old id is null)
        new_added_customers = comparison.filter(col('old.customer_id').isNull())

       # Scenario 2: SCD Type 2 Update (old id exists, and one of the tracked identity fields changed)
        type_2_update = comparison.filter(
            col("old.customer_id").isNotNull() &
            (
                (~col("new.customer_status").eqNullSafe(col("old.customer_status")))
                |
                (~col("new.customer_city").eqNullSafe(col("old.customer_city")))
                |
                (~col("new.income_bucket").eqNullSafe(col("old.income_bucket")))
                |
                (~col("new.occupation").eqNullSafe(col("old.occupation")))
                |
                (~col("new.risk_grade").eqNullSafe(col("old.risk_grade")))
            )
        )

        # Scenario 3: SCD Type 1 Update (old id exists, only name/total_cards_issued changed, all tracked fields same)
        type_1_update = comparison.filter(
            col("old.customer_id").isNotNull() &
            (
                (~col("new.customer_name").eqNullSafe(col("old.customer_name")))
                |
                (~col("new.total_cards_issued").eqNullSafe(col("old.total_cards_issued")))
            )
            &
            (
                col("new.customer_city").eqNullSafe(col("old.customer_city")) &
                col("new.customer_status").eqNullSafe(col("old.customer_status")) &
                col("new.income_bucket").eqNullSafe(col("old.income_bucket")) &
                col("new.occupation").eqNullSafe(col("old.occupation")) &
                col("new.risk_grade").eqNullSafe(col("old.risk_grade"))
            )
        )
        
        new_added_customers = new_added_customers.select("new.*")
        type_2_update = type_2_update.select("new.*")
        type_1_update = type_1_update.select("new.*")

        # Now we will save rows based on there Scenarios
        # Saving new customers
        new_added_customers = new_added_customers.withColumns({
                'valid_from': col('updated_at'),
                'valid_till': lit(None),
                'is_current': lit(True)
        })
        new_added_customers.write.mode('append').format('delta').save('/Volumes/workspace/default/cardpulse/silver/customers')
        logger.info("Appneded newly added customers")

        # Saving Type 2 Updates, by closing records and adding new rows
        from delta.tables import DeltaTable
        silver = DeltaTable.forPath(
            spark,
            "/Volumes/workspace/default/cardpulse/silver/customers"
        )
        silver.alias("old").merge(
            type_2_update.alias("new"),
            "old.customer_id = new.customer_id AND old.is_current = true"
        ).whenMatchedUpdate(
            set={
                "is_current": "false",
                "valid_till": "new.updated_at"
            }
        ).execute()

        
        new_versions = type_2_update.withColumns(
            {"valid_from": col("updated_at"), "valid_till": lit(None), "is_current":lit(True)}
        )
        new_versions.write.mode("append").format("delta").save("/Volumes/workspace/default/cardpulse/silver/customers")
        logger.info("Type 2 SCD Updated and new values saved")

        # Saving type 1 update
        silver.alias("old").merge(type_1_update.alias("new"), "old.customer_id = new.customer_id AND old.is_current = true"
        ).whenMatchedUpdate(set={"customer_name": "new.customer_name","total_cards_issued": "new.total_cards_issued"}).execute()
        logger.info("Type 1 SCD Updated")

        # Logging about unchanged rows
        processed = (new_added_customers.count()+ type_1_update.count()+ type_2_update.count())
        unchanged = good_rows.count() - processed
        logger.info(f"{unchanged} rows unchanged")

        # Updating Pipeline Metadata
        update_metadata(good_rows, pipeline_name, table_name)

    except Exception as e:
        logger.exception(f"Processing Customers failed.")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC #### Functions for Cards

# COMMAND ----------

def clean_cards(new_cards, SILVER_CUSTOMERS_PATH):
    try:
        # Deduplicating cards by using window
        card_window = Window.partitionBy('card_id').orderBy(desc('updated_at'))
        new_cards = new_cards.withColumn('row_num', row_number().over(card_window)).filter(col('row_num')==1).drop(col('row_num'))

        # standardization of text columns.
        new_cards = new_cards.withColumns({
            'card_brand_type': initcap(trim(col('card_brand_type'))),
            'card_holder_id': trim(col('card_holder_id')),
            'card_holder_type': initcap(trim(col('card_holder_type'))),
            'card_id': trim(col('card_id')),
            'card_name': initcap(trim(col('card_name'))),
            'card_rewards_category': initcap(trim(col('card_rewards_category'))),
            'card_status': initcap(trim(col('card_status')))
        })

        # Deriving the card_brand_type from card_name
        new_cards = new_cards.withColumn('card_brand_type', 
            when(col("card_name").contains("RuPay"), "RuPay")
            .when(col("card_name").contains("Visa"), "Visa")
            .when(col("card_name").contains("Mastercard"), "Mastercard")
            .otherwise(col("card_brand_type"))
        )

        # Changing datatypes of expiry_date, updated_at
        new_cards = new_cards.withColumns({
            'expiry_date': col('expiry_date').cast('date'),
            'updated_at': col('updated_at').cast('timestamp')
        })

        # Joining current customers with cards for referential integrity
        customers = spark.read.format('delta').load(SILVER_CUSTOMERS_PATH)
        active_customers = customers.filter(col('is_current')).select('customer_id').withColumnRenamed('customer_id','active_customer_id')
        new_cards = new_cards.join(active_customers, active_customers['active_customer_id'] == new_cards['card_holder_id'], "left")

        # Adding Metadata Columns
        new_cards = new_cards.withColumns({'silver_processed_at': current_timestamp(), 'data_source':lit('bronze')})

        # Change column order
        new_cards = new_cards.select('card_id', 'card_name', 'card_brand_type', 'card_holder_id', 'active_customer_id', 'card_holder_type', 'card_rewards_category', 'card_status', 'credit_limit', 'expiry_date', 'updated_at', 'data_source', 'silver_processed_at')

        return new_cards
    
    except Exception as e:
        logger.exception(f"Cleaning Cards failed")
        raise

# COMMAND ----------

def process_cards(BRONZE_CARDS_PATH, pipeline_name, table_name, SILVER_CUSTOMERS_PATH):
    try:
        logger.info('Processing Cards Started...')
        # Getting last_run for this table
        last_run = load_last_run(pipeline_name, table_name)
        # Loading new rows, If pipeline is running for first time then full load else incremental load
        new_cards = load_data_from_bronze(BRONZE_CARDS_PATH, last_run)
        row_count = new_cards.count()
        if row_count == 0: 
            logger.info('There are 0 new cards, thus early exiting')
            return # Early exit if now rows exists
        
        else:
            # Cleaning Cards Data
            cleaned_cards = clean_cards(new_cards, SILVER_CUSTOMERS_PATH)
            cleaned_cards_count = cleaned_cards.count()
            
            # Defining rules then validating based on that
            rules = [
                col('card_id').isNotNull(),
                col('active_customer_id').isNotNull() 
            ]
            good_rows, bad_rows = validation_engine(cleaned_cards, rules, row_count, cleaned_cards_count)
            
            # Storing bad_rows in quarantine layer
            bad_rows = bad_rows.withColumn(
                "failed_reason",
                when(col("card_id").isNull() , "card_id_missing")
                .when(col('active_customer_id').isNull(), "cardholder_not_found")
                .otherwise("multiple_rules_failed")
            ) # Adding reasons for quarantine
            # Droping our helper column
            bad_rows = bad_rows.drop('active_customer_id')
            bad_rows.write.mode('append').format('delta').save('/Volumes/workspace/default/cardpulse/quarantine/cards')
            good_rows = good_rows.drop('active_customer_id')

            # Saving Good rows, If pipeline is running for first time the full save, othwise merge based idempotancy
            if last_run is None:
                good_rows.write.mode('overwrite').format('delta').save('/Volumes/workspace/default/cardpulse/silver/cards')
                
            else:
                existing_cards = DeltaTable.forPath(spark, '/Volumes/workspace/default/cardpulse/silver/cards')
                update_map = {
                    'card_status': 'source.card_status',
                    'credit_limit': 'source.credit_limit',
                    'card_name': 'source.card_name',
                    'card_rewards_category': 'source.card_rewards_category',
                    'updated_at': 'source.updated_at',
                    'silver_processed_at': 'source.silver_processed_at'
                }

                insert_map = {
                    'card_id': 'source.card_id',
                    'card_name': 'source.card_name',
                    'card_brand_type': 'source.card_brand_type',
                    'card_holder_id': 'source.card_holder_id',
                    'card_holder_type': 'source.card_holder_type',
                    'card_rewards_category': 'source.card_rewards_category',
                    'card_status': 'source.card_status',
                    'credit_limit': 'source.credit_limit',
                    'expiry_date': 'source.expiry_date',
                    'updated_at': 'source.updated_at',
                    'silver_processed_at': 'source.silver_processed_at',
                    'data_source': 'source.data_source'
                }

                existing_cards.alias('target').merge(
                    good_rows.alias('source'), 'target.card_id = source.card_id'
                ).whenMatchedUpdate(set=update_map).whenNotMatchedInsert(values=insert_map).execute()

            update_metadata(good_rows, pipeline_name, table_name)
            logger.info(f'{good_rows.count()} Rows saved to Silver Cards')
    
    except Exception as e:
        logger.exception(f"Processing Cards Failed")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC #### Functions for Transactions

# COMMAND ----------

def clean_transactions(new_transactions, SILVER_MERCHANTS_PATH, SILVER_CUSTOMERS_PATH, SILVER_CARDS_PATH):
    try:
        # De-duplication based on recent transaction
        transactions_window = Window.partitionBy('transaction_id').orderBy(desc('updated_at'))
        new_transactions = new_transactions.withColumn('row_number', row_number().over(transactions_window)).filter(col('row_number')==1).drop('row_number')
        
        # flagging -tive amount with is_negative_amount_valid where refund is +tive and and Non-refund is -tive
        new_transactions = new_transactions.withColumn('is_negative_amount_valid', when(((col("transaction_type") == "Refund") & (col("amount") < 0)) | ((col("transaction_type") != "Refund") & (col("amount") >= 0)), True).otherwise(False))

        # fixing standardization issue of currency
        new_transactions = new_transactions.withColumn('currency', upper(trim(col('currency')))) 
        new_transactions = new_transactions.withColumn(
            'currency', 
            when(col('currency').rlike('USD|\\$'), 'USD')
            .when(col('currency').rlike('INR|₹'), 'INR')
            .when(col('currency').rlike('GBP|£'), 'GBP')
            .when(col('currency').rlike('YEN|JPY|¥'), 'JPY') # Standardized to JPY
            .otherwise(col('currency'))
        )

        # Casing the texual columns
        new_transactions = new_transactions.withColumns({
            'customer_id': trim(col('customer_id')),
            'device_channel': trim(col('device_channel')),
            'merchant_id': trim(col('merchant_id')),
            'status': initcap(trim(col('status'))),
            'transaction_id': trim(col('transaction_id')),
            'transaction_type': initcap(trim(col('transaction_type')))
        })

        # changing the data types of transaction_timestamp, updated_at
        new_transactions = new_transactions.withColumns({
            'transaction_timestamp': col('transaction_timestamp').cast('timestamp'),
            'updated_at': col('updated_at').cast('timestamp')
        })

        # changing column name status -> transactions_status
        new_transactions = new_transactions.withColumnRenamed('status','transaction_status')

        # Joining current customers with cards for referential integrity
        customers = spark.read.format('delta').load(SILVER_CUSTOMERS_PATH)
        active_customers = customers.filter(col('is_current')).select('customer_id').withColumnRenamed('customer_id','active_customer_id')
        cards = spark.read.format('delta').load(SILVER_CARDS_PATH)
        active_cards = cards.select('card_id').withColumnRenamed('card_id', 'active_card_id')
        merchants = spark.read.format('delta').load(SILVER_MERCHANTS_PATH)
        active_merchants = merchants.select('merchant_id').withColumnRenamed('merchant_id', 'active_merchant_id')

        new_transactions = new_transactions.join(active_customers, active_customers['active_customer_id'] == new_transactions['customer_id'], "left")
        new_transactions = new_transactions.join(active_cards, active_cards['active_card_id'] == new_transactions['card_id'], "left")
        new_transactions = new_transactions.join(active_merchants, active_merchants['active_merchant_id'] == new_transactions['merchant_id'], "left")

        # Adding Metadata columns
        new_transactions = new_transactions.withColumns({'silver_processed_at': current_timestamp(), 'data_source':lit('bronze')})

        # Re-ordering Columns
        new_transactions= new_transactions.select('transaction_id', 'card_id', 'customer_id', 'merchant_id', 'amount', 'currency', 'device_channel', 'transaction_status', 'transaction_type', 'transaction_timestamp', 'is_negative_amount_valid', 'updated_at', 'silver_processed_at', 'data_source', 'active_customer_id', 'active_card_id', 'active_merchant_id')

        return new_transactions

    except Exception as e:
        logger.exception("Cleaning Transactions Failed")
        raise

# COMMAND ----------

def process_transactions(pipeline_name, table_name, bronze_table_path, SILVER_MERCHANTS_PATH, SILVER_CUSTOMERS_PATH, SILVER_CARDS_PATH):
    try:
        logger.info('Processing Transactions Started')
        last_run = load_last_run(pipeline_name, table_name)
        new_transactions = load_data_from_bronze(bronze_table_path, last_run)
        row_count = new_transactions.count()
        if row_count == 0:
            logger.info('There are no new rows in Transactions, thus returning')
            return
        
        cleaned_transactions = clean_transactions(new_transactions, SILVER_MERCHANTS_PATH, SILVER_CUSTOMERS_PATH, SILVER_CARDS_PATH)
        cleaned_transactions_count = cleaned_transactions.count()

        rules = [
            col('transaction_id').isNotNull(),
            col('amount').isNotNull(),
            col('is_negative_amount_valid') == True,
            col('active_customer_id').isNotNull(),
            col('active_card_id').isNotNull(), 
            col('active_merchant_id').isNotNull(),
            col('currency').rlike('INR|USD|JPY|THB|SGD|GBP|AED|EUR|KWD|BHD|OMR|CAD|CNY')
        ]
        good_rows, bad_rows = validation_engine(cleaned_transactions, rules, row_count, cleaned_transactions_count)

        bad_rows = bad_rows.withColumn(
            'failed_reasons',
            array(
                when(~col('transaction_id').isNotNull(), lit("Missing Transaction ID")),
                when(~col('amount').isNotNull(), lit("Missing Transaction Amount")),
                when(~(col('is_negative_amount_valid') == True), lit("Invalid Negative Amount Flag")),
                when(~col('active_customer_id').isNotNull(), lit("Missing or Inactive Customer Profile")),
                when(~col('active_card_id').isNotNull(), lit("Card Reference Not Found")),
                when(~col('active_merchant_id').isNotNull(), lit("Merchant Profile Not Found")),
                when(~col('currency').rlike('INR|USD|JPY|THB|SGD|GBP|AED|EUR|KWD|BHD|OMR|CAD|CNY'), lit("Unsupported or Invalid Currency"))
            )
        )
        bad_rows = bad_rows.withColumn('failed_reasons', array_remove(col('failed_reasons'), None)).drop('active_customer_id', 'active_card_id', 'active_merchant_id')
        bad_rows.write.mode('append').format('delta').save('/Volumes/workspace/default/cardpulse/quarantine/transactions')
        
        good_rows = good_rows.drop('is_negative_amount_valid', 'active_customer_id', 'active_card_id', 'active_merchant_id')
        if last_run == None:
            good_rows.write.mode('overwrite').format('delta').save('/Volumes/workspace/default/cardpulse/silver/transactions')
            
        else:
            existing_transactions = DeltaTable.forPath(spark, '/Volumes/workspace/default/cardpulse/silver/transactions')
            update_map = {
                'transaction_status': 'source.transaction_status',
                'transaction_type': 'source.transaction_type',
                'transaction_timestamp': 'source.transaction_timestamp',
                'updated_at': 'source.updated_at',
                'silver_processed_at': 'source.silver_processed_at'
            }

            insert_map = {
                'transaction_id' : 'source.transaction_id', 	
                'card_id' : 'source.card_id',
                'customer_id' : 'source.customer_id',	
                'merchant_id' : 'source.merchant_id',	
                'amount' : 'source.amount',
                'currency' : 'source.currency',
                'device_channel' : 'source.device_channel',	
                'transaction_status' : 'source.transaction_status',	
                'transaction_type' : 'source.transaction_type',
                'transaction_timestamp' : 'source.transaction_timestamp',
                'updated_at' : 'source.updated_at',
                'silver_processed_at' : 'source.silver_processed_at',
                'data_source' : 'source.data_source'
            }

            existing_transactions.alias('target').merge(
                good_rows.alias('source'), 'target.transaction_id = source.transaction_id'
            ).whenMatchedUpdate(set=update_map).whenNotMatchedInsert(values=insert_map).execute()

        update_metadata(good_rows, pipeline_name, table_name)
        logger.info(f'{good_rows.count()} Rows saved to Silver Transactions')

    except Exception as e:
        logger.exception("Processing Transactions Failed")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orchestration Pipeline

# COMMAND ----------

def bronze_to_silver_orchestration_pipeline():
    try:
        logger.info('Bronze to Silver Orchestration Pipeline Started...')
        process_merchants(BRONZE_MERCHANTS_PATH)
        process_customers(BRONZE_CUSTOMERS_PATH, 'bronze_to_silver', 'silver_customers')
        process_cards(BRONZE_CARDS_PATH, 'bronze_to_silver', 'silver_cards', SILVER_CUSTOMERS_PATH)
        process_transactions('bronze_to_silver', 'silver_transactions', BRONZE_TRANSACTIONS_PATH, SILVER_MERCHANTS_PATH, SILVER_CUSTOMERS_PATH, SILVER_CARDS_PATH)
        logger.info('Bronze to Silver Orchestration Pipeline Sucessfully Finshed.')
    
    except Exception as e:
        logger.exception("Orchestration pipeline failed.")
        raise

# COMMAND ----------

bronze_to_silver_orchestration_pipeline()