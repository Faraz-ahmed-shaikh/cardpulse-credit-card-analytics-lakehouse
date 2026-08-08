# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from delta.tables import DeltaTable
import datetime
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
# MAGIC #### Modular Function used by each functions

# COMMAND ----------

# A Modular function which we will be used to update pipline's metadata
def update_metadata(gold_data, pipeline_name, table_name):
    try:
        # We will use current timestamp, as we an't using updated_at & incremental loading in gold 
        last_processed_timestamp = datetime.datetime.now()
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
# MAGIC #### Customer Spending Summary

# COMMAND ----------

def spent_summary_aggregation(transactions):
    try:
        # creating a month field for month grain
        transactions = transactions.withColumn('month', date_trunc('month', col('transaction_timestamp')))

        # Spliting the data in two: real spend, and refunds (money coming back)
        spend_txns = transactions.filter(col('transaction_type').isin('Purchase', 'EMI Conversion'))
        refund_txns = transactions.filter(col('transaction_type') == 'Refund')

        # Now making monthly_spend to find How much did each customer actually spend, per month?
        monthly_spend = spend_txns.groupBy('customer_id', 'month').agg(
            sum('amount').alias('total_spend'),
            count('transaction_id').alias('transaction_count'),
            avg('amount').alias('avg_transaction_amount')
        )

        #  How much was refunded to each customer, per month? Tracked on its own, never blended into spend
        monthly_refunds = refund_txns.groupBy('customer_id', 'month').agg(
            sum(abs(col('amount'))).alias('total_refunded')  # abs() makes this a positive number — "amount refunded", not a negative spend figure
        )

        # Now building category spend to answer, which merchant category did each customer spend the most on, each month?
        category_spend = spend_txns.groupBy('customer_id', 'month', 'merchant_category').agg(sum('amount').alias('category_spend'))

        # Now for each customer-month, rank the categories by spend and keep only the #1
        category_window = Window.partitionBy('customer_id', 'month').orderBy(desc('category_spend'))
        top_category = category_spend.withColumn('rank', row_number().over(category_window)).filter(col('rank') == 1).select('customer_id', 'month', col('merchant_category').alias('top_merchant_category'))

        # Combine everything into one row per customer, per month 
        monthly_summary = monthly_spend.join(monthly_refunds, on=['customer_id', 'month'], how='left').join(top_category, on=['customer_id', 'month'], how='left').withColumn('total_refunded', coalesce(col('total_refunded'), lit(0.0)))

        # Now talking about trend how does this month's spend compare to last month's, for this same customer?
        customer_month_window = Window.partitionBy('customer_id').orderBy('month')
        monthly_summary = monthly_summary.withColumn(
            'prev_month_spend', lag('total_spend').over(customer_month_window)
        )
        monthly_summary = monthly_summary.withColumn(
            'mom_spend_change_pct',
            when(col('prev_month_spend').isNotNull() & (col('prev_month_spend') != 0),
                (col('total_spend') - col('prev_month_spend')) / col('prev_month_spend') * 100)
        )

        # Detecting Unusual behavior: is this month's spend way higher than this customer's own recent normal? -
        # rowsBetween(-3, -1) means "the 3 rows before this one, not counting this one" 
        trailing_window = Window.partitionBy('customer_id').orderBy('month').rowsBetween(-3, -1)
        monthly_summary = monthly_summary.withColumn(
            'trailing_3mo_avg_spend', avg('total_spend').over(trailing_window)
        )
        monthly_summary = monthly_summary.withColumn(
            'is_anomalous_spend',
            when(col('trailing_3mo_avg_spend').isNotNull(),
                col('total_spend') > 2 * col('trailing_3mo_avg_spend')).otherwise(False)
        )

        return monthly_summary
    
    except Exception as e:
        logger.exception("Aggregating Spend Summary Failed")
        raise

# COMMAND ----------

def process_gold_spend_summary(SILVER_TRANSACTIONS_PATH, SILVER_MERCHANTS_PATH):
    try:
        logger.info("Processing Customer Spending Started....")
        # First loading Silver transactions and getting only "Successful" ones
        transactions = spark.read.format('delta').load(SILVER_TRANSACTIONS_PATH).filter(col('transaction_status') == 'Successful')
        # Then bringing in merchant_category so we can later ask "what category does this customer spend on most"
        merchants = spark.read.format('delta').load(SILVER_MERCHANTS_PATH).select('merchant_id', 'merchant_category')
        transactions = transactions.join(merchants, on='merchant_id', how='left')
        monthly_summary = spent_summary_aggregation(transactions)
        monthly_summary.write.mode('overwrite').format('delta').save('/Volumes/workspace/default/cardpulse/gold/customers_spend_summary')
        logger.info(f"{monthly_summary.count()} Rows Sucessfully saved to gold/customers_spend_summary")
        update_metadata(monthly_summary, 'silver_to_gold', 'customers_spend_summary')  # just for logging when it last ran
        logger.info("Successfully Processed Customer Spending.")

    except Exception as e:
        logger.exception("Processing Gold Spend Summary Failed")
        raise
        

# COMMAND ----------

# MAGIC %md
# MAGIC #### Portfolio Health

# COMMAND ----------

def risk_summary_aggregation(customers, cards, spend):
    try:
        # Rolling up each customer's cards into one row: total exposure, card count, trouble count
        card_summary = cards.groupBy('card_holder_id').agg(sum('credit_limit').alias('total_credit_limit'),count('card_id').alias('card_count'), sum(when(col('card_status').isin('Blocked', 'Lost'), 1).otherwise(0)).alias('blocked_or_lost_card_count')
        ).withColumnRenamed('card_holder_id', 'customer_id')

        # Pulling latest month's spend from the Gold table we already built and we only want the MOST RECENT month for each customer, since this is a  snapshot
        latest_month_window = Window.partitionBy('customer_id').orderBy(desc('month'))
        current_month_spend = spend.withColumn('rank', row_number().over(latest_month_window)).filter(col('rank') == 1).select('customer_id', col('total_spend').alias('current_month_spend'))

        # Combine everything into one row per customer
        risk_summary = customers.join(card_summary, on='customer_id', how='left').join(current_month_spend, on='customer_id', how='left')

        # fill in zeros for customers who had no matching cards/spend rows (the left join leaves nulls otherwise)
        risk_summary = risk_summary.withColumn('total_credit_limit', coalesce(col('total_credit_limit'), lit(0.0))).withColumn('card_count', coalesce(col('card_count'), lit(0))).withColumn('blocked_or_lost_card_count', coalesce(col('blocked_or_lost_card_count'), lit(0))).withColumn('current_month_spend', coalesce(col('current_month_spend'), lit(0.0)))

        # Credit utilization: how much of their available credit are they actually using
        risk_summary = risk_summary.withColumn(
            'credit_utilization_pct',
            when(col('total_credit_limit') > 0, (col('current_month_spend') / col('total_credit_limit')) * 100).otherwise(0.0)
        )
        # The combined "needs attention" flag
        risk_summary = risk_summary.withColumn(
            'is_high_risk_flag',
            (col('risk_grade') == 'High') |
            (col('customer_status').isin('Blocked', 'Dormant')) |
            (col('credit_utilization_pct') > 80)
        )

        return risk_summary

    except Exception as e:
        logger.exception("Risk Summary Aggregations Failed")
        raise

# COMMAND ----------

def process_portfolio_risk_summary(SILVER_CUSTOMERS_PATH, SILVER_CARDS_PATH, GOLD_SPEND_SUMMARY_PATH):
    try:
        logger.info("Processing Portfolio Risk Summary Started...")
        # Loading customers [only current version]
        customers = spark.read.format('delta').load(SILVER_CUSTOMERS_PATH).filter(col('is_current') == True).select('customer_id', 'risk_grade', 'customer_status')
        # Loading Cards data 
        cards = spark.read.format('delta').load(SILVER_CARDS_PATH)
        # Loading Spending Summary
        spend = spark.read.format('delta').load(GOLD_SPEND_SUMMARY_PATH)
        # Processing Risk Summary
        risk_summary = risk_summary_aggregation(customers, cards, spend)
        # Saving Risk Summary
        risk_summary.write.mode('overwrite').format('delta').save('/Volumes/workspace/default/cardpulse/gold/portfolio_risk_summary')
        logger.info(f"{risk_summary.count()} Rows Sucessfully saved to gold/portfolio_risk_summary")
        update_metadata(risk_summary, 'silver_to_gold', 'portfolio_risk_summary')  # just for logging when it last ran
        logger.info("Successfully Processed Portfolio Risk Summary.")

    except Exception as e:
        logger.exception("Processing Portfolio Risk Summary Failed")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC #### Customer Segmentation

# COMMAND ----------

def customer_segmentation_aggregation(customers, cards, spend):
    try:
        # Calculating how long has this customer been with business, in months!
        customers = customers.withColumn('customer_tenure_months',round(months_between(current_date(), col('joining_date'))))

        # Finding each customer's most common card_holder_type (Consumer/Commercial/Corporate)
        # counting how many cards of each type this customer has
        card_type_counts = cards.groupBy('card_holder_id', 'card_holder_type').agg(count('card_id').alias('type_count'))
        # Now for each customer, rank their card types by count and keep only the most common one
        card_type_window = Window.partitionBy('card_holder_id').orderBy(desc('type_count'))
        primary_card_type = card_type_counts.withColumn('rank', row_number().over(card_type_window)).filter(col('rank') == 1).select(col('card_holder_id').alias('customer_id'), col('card_holder_type').alias('primary_card_type'))

        # Same idea, but for their most common rewards category (Travel/Shopping/etc.)
        rewards_counts = cards.groupBy('card_holder_id', 'card_rewards_category').agg(count('card_id').alias('reward_count'))
        rewards_window = Window.partitionBy('card_holder_id').orderBy(desc('reward_count'))
        preferred_rewards_category = rewards_counts.withColumn('rank', row_number().over(rewards_window)).filter(col('rank') == 1).select(col('card_holder_id').alias('customer_id'), col('card_rewards_category').alias('preferred_rewards_category'))

        # Customer's typical monthly spend — the average of every month we have in gold_spend_summary ---
        avg_spend = spend.groupBy('customer_id').agg(avg('total_spend').alias('avg_monthly_spend'))

        # Now Combine everything into one row per customer 
        segmentation = customers.join(primary_card_type, on='customer_id', how='left').join(preferred_rewards_category, on='customer_id', how='left').join(avg_spend, on='customer_id', how='left')

        # Bucketing each customer into a spend segment, using the threshold 
        segmentation = segmentation.withColumn('spend_segment',
            when(col('avg_monthly_spend') <= 5000, 'Low')
            .when((col('avg_monthly_spend') > 5000) & (col('avg_monthly_spend') <= 20000), 'Medium')
            .when((col('avg_monthly_spend') > 20000) & (col('avg_monthly_spend') <= 60000), 'High')
            .when(col('avg_monthly_spend') > 60000, 'Very High')
            .otherwise('Unknown')  # covers customers with no spend history at all (avg_monthly_spend is null)
        )

        return segmentation
    
    except Exception as e:
        logger.exception("Customer Segmentation Aggregation Failed")
        raise

# COMMAND ----------

def process_customer_segmentation(SILVER_CUSTOMERS_PATH, SILVER_CARDS_PATH, GOLD_SPEND_SUMMARY_PATH):
    try:
        logger.info("Processing Customer Segmentation Started...")
        # Loading Our Customers [only the CURRENT version of each customer], cards and spend
        customers = spark.read.format('delta').load(SILVER_CUSTOMERS_PATH).filter(col('is_current') == True).select('customer_id', 'customer_city', 'occupation', 'income_bucket', 'joining_date')
        cards = spark.read.format('delta').load(SILVER_CARDS_PATH)
        spend = spark.read.format('delta').load(GOLD_SPEND_SUMMARY_PATH)
        # Processing Customer Segmentation
        customer_segmentation = customer_segmentation_aggregation(customers, cards, spend)
        # Saving Risk Summary
        customer_segmentation.write.mode('overwrite').format('delta').save('/Volumes/workspace/default/cardpulse/gold/customer_segmentation_summary')
        logger.info(f"{customer_segmentation.count()} Rows Sucessfully saved to gold/customer_segmentation_summary")
        update_metadata(customer_segmentation, 'silver_to_gold', 'customer_segmentation_summary')  # just for logging when it last ran
        logger.info("Successfully Processed Customer Segmentation Summary.")

    except Exception as e:
        logger.exception("Processing Customer Segmentation Summary Failed")
        raise

# COMMAND ----------

def silver_to_gold_orchestration_pipeline():
    try:
        logger.info("Silver to Gold Orchestration Pipeline Started...")
        process_gold_spend_summary(SILVER_TRANSACTIONS_PATH, SILVER_MERCHANTS_PATH)
        process_portfolio_risk_summary(SILVER_CUSTOMERS_PATH, SILVER_CARDS_PATH, GOLD_SPEND_SUMMARY_PATH)
        process_customer_segmentation(SILVER_CUSTOMERS_PATH, SILVER_CARDS_PATH, GOLD_SPEND_SUMMARY_PATH)
        logger.info("Silver to Gold Orchestration Pipeline Completed Sucessfully.")

    except Exception as e:
        logger.exception("Silver to Gold Orchestration Failed")
        raise

# COMMAND ----------

silver_to_gold_orchestration_pipeline()