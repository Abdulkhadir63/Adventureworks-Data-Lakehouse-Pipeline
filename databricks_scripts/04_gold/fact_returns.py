# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import logging

# 1. LOGGER CONFIGURATION
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 2. RUNTIME PARAMETERS (SOURCE WIDGETS)
dbutils.widgets.text("catalog", "spark_airflow_adventure_work_project", "Catalog")
dbutils.widgets.text("data_source", "returns", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

silver_schema = "silver"
gold_schema = "gold"

target_table_name = f"fact_{data_source}"

source_table = f"{catalog}.{silver_schema}.{data_source}"
target_table = f"{catalog}.{gold_schema}.{target_table_name}"
gold_load_path = f"s3://airflow-spark-project/gold/{target_table_name}"

# 3. HIGH-WATER MARK RETRIEVAL
def get_gold_high_water_mark(table_name):
    """
    Retrieves the maximum silver_processed_timestamp loaded into the Gold fact table in prior runs.
    Returns epoch timestamp if table does not exist yet.
    """
    if spark.catalog.tableExists(table_name):
        max_ts = spark.table(table_name).agg(F.max("max_silver_processed_timestamp")).collect()[0][0]
        if max_ts:
            logger.info(f"Found existing Gold High-Water Mark: {max_ts}")
            return max_ts
            
    logger.info("No prior Gold High-Water Mark found. Executing initial baseline load.")
    return "1970-01-01 00:00:00"

last_processed_timestamp = get_gold_high_water_mark(target_table)

# 4. INCREMENTAL READ FROM SILVER
logger.info(f"Scanning Silver source '{source_table}' where silver_processed_timestamp > '{last_processed_timestamp}'")

df_silver_batch = spark.read.table(source_table) \
    .filter(F.col("silver_processed_timestamp") > F.lit(last_processed_timestamp))

# Short-circuit if no new records exist in Silver
if df_silver_batch.isEmpty():
    logger.info("No new Silver records to process. Exiting Gold ingestion successfully.")
    dbutils.notebook.exit("SUCCESS_NO_NEW_DATA")

# Capture maximum watermark timestamp for active micro-batch
current_max_silver_ts = df_silver_batch.agg(F.max("silver_processed_timestamp")).collect()[0][0]

# 5. BUSINESS TRANSFORMATIONS, CASTING, & METADATA INJECTION
df_gold_batch = df_silver_batch.select(
    F.col("ReturnKey").alias("ReturnKey"),
    F.col("ReturnDate").cast("date").alias("ReturnDate"),
    F.col("TerritoryKey").cast("int").alias("TerritoryKey"),
    F.col("ProductKey").cast("int").alias("ProductKey"),
    F.col("ReturnQuantity").cast("int").alias("ReturnQuantity")
) \
.withColumn("gold_processed_timestamp", F.current_timestamp()) \
.withColumn("max_silver_processed_timestamp", F.lit(current_max_silver_ts))

# 6. DELTA UPSERT (MERGE) LAYER
logger.info(f"Writing transformed batch to Gold Delta table: {target_table}")

if spark.catalog.tableExists(target_table):
    gold_delta_table = DeltaTable.forName(spark, target_table)
    
    gold_delta_table.alias("target") \
        .merge(
            source = df_gold_batch.alias("updates"),
            condition = "target.ReturnKey = updates.ReturnKey"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    logger.info("Delta merge upsert successfully executed.")
else:
    logger.info(f"Target table does not exist. Initializing baseline schema at: {gold_load_path}")
    df_gold_batch.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", gold_load_path) \
        .saveAsTable(target_table)
    logger.info("Baseline Delta table successfully created.")

print(f"Transactional Returns Fact table process completed successfully for '{target_table}'. Advanced Watermark to: {current_max_silver_ts}")