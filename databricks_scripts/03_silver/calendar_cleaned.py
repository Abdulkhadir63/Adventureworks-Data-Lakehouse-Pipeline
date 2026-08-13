# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging

# 3. LOGGER CONFIGURATION FIX (PREVENTS LOG LOSS IN DATABRICKS NOTEBOOKS)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 1. Initialize Runtime Parameters via Widgets
dbutils.widgets.text("catalog", "spark_airflow_adventure_work_project", "Catalog")
dbutils.widgets.text("data_source", "calendar", "Data Source")

data_source = dbutils.widgets.get("data_source")
catalog = dbutils.widgets.get("catalog")

bronze_schema = "bronze"
silver_schema = "silver"

# Unity Catalog Table Pointers
source_table = f"{catalog}.{bronze_schema}.{data_source}"
target_table = f"{catalog}.{silver_schema}.{data_source}"

# 7. S3 PATH CONFIGURATION
silver_load_path = f"s3://airflow-spark-project/silver/{data_source}"
silver_checkpoint = f"s3://airflow-spark-project/checkpoints/silver_checkpoint/bronze_to_silver_{data_source}/"

logger.info(f"Reading Stream from Input Table: {source_table}")
logger.info(f"Tracking with Checkpoint: {silver_checkpoint}")
logger.info(f"Writing physically to: {silver_load_path}")
logger.info(f"Registering logically as Table: {target_table}")

# 2. MODULAR TRANSFORMATION FUNCTION
def transform_calendar_silver_tier(df_raw):
    """
    Applies explicit date string parsing, formats standard time hierarchies,
    isolates structural weekend metrics, and appends a silver processing timestamp.
    """
    df_standardized = df_raw.withColumn(
        "Date",
        F.coalesce(
            F.expr("try_to_date(Date, 'M/d/yyyy')"),
            F.expr("try_to_date(Date, 'M-d-yyyy')"),
            F.expr("try_to_date(Date, 'yyyy-MM-dd')"),
            F.expr("try_to_date(Date, 'dd/MM/yyyy')")
        )
    )
    
    return (
        df_standardized
        .withColumn("year", F.year(F.col("Date")))
        .withColumn("month", F.month(F.col("Date")))
        .withColumn("quarter", F.quarter(F.col("Date")))
        .withColumn("dayOfMonth", F.dayofmonth(F.col("Date")))
        .withColumn("DayName", F.initcap(F.date_format(F.col("Date"), "EEEE")))
        .withColumn(
            "IsWeekend",
            F.when(F.dayofweek(F.col("Date")).isin(1, 7), F.lit(True)).otherwise(F.lit(False))
        )
        .withColumn("silver_processed_timestamp", F.current_timestamp()) # 5. Keep silver_processed_timestamp
    )

# -------------------------------------------------------------------------
# 1 & 2. ONE-TIME TABLE INITIALIZATION (OUTSIDE FOREACHBATCH)
# -------------------------------------------------------------------------
# Ensures table existence check and initial write mode run ONCE before streaming starts.
if not spark.catalog.tableExists(target_table):
    logger.info(f"Target Silver table {target_table} does not exist. Initializing empty table with mode('overwrite') at {silver_load_path}")
    
    # Create empty schema baseline
    empty_schema_df = spark.read.table(source_table).limit(0)
    empty_transformed_df = transform_calendar_silver_tier(empty_schema_df)
    
    target_columns = [
        "Date",
        "year",
        "month",
        "quarter",
        "dayOfMonth",
        "DayName",
        "IsWeekend",
        "silver_processed_timestamp"
    ]
    
    empty_transformed_df.select(*target_columns).write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", silver_load_path) \
        .saveAsTable(target_table)

# Pre-instantiate the Delta Table reference once outside the micro-batch loop
silver_delta_table = DeltaTable.forName(spark, target_table)

# -------------------------------------------------------------------------
# 3. FOREACHBATCH MICRO-BATCH MERGE FUNCTION
# -------------------------------------------------------------------------
def process_silver_micro_batch(df_micro_batch, batch_id):
    # 4. isEmpty() check
    if df_micro_batch.isEmpty():
        logger.info(f"Micro-batch {batch_id} is empty. Skipping.")
        return

    logger.info(f"Processing micro-batch ID: {batch_id}")

    # 6. ENHANCED VALIDATION LAYER
    df_validated_batch = df_micro_batch.filter(
        F.col("Date").isNotNull() & 
        F.col("year").isNotNull() & 
        F.col("month").isNotNull()
    )

    # 8. WINDOW DEDUPLICATION AT DATE GRAIN (INGESTION_TIMESTAMP DESC)
    window_spec = Window.partitionBy("Date").orderBy(F.col("ingestion_timestamp").desc())
    df_deduplicated = (
        df_validated_batch
        .withColumn("row_rank", F.row_number().over(window_spec))
        .filter(F.col("row_rank") == 1)
        .drop("row_rank")
    )

    # Target Column Projections
    target_columns = [
        "Date",
        "year",
        "month",
        "quarter",
        "dayOfMonth",
        "DayName",
        "IsWeekend",
        "silver_processed_timestamp"
    ]
    df_final_batch = df_deduplicated.select(*target_columns)

    # FAST INCREMENTAL UPSERT (NO CATALOG LOOKUPS INSIDE MICRO-BATCH)
    silver_delta_table.alias("target") \
        .merge(
            source = df_final_batch.alias("updates"),
            condition = "target.Date = updates.Date"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

# -------------------------------------------------------------------------
# 4. STREAMING EXECUTION LAYER
# -------------------------------------------------------------------------
logger.info(f"Starting Delta Stream from {source_table}")

df_bronze_stream = (
    spark.readStream
    .format("delta")
    .table(source_table)
)

df_transformed_stream = transform_calendar_silver_tier(df_bronze_stream)

query = (
    df_transformed_stream.writeStream
    .foreachBatch(process_silver_micro_batch)
    .option("checkpointLocation", silver_checkpoint)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

print(f"🚀 Incremental Streaming Upsert completed successfully for {data_source} into Silver target: {target_table}!")