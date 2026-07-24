# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging

logger = logging.getLogger(__name__)

# 1. Initialize Runtime Parameters via Widgets
dbutils.widgets.text("catalog", "spark_airflow_adventure_work_project", "Catalog")
dbutils.widgets.text("data_source", "calendar", "Data Source")
dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run ID")

data_source = dbutils.widgets.get("data_source")
catalog = dbutils.widgets.get("catalog")
current_run_id = dbutils.widgets.get("pipeline_run_id")

bronze_schema = "bronze"
silver_schema = "silver"

# Unity Catalog Table Pointers
source_table = f"{catalog}.{bronze_schema}.{data_source}"
target_table = f"{catalog}.{silver_schema}.{data_source}"

# Decoupled AWS S3 Storage Location
silver_load_path = f"s3://airflow-spark-project/silver/{data_source}"


# 2. MODULAR TRANSFORMATION FUNCTION
def transform_calendar_silver_tier(df_raw):
    """
    Applies explicit date string parsing, formats standard time hierarchies,
    and isolates structural weekend metrics for the Calendar dimension.
    """
    logger.info("Parsing dates and extracting calendar components")
    
    df_standardized = df_raw \
        .withColumn("Date", F.coalesce(
            F.expr("try_to_date(Date, 'M/d/yyyy')"),
            F.expr("try_to_date(Date, 'M-d-yyyy')"),
            F.expr("try_to_date(Date, 'yyyy-MM-dd')"),
            F.expr("try_to_date(Date, 'dd/MM/yyyy')")
        ))
        
    return df_standardized \
        .withColumn("year", F.year(F.col("Date"))) \
        .withColumn("month", F.month(F.col("Date"))) \
        .withColumn("quarter", F.quarter(F.col("Date"))) \
        .withColumn("dayOfMonth", F.dayofmonth(F.col("Date"))) \
        .withColumn("DayName", F.initcap(F.date_format(F.col("Date"), "EEEE"))) \
        .withColumn(
            "IsWeekend", 
            F.when(F.dayofweek(F.col("Date")).isin(1, 7), F.lit(True)).otherwise(F.lit(False))
        ) \
        .withColumn("silver_processed_timestamp", F.current_timestamp())\
        .withColumn("pipeline_run_id", F.lit(current_run_id))   
    

# 3. METADATA-DRIVEN INCREMENTAL READ LAYER
logger.info(f"Scanning Bronze Table for Pipeline Run ID: {current_run_id}")

if not current_run_id.strip():
    raise ValueError("pipeline_run_id is required")
else:
    logger.info("Senior Strategy: Pushdown optimization filtering on target run ID metadata.")
    df_bronze_batch = spark.read.table(source_table).filter(F.col("pipeline_run_id") == current_run_id)


# 4. DATA VALIDATION & TRANSFORMATION LAYER
logger.info("Running date transformations prior to primary grain deduplication")
df_transformed = transform_calendar_silver_tier(df_bronze_batch)

# Drop records where the parsed date resolved to Null to prevent broken partition grains
df_validated_batch = df_transformed.filter(F.col("Date").isNotNull())


# 5. DETERMINISTIC DEDUPLICATION & PROJECTIONS
logger.info("Executing windowed deduplication based on unique Calendar Date grain")
window_spec = Window.partitionBy("Date").orderBy(F.col("ingestion_timestamp").desc())
df_deduplicated = df_validated_batch \
    .withColumn("row_rank", F.row_number().over(window_spec)) \
    .filter(F.col("row_rank") == 1) \
    .drop("row_rank")

# Columns perfectly matched with correct spelling definitions
target_columns = [
    "Date",
    "year",
    "month",
    "quarter",
    "dayOfMonth",
    "DayName",
    "IsWeekend",
    "silver_processed_timestamp",
    "pipeline_run_id"
]
df_final_batch = df_deduplicated.select(*target_columns)


# 6. EXTERNAL S3 DELTA MERGE WRITE LAYER
if spark.catalog.tableExists(target_table):
    logger.info(f"Target Silver table {target_table} exists. Merging new run updates into S3.")
    silver_delta_table = DeltaTable.forName(spark, target_table)
    
    silver_delta_table.alias("target") \
        .merge(
            source = df_final_batch.alias("updates"),
            condition = "target.Date = updates.Date"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
else:
    logger.info(f"Target Silver table does not exist. Initializing External Table at S3 path: {silver_load_path}")
    df_final_batch.write \
        .format("delta") \
        .mode("append") \
        .option("path", silver_load_path) \
        .saveAsTable(target_table)

print(f"🚀 Metadata-Driven Incremental Batch Upsert completed successfully for Run ID: {current_run_id}")

# COMMAND ----------

