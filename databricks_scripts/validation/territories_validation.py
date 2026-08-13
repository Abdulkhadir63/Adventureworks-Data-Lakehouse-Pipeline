# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.window import Window
import logging

# 1. LOGGER CONFIGURATION
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 2. RUNTIME PARAMETERS (Entity & Catalog Only)
dbutils.widgets.text("catalog", "spark_airflow_adventure_work_project", "Catalog")
dbutils.widgets.text("data_source", "territories", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

silver_schema = "silver"
quality_schema = "data_quality_check"

silver_table = f"{catalog}.{silver_schema}.{data_source}"
target_quality_table = f"{catalog}.{quality_schema}.{data_source}"

# 3. HIGH-WATER MARK RETRIEVAL
def get_validation_high_water_mark(table_name):
    """
    Retrieves the maximum silver_processed_timestamp evaluated in prior quality runs.
    Returns epoch timestamp if table does not exist yet.
    """
    if spark.catalog.tableExists(table_name):
        max_ts = spark.table(table_name).agg(F.max("max_silver_processed_timestamp")).collect()[0][0]
        if max_ts:
            logger.info(f"Found existing Validation High-Water Mark: {max_ts}")
            return max_ts
    
    logger.info("No prior Validation High-Water Mark found. Evaluating full Silver history.")
    return "1970-01-01 00:00:00"

last_validated_timestamp = get_validation_high_water_mark(target_quality_table)

# 4. INCREMENTAL READ FROM SILVER
logger.info(f"Scanning Silver {data_source} where silver_processed_timestamp > '{last_validated_timestamp}'")

df_inc_silver = spark.read.table(silver_table) \
    .filter(F.col("silver_processed_timestamp") > F.lit(last_validated_timestamp))

# Short-circuit if no new records were written to Silver
if df_inc_silver.isEmpty():
    logger.info("No new Silver records to validate. Exiting Quality Gate successfully.")
    dbutils.notebook.exit("SUCCESS_NO_NEW_DATA")

# Capture the maximum watermark of this active micro-batch
current_max_silver_ts = df_inc_silver.agg(F.max("silver_processed_timestamp")).collect()[0][0]

# 5. ROW-LEVEL DUPLICATE DETECTION & SINGLE-PASS AGGREGATION
primary_grain_window = Window.partitionBy("SalesTerritoryKey")
df_flagged = df_inc_silver \
    .withColumn("duplicate_flag", F.when(F.count("*").over(primary_grain_window) > 1, 1).otherwise(0))

metrics = df_flagged.select(
    F.count("*").alias("total_rows"),
    F.sum("duplicate_flag").alias("duplicate_rows"),
    
    # Required Structural Column Validations (Null & Blank Checks)
    F.sum(F.when(F.col("SalesTerritoryKey").isNull(), 1).otherwise(0)).alias("null_territory_key"),
    F.sum(F.when(F.col("Region").isNull() | (F.trim(F.col("Region")) == ""), 1).otherwise(0)).alias("null_region"),
    F.sum(F.when(F.col("Country").isNull() | (F.trim(F.col("Country")) == ""), 1).otherwise(0)).alias("null_country"),
    F.sum(F.when(F.col("Continent").isNull() | (F.trim(F.col("Continent")) == ""), 1).otherwise(0)).alias("null_continent")
).collect()[0]

# Extract scalar values with safe default fallbacks
total_rows = metrics["total_rows"] or 0
duplicate_rows = metrics["duplicate_rows"] or 0

null_territory_key = metrics["null_territory_key"] or 0
null_region = metrics["null_region"] or 0
null_country = metrics["null_country"] or 0
null_continent = metrics["null_continent"] or 0

# Track total critical structural validation errors
total_critical_failures = null_territory_key + null_region + null_country + null_continent + duplicate_rows

pipeline_passed = "PASS" if (total_critical_failures == 0 and total_rows > 0) else "FAIL"

# 6. REPORT GENERATION WITH WATERMARK SNAPSHOT
eval_time = F.current_timestamp()

validation_rows = [
    (data_source, "Row Count Verification", "PASS" if total_rows > 0 else "FAIL", int(total_rows if total_rows == 0 else 0), current_max_silver_ts),
    (data_source, "Unique Primary Key Grain", "PASS" if duplicate_rows == 0 else "FAIL", int(duplicate_rows), current_max_silver_ts),
    (data_source, "Required Column: SalesTerritoryKey", "PASS" if null_territory_key == 0 else "FAIL", int(null_territory_key), current_max_silver_ts),
    (data_source, "Required Column: Region", "PASS" if null_region == 0 else "FAIL", int(null_region), current_max_silver_ts),
    (data_source, "Required Column: Country", "PASS" if null_country == 0 else "FAIL", int(null_country), current_max_silver_ts),
    (data_source, "Required Column: Continent", "PASS" if null_continent == 0 else "FAIL", int(null_continent), current_max_silver_ts)
]

validation_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("rule_name", StringType(), False),
    StructField("status", StringType(), False),
    StructField("failed_rows", IntegerType(), False),
    StructField("max_silver_processed_timestamp", TimestampType(), False)
])

validation_df = spark.createDataFrame(validation_rows, schema=validation_schema) \
    .withColumn("evaluation_timestamp", eval_time)

# Append quality evaluation logs incrementally
logger.info(f"Writing quality metrics to audit table: {target_quality_table}")
validation_df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_quality_table)

# 7. HARD PIPELINE BREAKPOINT GATE
if pipeline_passed == "FAIL":
    raise ValueError(
        f"Data Quality Gate Failed for Silver Table '{data_source}'. "
        f"Critical failures detected: {total_critical_failures} (Null Territory Keys: {null_territory_key}, Null Regions: {null_region}, Null Countries: {null_country}, Null Continents: {null_continent}, Duplicates: {duplicate_rows}), Total Rows: {total_rows}. "
        f"Halting Gold ingestion."
    )

print(f"Data Quality Gate PASSED for '{data_source}'. Advanced Watermark to: {current_max_silver_ts}")