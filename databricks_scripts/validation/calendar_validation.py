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
dbutils.widgets.text("data_source", "calendar", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

silver_schema = "silver"
quality_schema = "data_quality_check"

silver_table = f"{catalog}.{silver_schema}.{data_source}"
target_quality_table = f"{catalog}.{quality_schema}.{data_source}"

# -------------------------------------------------------------------------
# 3. HIGH-WATER MARK RETRIEVAL (DECOUPLED FROM RUN IDs)
# -------------------------------------------------------------------------
def get_validation_high_water_mark(table_name):
    """
    Retrieves the maximum silver_processed_timestamp evaluated in prior quality runs.
    Returns epoch timestamp if table doesn't exist yet.
    """
    if spark.catalog.tableExists(table_name):
        max_ts = spark.table(table_name).agg(F.max("max_silver_processed_timestamp")).collect()[0][0]
        if max_ts:
            logger.info(f"Found existing Validation High-Water Mark: {max_ts}")
            return max_ts
    
    logger.info("No prior Validation High-Water Mark found. Evaluating full Silver history.")
    return "1970-01-01 00:00:00"

last_validated_timestamp = get_validation_high_water_mark(target_quality_table)

# -------------------------------------------------------------------------
# 4. INCREMENTAL READ FROM SILVER
# -------------------------------------------------------------------------
logger.info(f"Scanning Silver {data_source} where silver_processed_timestamp > '{last_validated_timestamp}'")

df_inc_silver = spark.read.table(silver_table) \
    .filter(F.col("silver_processed_timestamp") > F.lit(last_validated_timestamp))

# Short-circuit if no new records were written to Silver
if df_inc_silver.isEmpty():
    logger.info("No new Silver records to validate. Exiting Quality Gate successfully.")
    dbutils.notebook.exit("SUCCESS_NO_NEW_DATA")

# Capture the max watermark of this current evaluation batch
current_max_silver_ts = df_inc_silver.agg(F.max("silver_processed_timestamp")).collect()[0][0]

# -------------------------------------------------------------------------
# 5. ROW-LEVEL DUPLICATE DETECTION & SINGLE-PASS AGGREGATION
# -------------------------------------------------------------------------
primary_grain_window = Window.partitionBy("Date")
df_flagged = df_inc_silver \
    .withColumn("duplicate_flag", F.when(F.count("*").over(primary_grain_window) > 1, 1).otherwise(0))

metrics = df_flagged.select(
    F.count("*").alias("total_rows"),
    F.sum("duplicate_flag").alias("duplicate_rows"),
    
    # Required Structural Null Checks
    F.sum(F.when(F.col("Date").isNull(), 1).otherwise(0)).alias("null_date"),
    F.sum(F.when(F.col("year").isNull(), 1).otherwise(0)).alias("null_year"),
    F.sum(F.when(F.col("month").isNull(), 1).otherwise(0)).alias("null_month"),
    F.sum(F.when(F.col("quarter").isNull(), 1).otherwise(0)).alias("null_quarter"),
    F.sum(F.when(F.col("dayOfMonth").isNull(), 1).otherwise(0)).alias("null_day_of_month"),
    F.sum(F.when(F.col("DayName").isNull() | (F.trim(F.col("DayName")) == ""), 1).otherwise(0)).alias("null_day_name"),
    F.sum(F.when(F.col("IsWeekend").isNull(), 1).otherwise(0)).alias("null_is_weekend"),
    
    # Logical Bounds
    F.sum(F.when((F.col("month") < 1) | (F.col("month") > 12), 1).otherwise(0)).alias("invalid_months"),
    F.sum(F.when((F.col("quarter") < 1) | (F.col("quarter") > 4), 1).otherwise(0)).alias("invalid_quarters"),
    F.sum(F.when((F.col("dayOfMonth") < 1) | (F.col("dayOfMonth") > 31), 1).otherwise(0)).alias("invalid_days")
).collect()[0]

total_rows = metrics["total_rows"] or 0
duplicate_rows = metrics["duplicate_rows"] or 0

null_date = metrics["null_date"] or 0
null_year = metrics["null_year"] or 0
null_month = metrics["null_month"] or 0
null_quarter = metrics["null_quarter"] or 0
null_day_of_month = metrics["null_day_of_month"] or 0
null_day_name = metrics["null_day_name"] or 0
null_is_weekend = metrics["null_is_weekend"] or 0

invalid_months = metrics["invalid_months"] or 0
invalid_quarters = metrics["invalid_quarters"] or 0
invalid_days = metrics["invalid_days"] or 0

total_critical_failures = (
    null_date + null_year + null_month + null_quarter + 
    null_day_of_month + null_day_name + null_is_weekend +
    invalid_months + invalid_quarters + invalid_days
)

pipeline_passed = "PASS" if (total_critical_failures == 0 and duplicate_rows == 0 and total_rows > 0) else "FAIL"

# -------------------------------------------------------------------------
# 6. REPORT GENERATION WITH WATERMARK SNAPSHOT
# -------------------------------------------------------------------------
eval_time = F.current_timestamp()

validation_rows = [
    (data_source, "Row Count Verification", "PASS" if total_rows > 0 else "FAIL", int(total_rows if total_rows == 0 else 0), current_max_silver_ts),
    (data_source, "Unique Primary Key Grain", "PASS" if duplicate_rows == 0 else "FAIL", int(duplicate_rows), current_max_silver_ts),
    (data_source, "Required Column: Date", "PASS" if null_date == 0 else "FAIL", int(null_date), current_max_silver_ts),
    (data_source, "Required Column: year", "PASS" if null_year == 0 else "FAIL", int(null_year), current_max_silver_ts),
    (data_source, "Required Column: month", "PASS" if null_month == 0 else "FAIL", int(null_month), current_max_silver_ts),
    (data_source, "Required Column: quarter", "PASS" if null_quarter == 0 else "FAIL", int(null_quarter), current_max_silver_ts),
    (data_source, "Required Column: dayOfMonth", "PASS" if null_day_of_month == 0 else "FAIL", int(null_day_of_month), current_max_silver_ts),
    (data_source, "Required Column: DayName", "PASS" if null_day_name == 0 else "FAIL", int(null_day_name), current_max_silver_ts),
    (data_source, "Required Column: IsWeekend", "PASS" if null_is_weekend == 0 else "FAIL", int(null_is_weekend), current_max_silver_ts),
    (data_source, "Logical Bounds: Valid Month (1-12)", "PASS" if invalid_months == 0 else "FAIL", int(invalid_months), current_max_silver_ts),
    (data_source, "Logical Bounds: Valid Quarter (1-4)", "PASS" if invalid_quarters == 0 else "FAIL", int(invalid_quarters), current_max_silver_ts),
    (data_source, "Logical Bounds: Valid Day (1-31)", "PASS" if invalid_days == 0 else "FAIL", int(invalid_days), current_max_silver_ts)
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
validation_df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_quality_table)

# -------------------------------------------------------------------------
# 7. HARD PIPELINE BREAKPOINT GATE
# -------------------------------------------------------------------------
if pipeline_passed == "FAIL":
    raise ValueError(
        f"❌ Data Quality Gate Failed for Silver Table '{data_source}'. "
        f"Critical failures detected: {total_critical_failures}, Duplicates: {duplicate_rows}, Total Rows: {total_rows}. "
        f"Halting downstream Gold ingestion."
    )

print(f"✅ Incremental Quality Gate PASSED for '{data_source}'. Advanced Watermark to: {current_max_silver_ts}")