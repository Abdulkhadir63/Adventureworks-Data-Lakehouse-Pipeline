# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
import logging


logger = logging.getLogger(__name__)

# 1. Initialize Databricks Interactive Widgets
dbutils.widgets.text("catalog", "spark_airflow_adventure_work_project", "Catalog")
dbutils.widgets.text("data_source", "calendar", "Data Source")
dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run ID")

# 2. Extract Widget Values into Runtime Variables
catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
run_id = dbutils.widgets.get("pipeline_run_id")

silver_schema = "silver"
quality_schema = "data_quality_check"
target_quality_table = f"{catalog}.{quality_schema}.{data_source}"

# 3. Read the Target Cleaned Silver Table
logger.info(f"Loading Silver calendar data for final validation check. Run ID: {run_id}")
df_raw = spark.read.table(f"{catalog}.{silver_schema}.{data_source}")

# Pushdown Optimization: Filter for the active batch run metadata timestamp
if run_id.strip():
    df_batch = df_raw.filter(F.col("silver_processed_timestamp").isNotNull())
else:
    logger.warning("No pipeline_run_id provided. Processing entire Silver table snapshot.")
    df_batch = df_raw

# 4. SENIOR PERFORMANCE STRATEGY: Row-Level Duplicate Detection via Window Specs
# Verifies that every record represents exactly one unique calendar date grain entry
primary_grain_window = Window.partitionBy("Date")
df_flagged = df_batch \
    .withColumn("duplicate_flag", F.when(F.count("*").over(primary_grain_window) > 1, 1).otherwise(0))

# 5. Single-Pass Aggregation to Extract Validation Matrix & Required Columns
metrics = df_flagged.select(
    F.count("*").alias("total_rows"),
    F.sum("duplicate_flag").alias("duplicate_rows"),
    
    # Required Structural Column Validations (Null & Blank Checks)
    F.sum(F.when(F.col("Date").isNull(), 1).otherwise(0)).alias("null_date"),
    F.sum(F.when(F.col("year").isNull(), 1).otherwise(0)).alias("null_year"),
    F.sum(F.when(F.col("month").isNull(), 1).otherwise(0)).alias("null_month"),
    F.sum(F.when(F.col("quarter").isNull(), 1).otherwise(0)).alias("null_quarter"),
    F.sum(F.when(F.col("dayOfMonth").isNull(), 1).otherwise(0)).alias("null_day_of_month"),
    F.sum(F.when(F.col("DayName").isNull() | (F.trim(F.col("DayName")) == ""), 1).otherwise(0)).alias("null_day_name"),
    F.sum(F.when(F.col("IsWeekend").isNull(), 1).otherwise(0)).alias("null_is_weekend"),
    
    # Range Bounds & Boundary Logical Anomalies
    F.sum(F.when((F.col("month") < 1) | (F.col("month") > 12), 1).otherwise(0)).alias("invalid_months"),
    F.sum(F.when((F.col("quarter") < 1) | (F.col("quarter") > 4), 1).otherwise(0)).alias("invalid_quarters"),
    F.sum(F.when((F.col("dayOfMonth") < 1) | (F.col("dayOfMonth") > 31), 1).otherwise(0)).alias("invalid_days")
).collect()[0]

# Safe extraction with explicit safeguards
total_rows = metrics["total_rows"] if metrics["total_rows"] is not None else 0
duplicate_rows = metrics["duplicate_rows"] if metrics["duplicate_rows"] is not None else 0

# Required Columns Extracted Variables
null_date = metrics["null_date"] if metrics["null_date"] is not None else 0
null_year = metrics["null_year"] if metrics["null_year"] is not None else 0
null_month = metrics["null_month"] if metrics["null_month"] is not None else 0
null_quarter = metrics["null_quarter"] if metrics["null_quarter"] is not None else 0
null_day_of_month = metrics["null_day_of_month"] if metrics["null_day_of_month"] is not None else 0
null_day_name = metrics["null_day_name"] if metrics["null_day_name"] is not None else 0
null_is_weekend = metrics["null_is_weekend"] if metrics["null_is_weekend"] is not None else 0

# Bounds Anomalies Variables
invalid_months = metrics["invalid_months"] if metrics["invalid_months"] is not None else 0
invalid_quarters = metrics["invalid_quarters"] if metrics["invalid_quarters"] is not None else 0
invalid_days = metrics["invalid_days"] if metrics["invalid_days"] is not None else 0

# Track total critical structural validation errors
total_critical_failures = (
    null_date + null_year + null_month + null_quarter + 
    null_day_of_month + null_day_name + null_is_weekend +
    invalid_months + invalid_quarters + invalid_days
)

# Determine global validation state before Snowflake load sync
pipeline_passed = "PASS" if (total_critical_failures == 0 and duplicate_rows == 0 and total_rows > 0) else "FAIL"

# 6. Build Vertical Data Grid matching Enterprise Output Specifications
validation_rows = [
    (run_id, data_source, "Row Count Verification", "PASS" if total_rows > 0 else "FAIL", 0),
    (run_id, data_source, "Unique Primary Key Grain", "PASS" if duplicate_rows == 0 else "FAIL", int(duplicate_rows)),
    (run_id, data_source, "Required Column: Date", "PASS" if null_date == 0 else "FAIL", int(null_date)),
    (run_id, data_source, "Required Column: year", "PASS" if null_year == 0 else "FAIL", int(null_year)),
    (run_id, data_source, "Required Column: month", "PASS" if null_month == 0 else "FAIL", int(null_month)),
    (run_id, data_source, "Required Column: quarter", "PASS" if null_quarter == 0 else "FAIL", int(null_quarter)),
    (run_id, data_source, "Required Column: dayOfMonth", "PASS" if null_day_of_month == 0 else "FAIL", int(null_day_of_month)),
    (run_id, data_source, "Required Column: DayName", "PASS" if null_day_name == 0 else "FAIL", int(null_day_name)),
    (run_id, data_source, "Required Column: IsWeekend", "PASS" if null_is_weekend == 0 else "FAIL", int(null_is_weekend)),
    (run_id, data_source, "Logical Bounds: Valid Month (1-12)", "PASS" if invalid_months == 0 else "FAIL", int(invalid_months)),
    (run_id, data_source, "Logical Bounds: Valid Quarter (1-4)", "PASS" if invalid_quarters == 0 else "FAIL", int(invalid_quarters)),
    (run_id, data_source, "Logical Bounds: Valid Day (1-31)", "PASS" if invalid_days == 0 else "FAIL", int(invalid_days))
]

# 7. Define Strict Target Structural Schema Definition
validation_schema = StructType([
    StructField("pipeline_run_id", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("rule_name", StringType(), False),
    StructField("status", StringType(), False),
    StructField("failed_rows", IntegerType(), False)
])

# 8. Generate and Display the Final Validation Report Table
validation_df = spark.createDataFrame(validation_rows, schema=validation_schema)

# 9. Overwrite Quality Table to Keep Only the Latest Run Status View
logger.info(f"Overwriting data quality snapshot table: {target_quality_table}")
validation_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(target_quality_table)

# 10. HARD PIPELINE BREAKPOINT FOR DOWNSTREAM SNOWFLAKE SYNC
if pipeline_passed == "FAIL":
    raise ValueError(f"❌ Data Quality Release Failure for Silver Table {data_source} (Run: {run_id}). Critical time-dimension structural column missing, duplicate date, or bound outlier caught. Halting Snowflake staging synchronization loops.")
else:
    print(f"✅ Data Quality Release Gate Passed for Silver Table {data_source}. Safe to begin Snowflake COPY execution.")