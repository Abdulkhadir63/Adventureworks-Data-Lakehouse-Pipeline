# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
import logging


logger = logging.getLogger(__name__)

# 1. Initialize Databricks Interactive Widgets
dbutils.widgets.text("catalog", "spark_airflow_adventure_work_project", "Catalog")
dbutils.widgets.text("data_source", "product_sub_category", "Data Source")
dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run ID")

# 2. Extract Widget Values into Runtime Variables
catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
run_id = dbutils.widgets.get("pipeline_run_id")

silver_schema = "silver"
quality_schema = "data_quality_check"
target_quality_table = f"{catalog}.{quality_schema}.{data_source}"

# 3. Read the Target Cleaned Silver Table
logger.info(f"Loading Silver data for final validation check. Run ID: {run_id}")
df_raw = spark.read.table(f"{catalog}.{silver_schema}.{data_source}")

# Pushdown Optimization: Filter for the active batch run metadata timestamp
if run_id.strip():
    df_batch = df_raw.filter(F.col("silver_processed_timestamp").isNotNull())
else:
    logger.warning("No pipeline_run_id provided. Processing entire Silver table snapshot.")
    df_batch = df_raw

# 4. SENIOR PERFORMANCE STRATEGY: Row-Level Duplicate Detection via Window Specs
primary_grain_window = Window.partitionBy("ProductSubcategoryKey")
df_flagged = df_batch \
    .withColumn("duplicate_flag", F.when(F.count("*").over(primary_grain_window) > 1, 1).otherwise(0))

# 5. Single-Pass Aggregation to Extract Structural Values & Validate Required Columns
metrics = df_flagged.select(
    F.count("*").alias("total_rows"),
    F.sum("duplicate_flag").alias("duplicate_rows"),
    F.sum(F.when(F.col("ProductSubcategoryKey").isNull(), 1).otherwise(0)).alias("null_ps_key"),
    F.sum(F.when(F.col("ProductCategoryKey").isNull(), 1).otherwise(0)).alias("null_pc_key"),
    F.sum(F.when(F.col("SubcategoryName").isNull() | (F.trim(F.col("SubcategoryName")) == ""), 1).otherwise(0)).alias("null_sub_name")
).collect()[0]

# Safe extraction with explicit safeguards
total_rows = metrics["total_rows"] if metrics["total_rows"] is not None else 0
duplicate_rows = metrics["duplicate_rows"] if metrics["duplicate_rows"] is not None else 0
null_ps_key = metrics["null_ps_key"] if metrics["null_ps_key"] is not None else 0
null_pc_key = metrics["null_pc_key"] if metrics["null_pc_key"] is not None else 0
null_sub_name = metrics["null_sub_name"] if metrics["null_sub_name"] is not None else 0

# Track total bad rows across all structural schemas
total_required_failures = null_ps_key + null_pc_key + null_sub_name

# Absolute data integrity constraints to pass the Snowflake release gate
pipeline_passed = "PASS" if (total_required_failures == 0 and duplicate_rows == 0 and total_rows > 0) else "FAIL"

# 6. Build Vertical Data Grid matching Enterprise Output Specifications
# We break out every required column directly so the failing field is immediately logged!
validation_rows = [
    (run_id, data_source, "Row Count Verification", "PASS" if total_rows > 0 else "FAIL", 0),
    (run_id, data_source, "Unique Primary Key Grain", "PASS" if duplicate_rows == 0 else "FAIL", int(duplicate_rows)),
    (run_id, data_source, "Required Column: ProductSubcategoryKey", "PASS" if null_ps_key == 0 else "FAIL", int(null_ps_key)),
    (run_id, data_source, "Required Column: ProductCategoryKey", "PASS" if null_pc_key == 0 else "FAIL", int(null_pc_key)),
    (run_id, data_source, "Required Column: SubcategoryName", "PASS" if null_sub_name == 0 else "FAIL", int(null_sub_name))
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
    raise ValueError(f"❌ Data Quality Release Failure for Silver Table {data_source} (Run: {run_id}). Broken required columns or grain duplicates caught. Halting Snowflake staging synchronization loops.")
else:
    print(f"✅ Data Quality Release Gate Passed for Silver Table {data_source}. Safe to begin Snowflake COPY execution.")

# COMMAND ----------

