# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
import logging


logger = logging.getLogger(__name__)

# 1. Initialize Databricks Interactive Widgets
dbutils.widgets.text("catalog", "spark_airflow_adventure_work_project", "Catalog")
dbutils.widgets.text("data_source", "products", "Data Source")
dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run ID")

# 2. Extract Widget Values into Runtime Variables
catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
run_id = dbutils.widgets.get("pipeline_run_id")

silver_schema = "silver"
quality_schema = "data_quality_check"
target_quality_table = f"{catalog}.{quality_schema}.{data_source}"

# 3. Read the Target Cleaned Silver Table
logger.info(f"Loading Silver products data for final validation check. Run ID: {run_id}")
df_raw = spark.read.table(f"{catalog}.{silver_schema}.{data_source}")

# Pushdown Optimization: Filter for the active batch run metadata timestamp
if run_id.strip():
    df_batch = df_raw.filter(F.col("silver_processed_timestamp").isNotNull())
else:
    logger.warning("No pipeline_run_id provided. Processing entire Silver table snapshot.")
    df_batch = df_raw

# 4. SENIOR PERFORMANCE STRATEGY: Row-Level Duplicate Detection via Window Specs
primary_grain_window = Window.partitionBy("ProductKey")
df_flagged = df_batch \
    .withColumn("duplicate_flag", F.when(F.count("*").over(primary_grain_window) > 1, 1).otherwise(0))

# 5. Single-Pass Aggregation to Extract Validation Matrix & Required Columns
metrics = df_flagged.select(
    F.count("*").alias("total_rows"),
    F.sum("duplicate_flag").alias("duplicate_rows"),
    
    # Required Structural Column Validations (Null & Empty Checks)
    F.sum(F.when(F.col("ProductKey").isNull(), 1).otherwise(0)).alias("null_p_key"),
    F.sum(F.when(F.col("ProductSubcategoryKey").isNull(), 1).otherwise(0)).alias("null_sub_key"),
    F.sum(F.when(F.col("ProductSKU").isNull() | (F.trim(F.col("ProductSKU")) == ""), 1).otherwise(0)).alias("null_sku"),
    F.sum(F.when(F.col("ProductName").isNull() | (F.trim(F.col("ProductName")) == ""), 1).otherwise(0)).alias("null_name"),
    
    # Business Logic & Financial Integrity Validations
    F.sum(F.when((F.col("ProductPrice") < 0) | (F.col("ProductCost") < 0), 1).otherwise(0)).alias("negative_financials"),
    F.sum(F.when(F.col("ProductPrice") < F.col("ProductCost"), 1).otherwise(0)).alias("negative_margin_rows")
).collect()[0]

# Safe extraction with explicit safeguards
total_rows = metrics["total_rows"] if metrics["total_rows"] is not None else 0
duplicate_rows = metrics["duplicate_rows"] if metrics["duplicate_rows"] is not None else 0

# Required Columns Extracted Variables
null_p_key = metrics["null_p_key"] if metrics["null_p_key"] is not None else 0
null_sub_key = metrics["null_sub_key"] if metrics["null_sub_key"] is not None else 0
null_sku = metrics["null_sku"] if metrics["null_sku"] is not None else 0
null_name = metrics["null_name"] if metrics["null_name"] is not None else 0

# Financial Columns Extracted Variables
negative_financials = metrics["negative_financials"] if metrics["negative_financials"] is not None else 0
negative_margin_rows = metrics["negative_margin_rows"] if metrics["negative_margin_rows"] is not None else 0

# Calculate structural pipeline status metrics
total_structural_failures = null_p_key + null_sub_key + null_sku + null_name + negative_financials

# Determine global validation state before Snowflake load sync
pipeline_passed = "PASS" if (total_structural_failures == 0 and duplicate_rows == 0 and total_rows > 0) else "FAIL"

# 6. Build Vertical Data Grid matching Enterprise Output Specifications
validation_rows = [
    (run_id, data_source, "Row Count Verification", "PASS" if total_rows > 0 else "FAIL", 0),
    (run_id, data_source, "Unique Primary Key Grain", "PASS" if duplicate_rows == 0 else "FAIL", int(duplicate_rows)),
    (run_id, data_source, "Required Column: ProductKey", "PASS" if null_p_key == 0 else "FAIL", int(null_p_key)),
    (run_id, data_source, "Required Column: ProductSubcategoryKey", "PASS" if null_sub_key == 0 else "FAIL", int(null_sub_key)),
    (run_id, data_source, "Required Column: ProductSKU", "PASS" if null_sku == 0 else "FAIL", int(null_sku)),
    (run_id, data_source, "Required Column: ProductName", "PASS" if null_name == 0 else "FAIL", int(null_name)),
    (run_id, data_source, "Financial Logic: Non-Negative Values", "PASS" if negative_financials == 0 else "FAIL", int(negative_financials)),
    (run_id, data_source, "Business Rule: Price Greater Than Cost", "PASS" if negative_margin_rows == 0 else "WARNING", int(negative_margin_rows))
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
    raise ValueError(f"❌ Data Quality Release Failure for Silver Table {data_source} (Run: {run_id}). Critical column missing, negative value, or structural duplicates caught. Halting Snowflake staging synchronization loops.")
else:
    print(f"✅ Data Quality Release Gate Passed for Silver Table {data_source}. Safe to begin Snowflake COPY execution.")

# COMMAND ----------

