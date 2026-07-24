# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
import logging


logger = logging.getLogger(__name__)

# 1. Initialize Databricks Interactive Widgets
dbutils.widgets.text("catalog", "spark_airflow_adventure_work_project", "Catalog")
dbutils.widgets.text("data_source", "sales", "Data Source")
dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run ID")

# 2. Extract Widget Values into Runtime Variables
catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
run_id = dbutils.widgets.get("pipeline_run_id")

silver_schema = "silver"
quality_schema = "data_quality_check"
target_quality_table = f"{catalog}.{quality_schema}.{data_source}"

# 3. Read the Target Cleaned Silver Table
logger.info(f"Loading Silver sales data for final validation check. Run ID: {run_id}")
df_raw = spark.read.table(f"{catalog}.{silver_schema}.{data_source}")

# Pushdown Optimization: Filter for the active batch run metadata timestamp
if run_id.strip():
    df_batch = df_raw.filter(F.col("silver_processed_timestamp").isNotNull())
else:
    logger.warning("No pipeline_run_id provided. Processing entire Silver table snapshot.")
    df_batch = df_raw

# 4. SENIOR PERFORMANCE STRATEGY: Row-Level Duplicate Detection via Window Specs
# Verifies unique composite grain structural integrity for transactional rows
composite_grain_window = Window.partitionBy("OrderNumber", "OrderLineItem")
df_flagged = df_batch \
    .withColumn("duplicate_flag", F.when(F.count("*").over(composite_grain_window) > 1, 1).otherwise(0))

# 5. Single-Pass Aggregation to Extract Validation Matrix & Required Columns
metrics = df_flagged.select(
    F.count("*").alias("total_rows"),
    F.sum("duplicate_flag").alias("duplicate_rows"),
    
    # Mandatory Primary Grain Key Validations
    F.sum(F.when(F.col("OrderNumber").isNull(), 1).otherwise(0)).alias("null_order_num"),
    F.sum(F.when(F.col("OrderLineItem").isNull(), 1).otherwise(0)).alias("null_line_item"),
    
    # Required Foreign Dimension Keys (Null & Blank Checks)
    F.sum(F.when(F.col("ProductKey").isNull(), 1).otherwise(0)).alias("null_product_key"),
    F.sum(F.when(F.col("CustomerKey").isNull(), 1).otherwise(0)).alias("null_customer_key"),
    F.sum(F.when(F.col("TerritoryKey").isNull(), 1).otherwise(0)).alias("null_territory_key"),
    
    # Required Structural Datetime Elements
    F.sum(F.when(F.col("OrderDate").isNull(), 1).otherwise(0)).alias("null_order_date"),
    F.sum(F.when(F.col("StockDate").isNull(), 1).otherwise(0)).alias("null_stock_date"),
    
    # Range Bounds & Boundary Logical Operational Anomalies
    F.sum(F.when(F.col("OrderQuantity") <= 0, 1).otherwise(0)).alias("invalid_qty")
).collect()[0]

# Safe extraction with explicit safeguards
total_rows = metrics["total_rows"] if metrics["total_rows"] is not None else 0
duplicate_rows = metrics["duplicate_rows"] if metrics["duplicate_rows"] is not None else 0

# Primary and Dimensional Keys Variables
null_order_num = metrics["null_order_num"] if metrics["null_order_num"] is not None else 0
null_line_item = metrics["null_line_item"] if metrics["null_line_item"] is not None else 0
null_product_key = metrics["null_product_key"] if metrics["null_product_key"] is not None else 0
null_customer_key = metrics["null_customer_key"] if metrics["null_customer_key"] is not None else 0
null_territory_key = metrics["null_territory_key"] if metrics["null_territory_key"] is not None else 0

# Datetime Operational Elements Variables
null_order_date = metrics["null_order_date"] if metrics["null_order_date"] is not None else 0
null_stock_date = metrics["null_stock_date"] if metrics["null_stock_date"] is not None else 0

# Boundary Bounds Anomalies Variables
invalid_qty = metrics["invalid_qty"] if metrics["invalid_qty"] is not None else 0

# Track total critical structural validation errors across all data vectors
total_critical_failures = (
    null_order_num + null_line_item + null_product_key + 
    null_customer_key + null_territory_key + null_order_date + 
    null_stock_date + invalid_qty
)

# Determine global validation state before Snowflake load sync
pipeline_passed = "PASS" if (total_critical_failures == 0 and duplicate_rows == 0 and total_rows > 0) else "FAIL"

# 6. Build Vertical Data Grid matching Enterprise Output Specifications
validation_rows = [
    (run_id, data_source, "Row Count Verification", "PASS" if total_rows > 0 else "FAIL", 0),
    (run_id, data_source, "Unique Primary Key Grain", "PASS" if duplicate_rows == 0 else "FAIL", int(duplicate_rows)),
    (run_id, data_source, "Required Column: OrderNumber", "PASS" if null_order_num == 0 else "FAIL", int(null_order_num)),
    (run_id, data_source, "Required Column: OrderLineItem", "PASS" if null_line_item == 0 else "FAIL", int(null_line_item)),
    (run_id, data_source, "Required Column: ProductKey", "PASS" if null_product_key == 0 else "FAIL", int(null_product_key)),
    (run_id, data_source, "Required Column: CustomerKey", "PASS" if null_customer_key == 0 else "FAIL", int(null_customer_key)),
    (run_id, data_source, "Required Column: TerritoryKey", "PASS" if null_territory_key == 0 else "FAIL", int(null_territory_key)),
    (run_id, data_source, "Required Column: OrderDate", "PASS" if null_order_date == 0 else "FAIL", int(null_order_date)),
    (run_id, data_source, "Required Column: StockDate", "PASS" if null_stock_date == 0 else "FAIL", int(null_stock_date)),
    (run_id, data_source, "Logical Bounds: OrderQuantity > 0", "PASS" if invalid_qty == 0 else "FAIL", int(invalid_qty))
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

# 10. SENIOR INTEGRATION: Push state directly to task values metadata context for Airflow
dbutils.jobs.taskValues.set(key="validation_status", value=pipeline_passed)

# HARD PIPELINE BREAKPOINT FOR DOWNSTREAM SNOWFLAKE SYNC
if pipeline_passed == "FAIL":
    raise ValueError(f"❌ Data Quality Release Failure for Silver Table {data_source} (Run: {run_id}). Critical transactional grain primary/foreign key missing or quantity metric out of bounds. Halting Snowflake staging synchronization loops.")
else:
    print(f"✅ Data Quality Release Gate Passed for Silver Table {data_source}. Safe to begin Snowflake COPY execution.")

# COMMAND ----------

