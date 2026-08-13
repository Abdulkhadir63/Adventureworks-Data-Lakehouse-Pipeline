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
dbutils.widgets.text("data_source", "sales", "Data Source")

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

# 5. COMPOSITE GRAIN DUPLICATE DETECTION & SINGLE-PASS AGGREGATION
composite_grain_window = Window.partitionBy("OrderNumber", "OrderLineItem")
df_flagged = df_inc_silver \
    .withColumn("duplicate_flag", F.when(F.count("*").over(composite_grain_window) > 1, 1).otherwise(0))

metrics = df_flagged.select(
    F.count("*").alias("total_rows"),
    F.sum("duplicate_flag").alias("duplicate_rows"),
    
    # Primary Grain Key Validations
    F.sum(F.when(F.col("OrderNumber").isNull(), 1).otherwise(0)).alias("null_order_num"),
    F.sum(F.when(F.col("OrderLineItem").isNull(), 1).otherwise(0)).alias("null_line_item"),
    
    # Foreign Dimension Keys
    F.sum(F.when(F.col("ProductKey").isNull(), 1).otherwise(0)).alias("null_product_key"),
    F.sum(F.when(F.col("CustomerKey").isNull(), 1).otherwise(0)).alias("null_customer_key"),
    F.sum(F.when(F.col("TerritoryKey").isNull(), 1).otherwise(0)).alias("null_territory_key"),
    
    # Datetime Structural Elements
    F.sum(F.when(F.col("OrderDate").isNull(), 1).otherwise(0)).alias("null_order_date"),
    F.sum(F.when(F.col("StockDate").isNull(), 1).otherwise(0)).alias("null_stock_date"),
    
    # Operational Bounds Constraints
    F.sum(F.when(F.col("OrderQuantity") <= 0, 1).otherwise(0)).alias("invalid_qty")
).collect()[0]

# Extract scalar values with safe default fallbacks
total_rows = metrics["total_rows"] or 0
duplicate_rows = metrics["duplicate_rows"] or 0

null_order_num = metrics["null_order_num"] or 0
null_line_item = metrics["null_line_item"] or 0
null_product_key = metrics["null_product_key"] or 0
null_customer_key = metrics["null_customer_key"] or 0
null_territory_key = metrics["null_territory_key"] or 0

null_order_date = metrics["null_order_date"] or 0
null_stock_date = metrics["null_stock_date"] or 0

invalid_qty = metrics["invalid_qty"] or 0

# Track total critical structural validation errors
total_critical_failures = (
    null_order_num + null_line_item + null_product_key + 
    null_customer_key + null_territory_key + null_order_date + 
    null_stock_date + invalid_qty + duplicate_rows
)

pipeline_passed = "PASS" if (total_critical_failures == 0 and total_rows > 0) else "FAIL"

# 6. REPORT GENERATION WITH WATERMARK SNAPSHOT
eval_time = F.current_timestamp()

validation_rows = [
    (data_source, "Row Count Verification", "PASS" if total_rows > 0 else "FAIL", int(total_rows if total_rows == 0 else 0), current_max_silver_ts),
    (data_source, "Unique Primary Key Grain", "PASS" if duplicate_rows == 0 else "FAIL", int(duplicate_rows), current_max_silver_ts),
    (data_source, "Required Column: OrderNumber", "PASS" if null_order_num == 0 else "FAIL", int(null_order_num), current_max_silver_ts),
    (data_source, "Required Column: OrderLineItem", "PASS" if null_line_item == 0 else "FAIL", int(null_line_item), current_max_silver_ts),
    (data_source, "Required Column: ProductKey", "PASS" if null_product_key == 0 else "FAIL", int(null_product_key), current_max_silver_ts),
    (data_source, "Required Column: CustomerKey", "PASS" if null_customer_key == 0 else "FAIL", int(null_customer_key), current_max_silver_ts),
    (data_source, "Required Column: TerritoryKey", "PASS" if null_territory_key == 0 else "FAIL", int(null_territory_key), current_max_silver_ts),
    (data_source, "Required Column: OrderDate", "PASS" if null_order_date == 0 else "FAIL", int(null_order_date), current_max_silver_ts),
    (data_source, "Required Column: StockDate", "PASS" if null_stock_date == 0 else "FAIL", int(null_stock_date), current_max_silver_ts),
    (data_source, "Logical Bounds: OrderQuantity > 0", "PASS" if invalid_qty == 0 else "FAIL", int(invalid_qty), current_max_silver_ts)
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

# 7. AIRFLOW METADATA TASK PUSH & HARD BREAKPOINT GATE
try:
    dbutils.jobs.taskValues.set(key="validation_status", value=pipeline_passed)
except Exception as e:
    logger.warning(f"Task values API not available in local test execution context: {str(e)}")

if pipeline_passed == "FAIL":
    raise ValueError(
        f"Data Quality Gate Failed for Silver Table '{data_source}'. "
        f"Critical failures detected: {total_critical_failures} (Null Order Numbers: {null_order_num}, Null Line Items: {null_line_item}, Null Product Keys: {null_product_key}, Null Customer Keys: {null_customer_key}, Null Territory Keys: {null_territory_key}, Null Order Dates: {null_order_date}, Null Stock Dates: {null_stock_date}, Invalid Quantities: {invalid_qty}, Composite Key Duplicates: {duplicate_rows}), Total Rows: {total_rows}. "
        f"Halting Gold ingestion."
    )

print(f"Data Quality Gate PASSED for '{data_source}'. Advanced Watermark to: {current_max_silver_ts}")