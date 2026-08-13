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
dbutils.widgets.text("data_source", "sales", "Data Source")

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

# 4. INCREMENTAL READ FROM SILVER SALES DRIVER TABLE
logger.info(f"Scanning Silver source '{source_table}' where silver_processed_timestamp > '{last_processed_timestamp}'")

df_sales_batch = spark.read.table(source_table) \
    .filter(F.col("silver_processed_timestamp") > F.lit(last_processed_timestamp))

# Short-circuit if no new sales records exist in Silver
if df_sales_batch.isEmpty():
    logger.info("No new Silver sales records to process. Exiting Gold ingestion successfully.")
    dbutils.notebook.exit("SUCCESS_NO_NEW_DATA")

# Capture maximum watermark timestamp for active micro-batch
current_max_silver_ts = df_sales_batch.agg(F.max("silver_processed_timestamp")).collect()[0][0]

# Read full products dimension table for cost and price resolution
df_dim_prod = spark.read.table(f"{catalog}.{silver_schema}.products")

# 5. JOIN SALES TO PRODUCT DIMENSION
df_joined = df_sales_batch.alias("s") \
    .join(
        df_dim_prod.alias("p"),
        F.col("s.ProductKey") == F.col("p.ProductKey"),
        "left"
    )

# 6. BUSINESS TRANSFORMATIONS, FINANCIAL CALCULATIONS, & METADATA INJECTION
df_gold_batch = df_joined.select(
    F.col("s.OrderDate").cast("date").alias("OrderDate"),
    F.col("s.StockDate").cast("date").alias("StockDate"),
    F.col("s.OrderNumber").alias("OrderNumber"),
    F.col("s.ProductKey").cast("int").alias("ProductKey"),
    F.col("s.CustomerKey").cast("int").alias("CustomerKey"),
    F.col("s.TerritoryKey").cast("int").alias("TerritoryKey"),
    F.col("s.OrderLineItem").cast("int").alias("OrderLineItem"),
    F.col("s.OrderQuantity").cast("int").alias("OrderQuantity"),
    
    # Unit prices and costs
    F.col("p.ProductPrice").cast("decimal(18,4)").alias("ProductPrice"),
    F.col("p.ProductCost").cast("decimal(18,4)").alias("ProductCost"),
    
    # Senior Calculation Layer: Revenue, Cost, Profit
    (F.col("s.OrderQuantity") * F.col("p.ProductPrice")).cast("decimal(18,4)").alias("Revenue"),
    (F.col("s.OrderQuantity") * F.col("p.ProductCost")).cast("decimal(18,4)").alias("Cost"),
    ((F.col("s.OrderQuantity") * F.col("p.ProductPrice")) - 
     (F.col("s.OrderQuantity") * F.col("p.ProductCost"))).cast("decimal(18,4)").alias("Profit")
) \
.withColumn("gold_processed_timestamp", F.current_timestamp()) \
.withColumn("max_silver_processed_timestamp", F.lit(current_max_silver_ts))

# 7. DELTA UPSERT (MERGE) LAYER
logger.info(f"Writing transformed batch to Gold Delta table: {target_table}")

merge_condition = """
    target.OrderNumber = updates.OrderNumber AND 
    target.OrderLineItem = updates.OrderLineItem AND 
    target.ProductKey = updates.ProductKey
"""

if spark.catalog.tableExists(target_table):
    gold_delta_table = DeltaTable.forName(spark, target_table)
    
    gold_delta_table.alias("target") \
        .merge(
            source = df_gold_batch.alias("updates"),
            condition = merge_condition
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

print(f"Transactional Sales Fact table process completed successfully for '{target_table}'. Advanced Watermark to: {current_max_silver_ts}")