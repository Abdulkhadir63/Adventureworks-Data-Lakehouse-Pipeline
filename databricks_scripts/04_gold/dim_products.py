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
dbutils.widgets.text("data_source", "products", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

silver_schema = "silver"
gold_schema = "gold"

target_table_name = f"dim_{data_source}"
target_table = f"{catalog}.{gold_schema}.{target_table_name}"
gold_load_path = f"s3://airflow-spark-project/gold/{target_table_name}"

# 3. HIGH-WATER MARK RETRIEVAL
def get_gold_high_water_mark(table_name):
    """
    Retrieves the maximum silver_processed_timestamp loaded into the Gold dimension in prior runs.
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

# 4. INCREMENTAL READ FROM SILVER DRIVER TABLE
logger.info(f"Scanning Silver products table where silver_processed_timestamp > '{last_processed_timestamp}'")

df_p = spark.read.table(f"{catalog}.{silver_schema}.{data_source}") \
    .filter(F.col("silver_processed_timestamp") > F.lit(last_processed_timestamp))

# Short-circuit if no new product records exist in Silver
if df_p.isEmpty():
    logger.info("No new Silver product records to process. Exiting Gold ingestion successfully.")
    dbutils.notebook.exit("SUCCESS_NO_NEW_DATA")

# Capture maximum watermark timestamp for active micro-batch
current_max_silver_ts = df_p.agg(F.max("silver_processed_timestamp")).collect()[0][0]

# Read dimension lookup tables
df_psc = spark.read.table(f"{catalog}.{silver_schema}.product_sub_category")
df_pc = spark.read.table(f"{catalog}.{silver_schema}.product_category")

# 5. EXECUTE STRUCTURAL LEFT JOINS
df_joined = df_p.alias("p") \
    .join(
        df_psc.alias("psc"),
        F.col("p.ProductSubcategoryKey") == F.col("psc.ProductSubcategoryKey"),
        "left"
    ) \
    .join(
        df_pc.alias("pc"),
        F.col("psc.ProductCategoryKey") == F.col("pc.ProductCategoryKey"),
        "left"
    )

# 6. BUSINESS TRANSFORMATIONS, CASTING, & METADATA INJECTION
df_gold_batch = df_joined.select(
    F.col("p.ProductKey").alias("ProductKey"),
    F.col("p.ProductName").alias("ProductName"),
    F.col("p.ProductSKU").alias("ProductSKU"),
    F.col("p.ModelName").alias("ModelName"),
    F.col("p.ProductDescription").alias("ProductDescription"),
    F.col("p.ProductColor").alias("ProductColor"),
    F.col("p.ProductSize").alias("ProductSize"),
    F.col("p.ProductStyle").alias("ProductStyle"),
    
    F.col("pc.ProductCategoryKey").alias("ProductCategoryKey"),
    F.col("pc.CategoryName").alias("CategoryName"),
    
    F.col("psc.ProductSubcategoryKey").alias("ProductSubcategoryKey"),
    F.col("psc.SubcategoryName").alias("SubcategoryName"),
    
    F.col("p.ProductCost").cast("decimal(10,4)").alias("ProductCost"),
    F.col("p.ProductPrice").cast("decimal(10,4)").alias("ProductPrice")
) \
.withColumn("gold_processed_timestamp", F.current_timestamp()) \
.withColumn("max_silver_processed_timestamp", F.lit(current_max_silver_ts))

# 7. DELTA UPSERT (MERGE) LAYER
logger.info(f"Writing transformed batch to Gold Delta table: {target_table}")

if spark.catalog.tableExists(target_table):
    gold_delta_table = DeltaTable.forName(spark, target_table)
    
    gold_delta_table.alias("target") \
        .merge(
            source = df_gold_batch.alias("updates"),
            condition = "target.ProductKey = updates.ProductKey"
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

print(f"Master Product Dimension table process completed successfully for '{target_table}'. Advanced Watermark to: {current_max_silver_ts}")

# COMMAND ----------

# MAGIC %md
# MAGIC