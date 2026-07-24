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
dbutils.widgets.text("data_source", "product_category", "Data Source")
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


# 2. MODULAR TRANSFORMATION FUNCTION (Fixed for Products schema)
def transform_product_category_silver_tier(df_raw):
    """
    Applies explicit type casting, standards mapping, and string trimming 
    specifically optimized for the Products data pipeline.
    """
    logger.info("Applying product table transformation rules")
    return df_raw \
        .withColumn("CategoryName", F.trim(F.initcap(F.coalesce(F.col("CategoryName"), F.lit("Unknown"))))) \
        .withColumn("silver_processed_timestamp", F.current_timestamp())\
        .withColumn("pipeline_run_id", F.lit(current_run_id)) 
    

# 3. METADATA-DRIVEN INCREMENTAL READ LAYER
logger.info(f"Scanning Bronze Table for Pipeline Run ID: {current_run_id}")


if not current_run_id.strip():
    raise ValueError(
        "pipeline_run_id is required"
    )
else:
    logger.info("Senior Strategy: Pushdown optimization filtering on target run ID metadata.")
    df_bronze_batch = spark.read.table(source_table).filter(F.col("pipeline_run_id") == current_run_id)


# 4. DETERMINISTIC DEDUPLICATION
logger.info("Executing windowed deduplication based on latest ingestion timestamp")
window_spec = Window.partitionBy("ProductCategoryKey").orderBy(F.col("ingestion_timestamp").desc())
df_deduplicated = df_bronze_batch \
    .withColumn("row_rank", F.row_number().over(window_spec)) \
    .filter(F.col("row_rank") == 1) \
    .drop("row_rank")


# 5. EXECUTE TRANSFORMATIONS & PROJECTIONS
logger.info("Running product-specific schema transformations")
df_transformed = transform_product_category_silver_tier(df_deduplicated)

# Columns perfectly matched with their transformed variable casing definitions
target_columns = [
    "ProductCategoryKey",
    "CategoryName",
    "silver_processed_timestamp",
    "pipeline_run_id"
]
df_final_batch = df_transformed.select(*target_columns)


# 6. EXTERNAL S3 DELTA MERGE WRITE LAYER
if spark.catalog.tableExists(target_table):
    logger.info(f"Target Silver table {target_table} exists. Merging new run updates into S3.")
    silver_delta_table = DeltaTable.forName(spark, target_table)
    
    silver_delta_table.alias("target") \
        .merge(
            source = df_final_batch.alias("updates"),
            condition = "target.ProductCategoryKey = updates.ProductCategoryKey"
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

 