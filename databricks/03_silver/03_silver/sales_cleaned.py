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
dbutils.widgets.text("data_source", "sales", "Data Source")
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


# 2. MODULAR TRANSFORMATION FUNCTION
def transform_sales_silver_tier(df_raw):
    """
    Applies explicit type casting, date standardization, and structural validation
    specifically optimized for the core Sales transaction dataset.
    """
    logger.info("Applying sales table transformation and column type stabilization rules")
    
    # Standardize transaction metrics, calendar keys, and clean dimension foreign keys
    return df_raw \
        .withColumn("OrderNumber", F.trim(F.col("OrderNumber"))) \
        .withColumn("OrderLineItem", F.col("OrderLineItem").cast(IntegerType())) \
        .withColumn("ProductKey", F.col("ProductKey").cast(IntegerType())) \
        .withColumn("CustomerKey", F.col("CustomerKey").cast(IntegerType())) \
        .withColumn("TerritoryKey", F.col("TerritoryKey").cast(IntegerType())) \
        .withColumn("OrderQuantity", F.abs(F.col("OrderQuantity")).cast(IntegerType())) \
        .withColumn("OrderDate", F.coalesce(
            F.expr("try_to_date(OrderDate, 'M/d/yyyy')"),
            F.expr("try_to_date(OrderDate, 'M-d-yyyy')"),
            F.expr("try_to_date(OrderDate, 'yyyy-MM-dd')"),
            F.expr("try_to_date(OrderDate, 'dd/MM/yyyy')")
        )) \
        .withColumn("StockDate", F.coalesce(
            F.expr("try_to_date(StockDate, 'M/d/yyyy')"),
            F.expr("try_to_date(StockDate, 'M-d-yyyy')"),
            F.expr("try_to_date(StockDate, 'yyyy-MM-dd')"),
            F.expr("try_to_date(StockDate, 'dd/MM/yyyy')")
        )) \
        .withColumn("silver_processed_timestamp", F.current_timestamp())\
        .withColumn( "pipeline_run_id",F.lit(current_run_id))


# 3. METADATA-DRIVEN INCREMENTAL READ LAYER
logger.info(f"Scanning Bronze Table for Pipeline Run ID: {current_run_id}")

# if not current_run_id.strip():
#     raise ValueError("pipeline_run_id is required")
if not current_run_id.strip():
    raise ValueError(
        "pipeline_run_id is required"
    )
else:
    logger.info("Senior Strategy: Pushdown optimization filtering on target run ID metadata.")
    df_bronze_batch = spark.read.table(source_table).filter(F.col("pipeline_run_id") == current_run_id)


# 4. DETERMINISTIC DEDUPLICATION & INTEGRITY CHECK
logger.info("Enforcing identity constraints and executing windowed grain deduplication")

# Drop any fully broken transaction lines that lack core identifiers before running windows
df_validated_batch = df_bronze_batch.filter(
    F.col("OrderNumber").isNotNull() & 
    F.col("OrderLineItem").isNotNull()
)

df_transformed = transform_sales_silver_tier(df_validated_batch)

# Partition by composite natural key to handle transaction grain
window_spec = Window.partitionBy("OrderNumber", "OrderLineItem").orderBy(F.col("ingestion_timestamp").desc())
df_deduplicated = df_transformed \
    .withColumn("row_rank", F.row_number().over(window_spec)) \
    .filter(F.col("row_rank") == 1) \
    .drop("row_rank")


# 5. EXECUTE TARGET PROJECTIONS
logger.info("Projecting final clean business columns schema matching target definitions")
target_columns = [
    "OrderDate",
    "StockDate",
    "OrderNumber",
    "ProductKey",
    "CustomerKey",
    "TerritoryKey",
    "OrderLineItem",
    "OrderQuantity",
    "silver_processed_timestamp",
    "pipeline_run_id"
]
df_final_batch = df_deduplicated.select(*target_columns)


# 6. EXTERNAL S3 DELTA MERGE WRITE LAYER
if spark.catalog.tableExists(target_table):
    logger.info(f"Target Silver table {target_table} exists. Merging new run updates into S3.")
    silver_delta_table = DeltaTable.forName(spark, target_table)
    
    silver_delta_table.alias("target") \
        .merge(
            source = df_final_batch.alias("updates"),
            condition = """
                target.OrderNumber = updates.OrderNumber 
                AND target.OrderLineItem = updates.OrderLineItem
            """
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

