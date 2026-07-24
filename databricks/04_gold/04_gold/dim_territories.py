# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# 1. Initialize Runtime Parameters via Airflow/Manual Widgets
dbutils.widgets.text("catalog", "spark_airflow_adventure_work_project", "Catalog")
dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run ID")

catalog = dbutils.widgets.get("catalog")
current_run_id = dbutils.widgets.get("pipeline_run_id")

# Medallion Schema Layout Configuration
silver_schema = "silver"
gold_schema = "gold"

# Target Dimension Table Identity
target_table_name = "dim_territories"
source_table_name = "territories"

source_table = f"{catalog}.{silver_schema}.{source_table_name}"
target_table = f"{catalog}.{gold_schema}.{target_table_name}"

# Decoupled Target S3 Gold Storage Location
gold_load_path = f"s3://airflow-spark-project/gold/{target_table_name}"

# 2. ADAPTIVE READ LAYER (WITH SNAPSHOT FALLBACK)
if not current_run_id or current_run_id.strip() in ("", "None", "null"):
    print("⚠️ No valid pipeline_run_id provided or cleared. Snapshot fallback activated: Scanning full table.")
    df_silver_batch = spark.read.table(source_table)
else:
    print(f"📊 Pipeline Run ID detected: {current_run_id}. Processing incremental dimension batch updates.")
    df_silver_batch = spark.read.table(source_table).filter(F.col("pipeline_run_id") == current_run_id)

# 3. SELECT SCHEMA BUSINESS FIELDS & EXECUTE KEY STANDARDIZATION
df_gold_batch = df_silver_batch.select(
    # Key Name Alignment Strategy: Changing SalesTerritoryKey to unified TerritoryKey
    F.col("SalesTerritoryKey").cast("int").alias("TerritoryKey"),
    F.col("Region").alias("Region"),
    F.col("Country").alias("Country"),
    F.col("Continent").alias("Continent")
)

# 4. EXTERNAL S3 DELTA MERGE WRITE LAYER
if spark.catalog.tableExists(target_table):
    # Execute an incremental upsert (Merge) based on the unified key
    gold_delta_table = DeltaTable.forName(spark, target_table)
    
    gold_delta_table.alias("target") \
        .merge(
            source = df_gold_batch.alias("updates"),
            condition = "target.TerritoryKey = updates.TerritoryKey"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
else:
    # Initial run baseline setup pointing cleanly to the S3 Gold storage location
    df_gold_batch.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", gold_load_path) \
        .saveAsTable(target_table)

print(f"🚀 Dimension table dim_territories successfully processed into S3 with unified keys!")