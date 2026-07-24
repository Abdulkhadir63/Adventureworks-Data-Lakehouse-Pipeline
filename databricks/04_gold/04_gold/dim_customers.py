# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# 1. Initialize Runtime Parameters via Airflow/Manual Widgets
dbutils.widgets.text("catalog", "spark_airflow_adventure_work_project", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")
dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run ID")

data_source = dbutils.widgets.get("data_source")
catalog = dbutils.widgets.get("catalog")
current_run_id = dbutils.widgets.get("pipeline_run_id")

# Medallion Schema Layout Configuration
silver_schema = "silver"
gold_schema = "gold"

# Unity Catalog Table Pointers
source_table = f"{catalog}.{silver_schema}.{data_source}"
target_table = f"{catalog}.{gold_schema}.{data_source}"

# Decoupled Target S3 Gold Storage Location
gold_load_path = f"s3://airflow-spark-project/gold/dim_{data_source}"

# 2. ADAPTIVE READ LAYER (INCREMENTAL VS FULL SNAPSHOT)
if not current_run_id or current_run_id.strip() in ("", "None", "null"):
    print("⚠️ No valid pipeline_run_id provided or cleared. Snapshot fallback activated: Scanning full table.")
    df_silver_batch = spark.read.table(source_table)
else:
    print(f"📊 Pipeline Run ID detected: {current_run_id}. Applying pushdown optimization filter.")
    df_silver_batch = spark.read.table(source_table).filter(F.col("pipeline_run_id") == current_run_id)

# 3. SELECT REQUIRED COLUMNS AND INJECT NEW_DATE METADATA
df_gold_batch = df_silver_batch.select(
    F.col("customerKey").alias("CustomerKey"),
    F.col("prefix").alias("Prefix"),
    F.concat_ws(" ", F.col("firstName"), F.col("lastName")).alias("FullName"),
    F.col("birthDate").alias("BirthDate"),
    F.col("maritalStatus").alias("MaritalStatus"),
    F.col("gender").alias("Gender"),
    F.col("emailAddress").alias("EmailAddress"),
    F.col("annualIncome").alias("AnnualIncome"),
    F.col("totalChildren").alias("TotalChildren"),
    F.col("educationLevel").alias("EducationLevel"),
    F.col("occupation").alias("Occupation"),
    F.col("HomeOwner")
)

# 4. EXTERNAL S3 DELTA UPSERT (MERGE) LAYER
if spark.catalog.tableExists(target_table):
    # If Gold target table exists, run incremental upsert (Merge)
    gold_delta_table = DeltaTable.forName(spark, target_table)
    
    gold_delta_table.alias("target") \
        .merge(
            source = df_gold_batch.alias("updates"),
            condition = "target.CustomerKey = updates.CustomerKey"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
else:
    # Initial run baseline setup pointing to S3 path
    df_gold_batch.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", gold_load_path) \
        .saveAsTable(target_table)

print(f"🚀 Gold Master Table process completed successfully.")

# COMMAND ----------



# COMMAND ----------

