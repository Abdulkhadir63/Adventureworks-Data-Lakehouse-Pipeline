# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import logging

# 1. LOGGER CONFIGURATION
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 2. FETCH DYNAMIC PARAMETERS VIA WIDGETS
dbutils.widgets.text("catalog", "spark_airflow_adventure_work_project", "Catalog")
dbutils.widgets.text("data_source", "calendar", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

bronze_schema = "bronze"

# 3. DYNAMICALLY ROUTED & DECOUPLED ARCHITECTURE
base_path         = f"s3://airflow-spark-project/incoming/{data_source}/"
bronze_checkpoint = f"s3://airflow-spark-project/checkpoints/bronze_checkpoint/raw_to_bronze_{data_source}/"
load_path         = f"s3://airflow-spark-project/bronze/{data_source}"
target_table      = f"{catalog}.{bronze_schema}.{data_source}"

logger.info(f"Reading from Input Path: {base_path}")
logger.info(f"Tracking with Checkpoint: {bronze_checkpoint}")
logger.info(f"Writing physically to: {load_path}")
logger.info(f"Registering logically as Table: {target_table}")

# 4. EXPLICIT INGESTION DATA CONTRACT
schema = StructType([
    StructField("Date", StringType(), True)
])

# 5. AUTO LOADER STREAM READ WITH METADATA INJECTION
logger.info(f"Reading {data_source} Data and Setting Defensive Guard-Dog Stream Read")

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(schema)
    .option("cloudFiles.schemaEvolutionMode", "failOnNewColumns")
    .load(base_path)
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.col("_metadata.file_path"))
    .withColumn("ingestion_date", F.current_date())
)

# 6. STREAM WRITE TO BRONZE DELTA TABLE
logger.info(f"Writing {data_source} Data to Bronze Table {target_table}")

query = (
    df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", bronze_checkpoint)
    .option("path", load_path)
    .trigger(availableNow=True)
    .toTable(target_table)
)

query.awaitTermination()

print(f"Raw to Bronze Auto Loader ingestion completed successfully for {data_source}.")