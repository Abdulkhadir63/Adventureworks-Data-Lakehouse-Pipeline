# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DateType, IntegerType,DoubleType
import uuid
import logging
logger = logging.getLogger(__name__)
dbutils.widgets.text("catalog", "spark_airflow_adventure_work_project", "Catalog")
dbutils.widgets.text("data_source", "returns", "Data Source")

bronze_schema = "bronze"

data_source = dbutils.widgets.get("data_source")
catalog = dbutils.widgets.get("catalog")

# 2. FIXED: Dynamically Routed & Decoupled Architecture
base_path         = f's3://airflow-spark-project/incoming/{data_source}/' # Removed the *.csv glob
bronze_checkpoint = f's3://airflow-spark-project/checkpoints/bronze_checkpoint/raw_to_bronze_{data_source}/'
load_path         = f's3://airflow-spark-project/bronze/{data_source}'
target_table      = f'{catalog}.{bronze_schema}.{data_source}'

logger.info(f"Reading from Input Path: {base_path}")
logger.info(f"Tracking with Checkpoint: {bronze_checkpoint}")
logger.info(f"Writing physically to: {load_path}")
logger.info(f"Registering logically as Table: {target_table}")

 

logger.info(f"Defining Schema for {data_source} Data")
schema = StructType([
    # Safe Ingestion: Read raw dates as String to prevent silent NULL conversions
    StructField("ReturnDate", StringType(), True),  
    StructField("TerritoryKey", IntegerType(), True),
    StructField("ProductKey", IntegerType(), True),
    StructField("ReturnQuantity", IntegerType(), True),
])


dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run ID")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")

logger.info(f"Reading {data_source} Data and Setting Defensive Guard-Dog Stream Read")
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("header", "true") \
    .schema(schema) \
    .option("cloudFiles.schemaEvolutionMode", "failOnNewColumns") \
    .load(base_path) \
    .withColumn( "pipeline_run_id",F.lit(pipeline_run_id)) \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .withColumn("source_file", F.col("_metadata.file_path")) \
    .withColumn("ingestion_date", F.current_date())



logger.info(f"Writing {data_source} Data to Bronze Table {target_table}")
query = df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", bronze_checkpoint) \
    .option("path", load_path) \
    .trigger(availableNow=True) \
    .toTable(target_table)

query.awaitTermination()
