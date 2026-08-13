# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window
import logging

# 1. LOGGER CONFIGURATION
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 2. INITIALIZE RUNTIME PARAMETERS VIA WIDGETS
dbutils.widgets.text("catalog", "spark_airflow_adventure_work_project", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")

data_source = dbutils.widgets.get("data_source")
catalog = dbutils.widgets.get("catalog")

bronze_schema = "bronze"
silver_schema = "silver"

# Unity Catalog Table Pointers
source_table = f"{catalog}.{bronze_schema}.{data_source}"
target_table = f"{catalog}.{silver_schema}.{data_source}"

# Decoupled S3 Storage & Checkpoint Locations
silver_load_path = f"s3://airflow-spark-project/silver/{data_source}"
silver_checkpoint = f"s3://airflow-spark-project/checkpoints/silver_checkpoint/bronze_to_silver_{data_source}/"

logger.info(f"Reading Stream from Input Table: {source_table}")
logger.info(f"Tracking with Checkpoint: {silver_checkpoint}")
logger.info(f"Writing physically to: {silver_load_path}")
logger.info(f"Registering logically as Table: {target_table}")

# 3. MODULAR TRANSFORMATION FUNCTION
def transform_customer_silver_tier(df_raw):
    """
    Applies explicit type casting, date standardization, string cleansing, 
    and appends a silver processing timestamp for Customer records.
    """
    return df_raw \
        .withColumn("CustomerKey", F.col("CustomerKey").cast(IntegerType())) \
        .withColumn("AnnualIncome", F.regexp_replace(F.col("AnnualIncome"), r"[\$\s,]", "").cast(DoubleType())) \
        .withColumn(
            "BirthDate", 
            F.coalesce(
                F.expr("try_to_date(BirthDate, 'M/d/yyyy')"),
                F.expr("try_to_date(BirthDate, 'M-d-yyyy')"),
                F.expr("try_to_date(BirthDate, 'yyyy-MM-dd')"),
                F.expr("try_to_date(BirthDate, 'dd/MM/yyyy')")
            )
        ) \
        .withColumn("Prefix", F.coalesce(F.col("Prefix"), F.lit("Unknown"))) \
        .withColumn("FirstName", F.trim(F.upper(F.coalesce(F.col("FirstName"), F.lit("Unknown"))))) \
        .withColumn("LastName", F.trim(F.upper(F.coalesce(F.col("LastName"), F.lit("Unknown"))))) \
        .withColumn("Gender", F.trim(F.upper(F.coalesce(F.col("Gender"), F.lit("Unknown"))))) \
        .withColumn("MaritalStatus", F.trim(F.upper(F.coalesce(F.col("MaritalStatus"), F.lit("Unknown"))))) \
        .withColumn("EducationLevel", F.trim(F.coalesce(F.col("EducationLevel"), F.lit("Unknown")))) \
        .withColumn("Occupation", F.trim(F.coalesce(F.col("Occupation"), F.lit("Unknown")))) \
        .withColumn("TotalChildren", F.coalesce(F.col("TotalChildren").cast(IntegerType()), F.lit(0))) \
        .withColumn("silver_processed_timestamp", F.current_timestamp())

# -------------------------------------------------------------------------
# 4. ONE-TIME TABLE INITIALIZATION (BOOTSTRAP OUTSIDE FOREACHBATCH)
# -------------------------------------------------------------------------
if not spark.catalog.tableExists(target_table):
    logger.info(f"Target Silver table {target_table} does not exist. Initializing empty table schema at {silver_load_path}")
    
    empty_schema_df = spark.read.table(source_table).limit(0)
    empty_transformed_df = transform_customer_silver_tier(empty_schema_df)
    
    target_columns = [
        "CustomerKey", "Prefix", "FirstName", "LastName", "BirthDate", 
        "MaritalStatus", "Gender", "EmailAddress", "AnnualIncome", 
        "TotalChildren", "EducationLevel", "Occupation", "HomeOwner",
        "silver_processed_timestamp"
    ]
    
    empty_transformed_df.select(*target_columns).write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", silver_load_path) \
        .saveAsTable(target_table)

# -------------------------------------------------------------------------
# 5. FOREACHBATCH MICRO-BATCH MERGE FUNCTION (SPARK SQL PATTERN)
# -------------------------------------------------------------------------
def process_customer_micro_batch(df_micro_batch, batch_id):
    if df_micro_batch.isEmpty():
        logger.info(f"Micro-batch {batch_id} is empty. Skipping.")
        return

    logger.info(f"Processing micro-batch ID: {batch_id}")

    # QUALITY GATE
    df_validated_batch = df_micro_batch.filter(
        F.col("CustomerKey").isNotNull()
    )

    # WINDOW DEDUPLICATION ON PRIMARY KEY (CustomerKey)
    window_spec = Window.partitionBy("CustomerKey").orderBy(F.col("ingestion_timestamp").desc())
    df_deduplicated = df_validated_batch \
        .withColumn("row_rank", F.row_number().over(window_spec)) \
        .filter(F.col("row_rank") == 1) \
        .drop("row_rank")

    # TARGET SCHEMA PROJECTIONS
    target_columns = [
        "CustomerKey", "Prefix", "FirstName", "LastName", "BirthDate", 
        "MaritalStatus", "Gender", "EmailAddress", "AnnualIncome", 
        "TotalChildren", "EducationLevel", "Occupation", "HomeOwner",
        "silver_processed_timestamp"
    ]
    df_final_batch = df_deduplicated.select(*target_columns)

    # REGISTER TEMP VIEW FOR SPARK SQL MERGE
    df_final_batch.createOrReplaceTempView("updates_batch")

    # NATIVE SPARK SQL MERGE INTO (COMPATIBLE WITH SPARK CONNECT & SERVERLESS)
    spark.sql(f"""
        MERGE INTO {target_table} AS target
        USING updates_batch AS updates
        ON target.CustomerKey = updates.CustomerKey
        WHEN MATCHED THEN
          UPDATE SET *
        WHEN NOT MATCHED THEN
          INSERT *
    """)

# -------------------------------------------------------------------------
# 6. STREAMING ENGINE EXECUTION
# -------------------------------------------------------------------------
logger.info(f"Starting Delta Stream from {source_table}")

df_bronze_stream = spark.readStream \
    .format("delta") \
    .table(source_table)

df_transformed_stream = transform_customer_silver_tier(df_bronze_stream)

query = df_transformed_stream.writeStream \
    .foreachBatch(process_customer_micro_batch) \
    .option("checkpointLocation", silver_checkpoint) \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()

print(f"🚀 Incremental Streaming Upsert completed successfully for {data_source} into Silver target: {target_table}!")