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
dbutils.widgets.text("data_source", "customers", "Data Source")
# SENIOR INTEGRATION: Airflow passes the unique UUID string of the current batch run
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
def transform_customer_silver_tier(df_raw):
    """
    Applies explicit type casting, standards mapping, and string trimming.
    """
    logger.info("Applying customer transformation rules")
    return df_raw \
        .withColumn("AnnualIncome",
            F.regexp_replace(F.col("AnnualIncome"), r"[\$\s,]", "").cast(DoubleType())
        ) \
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
        .withColumn("silver_processed_timestamp", F.current_timestamp())\
        .withColumn( "pipeline_run_id",F.lit(current_run_id))


# 3. METADATA-DRIVEN INCREMENTAL READ LAYER
logger.info(f"Scanning Bronze Table for Pipeline Run ID: {current_run_id}")

# If Airflow doesn't pass a run ID (e.g. manual execution), process all data as a fallback
# ''''''    # if current_run_id.strip() == "":
                #     logger.warning("No pipeline_run_id provided. Processing entire table snapshot.")
                #     df_bronze_batch = spark.read.table(source_table)      ''''''

if not current_run_id.strip():
    raise ValueError(
        "pipeline_run_id is required"
    )
else:
    logger.info("Senior Strategy: Pushdown optimization. Spark scans only file paths matching the filtered run ID")
    df_bronze_batch = spark.read.table(source_table).filter(F.col("pipeline_run_id") == current_run_id)


logger.info(""" DETERMINISTIC DEDUPLICATION
# Even inside a single incoming CSV file, duplicate rows can exist. 
# We sort by ingestion_timestamp desc to always pick the newest record if duplicates occur.""")

window_spec = Window.partitionBy("CustomerKey").orderBy(F.col("ingestion_timestamp").desc())
df_deduplicated = df_bronze_batch \
    .withColumn("row_rank", F.row_number().over(window_spec)) \
    .filter(F.col("row_rank") == 1) \
    .drop("row_rank")


logger.info("EXECUTE TRANSFORMATIONS & PROJECTIONS")
df_transformed = transform_customer_silver_tier(df_deduplicated)

logger.info("Select only clean business columns (removing temporary file processing metadata columns")
target_columns = [
    "CustomerKey", "Prefix", "FirstName", "LastName", "BirthDate", 
    "MaritalStatus", "Gender", "EmailAddress", "AnnualIncome", 
    "TotalChildren", "EducationLevel", "Occupation", "HomeOwner",
    "silver_processed_timestamp", "pipeline_run_id"
]
df_final_batch = df_transformed.select(*target_columns)


logger.info(" EXTERNAL S3 DELTA MERGE WRITE LAYER ")
if spark.catalog.tableExists(target_table):

        logger.info(f"Target Silver table {target_table} exists. Merging new run updates into S3.")
        silver_delta_table = DeltaTable.forName(spark, target_table)
        
        silver_delta_table.alias("target") \
            .merge(
                source = df_final_batch.alias("updates"),
                condition = "target.CustomerKey = updates.CustomerKey"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
else:
        logger.info(f"Target Silver table does not exist. Initializing External Table at S3 path: {silver_load_path}")
        source_table = f"{catalog}.{bronze_schema}.{data_source}"
        silver_load_path = f"s3://airflow-spark-project/silver/{data_source}"
        df_final_batch.write \
            .format("delta") \
            .mode("append") \
            .option("path", silver_load_path) \
            .saveAsTable(target_table)

print(f"🚀 Metadata-Driven Incremental Batch Upsert completed successfully for Run ID: {current_run_id}")