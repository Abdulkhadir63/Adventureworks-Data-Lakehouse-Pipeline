# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging


logger = logging.getLogger(__name__)

# 1. Initialize Databricks Interactive Widgets for Metadata
dbutils.widgets.text("catalog", "spark_airflow_adventure_work_project", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")
dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run ID")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
run_id = dbutils.widgets.get("pipeline_run_id")

silver_schema = "silver"
quality_schema = "data_quality_check"

# Target quality log table pointer
target_quality_table = f"{catalog}.{quality_schema}.{data_source}"

# 2. Read ONLY the target Incremental Batch from Silver to isolate validation
logger.info(f"Loading incremental Silver data for validation check. Run ID: {run_id}")
if not run_id.strip():
    logger.warning("No pipeline_run_id provided. Auditing the entire table snapshot.")
    customer_df = spark.read.table(f"{catalog}.{silver_schema}.{data_source}")
else:
    # Read the data and filter explicitly on the active processing metadata tracking column
    customer_df = spark.read.table(f"{catalog}.{silver_schema}.{data_source}") \
                       .filter(F.col("silver_processed_timestamp").isNotNull()) # Or filter by your custom batch run identifier column if appended

# 3. Calculate Audit Metrics using a Single Pass over Storage
metrics = customer_df.select(
    F.count("*").alias("total_rows"),
    F.sum(F.when(F.col("CustomerKey").isNull(), 1).otherwise(0)).alias("null_pks"),
    F.sum(
        F.when(
            F.col("EmailAddress").isNull() | (~F.col("EmailAddress").like("%@%.%")), 1
        ).otherwise(0)
    ).alias("malformed_emails")
).collect()[0]

# Safely extract metrics into scalar variables
total_rows = metrics["total_rows"] if metrics["total_rows"] is not None else 0
null_pks = metrics["null_pks"] if metrics["null_pks"] is not None else 0
malformed_emails = metrics["malformed_emails"] if metrics["malformed_emails"] is not None else 0

# 4. Construct the Vertical Structured Audit Grid
# We calculate a global pass/fail metric to report to Airflow orchestration
pipeline_passed = "PASS" if (null_pks == 0 and total_rows > 0) else "FAIL"

validation_rows = [
    (run_id, "Row Count Check", "PASS" if total_rows > 0 else "FAIL", int(total_rows)),
    (run_id, "Valid Primary Keys", "PASS" if null_pks == 0 else "FAIL", int(null_pks)),
    (run_id, "Email Format Check", "PASS" if malformed_emails == 0 else "FAIL", int(malformed_emails))
]

# 5. Define Uniform Target Output Framework Schema
validation_schema = StructType([
    StructField("pipeline_run_id", StringType(), False),
    StructField("rule_name", StringType(), False),
    StructField("status", StringType(), False),
    StructField("failed_rows", IntegerType(), False)
])

# 6. Generate Validation Report DataFrame
validation_df = spark.createDataFrame(validation_rows, schema=validation_schema)

# 7. Write Verification Summary Results out to Logs (Enforce APPEND Mode)
logger.info(f"Writing quality metrics out to central audit log: {target_quality_table}")
validation_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(target_quality_table)

# 8. HARD PIPELINE BREAKPOINT FOR ORCHESTRATION (Airflow Gatekeeper)
if pipeline_passed == "FAIL":
    raise ValueError(f"❌ Data Quality Gatekeeper Failure for Run ID {run_id}. Missing Primary Keys or empty batch detected. Blocking Snowflake load sync.")
else:
    print(f"✅ Data Quality Gatekeeper Passed for Run ID {run_id}. Proceeding to Snowflake Sync.")