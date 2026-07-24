# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# 1. Initialize Runtime Parameters
dbutils.widgets.text("catalog", "spark_airflow_adventure_work_project", "Catalog")
dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run ID")

catalog = dbutils.widgets.get("catalog")
current_run_id = dbutils.widgets.get("pipeline_run_id")

silver_schema = "silver"
gold_schema = "gold"

target_table_name = "fact_sales"
target_table = f"{catalog}.{gold_schema}.{target_table_name}"
gold_load_path = f"s3://airflow-spark-project/gold/{target_table_name}"

# 2. ADAPTIVE READ LAYER (Sales & Product Dimension)
if not current_run_id or current_run_id.strip() in ("", "None", "null"):
    print("⚠️ No valid pipeline_run_id provided or cleared. Snapshot fallback activated: Scanning full table.")
    df_sales = spark.read.table(f"{catalog}.{silver_schema}.sales")
else:
    print(f"📊 Pipeline Run ID detected: {current_run_id}. Processing incremental run updates.")
    df_sales = spark.read.table(f"{catalog}.{silver_schema}.sales").filter(F.col("pipeline_run_id") == current_run_id)

# We always read the full product dimension to ensure matches on historical keys
df_dim_prod = spark.read.table(f"{catalog}.{silver_schema}.products")

# 3. JOIN SALES TO PRODUCT DIMENSION
df_joined = df_sales.alias("s") \
    .join(
        df_dim_prod.alias("p"),
        F.col("s.ProductKey") == F.col("p.ProductKey"),
        "left"
    )

# 4. EXECUTE REAL-WORLD BUSINESS MATH & PROJECTIONS
df_gold_batch = df_joined.select(
    F.col("s.OrderDate").alias("OrderDate"),
    F.col("s.StockDate").alias("StockDate"),
    F.col("s.OrderNumber").alias("OrderNumber"),
    F.col("s.ProductKey").cast("int").alias("ProductKey"),
    F.col("s.CustomerKey").cast("int").alias("CustomerKey"),
    F.col("s.TerritoryKey").cast("int").alias("TerritoryKey"),
    F.col("s.OrderLineItem").cast("int").alias("OrderLineItem"),
    F.col("s.OrderQuantity").cast("int").alias("OrderQuantity"),
    
    # Bring in raw baseline items
    F.col("p.ProductPrice").cast("decimal(18,4)").alias("ProductPrice"),
    F.col("p.ProductCost").cast("decimal(18,4)").alias("ProductCost"),
    
    # Senior Calculation Layer: Math execution using standard operators
    (F.col("s.OrderQuantity") * F.col("p.ProductPrice")).cast("decimal(18,4)").alias("Revenue"),
    (F.col("s.OrderQuantity") * F.col("p.ProductCost")).cast("decimal(18,4)").alias("Cost"),
    ((F.col("s.OrderQuantity") * F.col("p.ProductPrice")) - 
     (F.col("s.OrderQuantity") * F.col("p.ProductCost"))).cast("decimal(18,4)").alias("Profit")
)
# 5. EXTERNAL S3 DELTA MERGE WRITE LAYER
if spark.catalog.tableExists(target_table):
    gold_delta_table = DeltaTable.forName(spark, target_table)
    gold_delta_table.alias("target") \
        .merge(
            source = df_gold_batch.alias("updates"),
            condition = """
                target.OrderNumber = updates.OrderNumber AND 
                target.OrderLineItem = updates.OrderLineItem AND 
                target.ProductKey = updates.ProductKey
            """
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
else:
    df_gold_batch.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", gold_load_path) \
        .saveAsTable(target_table)

print(f"🚀 Real-world Calculated Fact Table built successfully!")

# COMMAND ----------

