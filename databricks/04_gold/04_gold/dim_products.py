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

# Target Master Table Identity
target_table_name = "dim_products"
target_table = f"{catalog}.{gold_schema}.{target_table_name}"

# Decoupled Target S3 Gold Storage Location
gold_load_path = f"s3://airflow-spark-project/gold/{target_table_name}"

# 2. ADAPTIVE READ LAYER FOR ALL 3 SILVER SOURCE TABLES
# Applies full table scan fallback if run ID is blank, otherwise filters incrementally
if not current_run_id or current_run_id.strip() in ("", "None", "null"):
    print("⚠️ No valid pipeline_run_id provided or cleared. Snapshot fallback activated: Scanning full table.")
    df_p = spark.read.table(f"{catalog}.{silver_schema}.products")
    df_psc = spark.read.table(f"{catalog}.{silver_schema}.product_sub_category")
    df_pc = spark.read.table(f"{catalog}.{silver_schema}.product_category")
else:
    print(f"📊 Pipeline Run ID detected: {current_run_id}. Processing incremental run updates.")
    df_p = spark.read.table(f"{catalog}.{silver_schema}.products").filter(F.col("pipeline_run_id") == current_run_id)
    df_psc = spark.read.table(f"{catalog}.{silver_schema}.product_sub_category").filter(F.col("pipeline_run_id") == current_run_id)
    df_pc = spark.read.table(f"{catalog}.{silver_schema}.product_category").filter(F.col("pipeline_run_id") == current_run_id)

# 3. EXECUTE STRUCTURAL LEFT JOINS (Products -> Subcategory -> Category)
df_joined = df_p.alias("p") \
    .join(
        df_psc.alias("psc"),
        F.col("p.ProductSubcategoryKey") == F.col("psc.ProductSubcategoryKey"),
        "left"
    ) \
    .join(
        df_pc.alias("pc"),
        F.col("psc.ProductCategoryKey") == F.col("pc.ProductCategoryKey"),
        "left"
    )

# 4. SELECT EXACT BLUEPRINT BUSINESS COLUMNS & INJECT NEW_DATE METADATA
df_gold_batch = df_joined.select(
    F.col("p.ProductKey").alias("ProductKey"),
    F.col("p.ProductName").alias("ProductName"),
    F.col("p.ProductSKU").alias("ProductSKU"),
    F.col("p.ModelName").alias("ModelName"),
    F.col("p.ProductDescription").alias("ProductDescription"),
    F.col("p.ProductColor").alias("ProductColor"),
    F.col("p.ProductSize").alias("ProductSize"),
    F.col("p.ProductStyle").alias("ProductStyle"),
    
    F.col("pc.ProductCategoryKey").alias("ProductCategoryKey"),
    F.col("pc.CategoryName").alias("CategoryName"),
    
    F.col("psc.ProductSubcategoryKey").alias("ProductSubcategoryKey"),
    F.col("psc.SubcategoryName").alias("SubcategoryName"),
    
    F.col("p.ProductCost").cast("decimal(10,4)").alias("ProductCost"),
    F.col("p.ProductPrice").cast("decimal(10,4)").alias("ProductPrice"),
)

# 5. EXTERNAL S3 DELTA UPSERT (MERGE) LAYER
if spark.catalog.tableExists(target_table):
    # If the Master Gold table exists, execute an incremental upsert (Merge)
    gold_delta_table = DeltaTable.forName(spark, target_table)
    
    gold_delta_table.alias("target") \
        .merge(
            source = df_gold_batch.alias("updates"),
            condition = "target.ProductKey = updates.ProductKey"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
else:
    # Initial run setup pointing cleanly to the S3 Gold storage location
    df_gold_batch.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", gold_load_path) \
        .saveAsTable(target_table)

print(f"🚀 Master Product Dimension table successfully upserted into S3 and Unity Catalog!")

# COMMAND ----------

