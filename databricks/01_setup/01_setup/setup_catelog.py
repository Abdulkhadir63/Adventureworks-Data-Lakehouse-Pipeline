# Databricks notebook source
# MAGIC
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS  spark_airflow_adventure_work_project;
# MAGIC
# MAGIC -- 2. Re-create them linked directly to your AWS S3 paths
# MAGIC CREATE SCHEMA spark_airflow_adventure_work_project.bronze
# MAGIC MANAGED LOCATION 's3://airflow-spark-project/bronze/';
# MAGIC
# MAGIC CREATE SCHEMA spark_airflow_adventure_work_project.silver
# MAGIC MANAGED LOCATION 's3://airflow-spark-project/silver/';
# MAGIC
# MAGIC CREATE SCHEMA spark_airflow_adventure_work_project.gold
# MAGIC MANAGED LOCATION 's3://airflow-spark-project/gold/';