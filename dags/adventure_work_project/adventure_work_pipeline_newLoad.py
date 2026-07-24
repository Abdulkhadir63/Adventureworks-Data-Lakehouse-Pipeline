from datetime import datetime, timedelta
from airflow.sdk import task,dag
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.bases.sensor import PokeReturnValue
import pendulum
import logging
logger = logging.getLogger(__name__)


# default_args = {
#     'owner': 'data_engineering',
#     'depends_on_past': False,
#     'start_date': datetime(2026, 1, 1), # Anchored for our 2026 runs
#     'retries': 2,
#     'retry_delay': timedelta(minutes=3),
# }

# The 8 core tables from your e-commerce data footprint
DATA_SOURCES = [
    "calendar",
    "customers",
    "product_category",
    "product_sub_category",
    "returns",
    "sales",
    "territories",
    "products"
]
DATABRICKS_JOB_ID = 22508717830898
S3_BUCKET = "airflow-spark-project"

@dag(
    dag_id='adventure_works_newLoad_bronze',
    start_date=datetime(2026, 7, 18, tzinfo=pendulum.timezone("Asia/Kolkata")),
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['local_dev', 'bronze']
) 

def adventure_works_newLoad_bronze():

    start_pipeline = EmptyOperator(task_id='start_pipeline')
    end_pipeline = EmptyOperator(task_id='end_pipeline')
    sensor_tasks = []
    # Dynamically loops and creates parallel tracks for each table
    for source in DATA_SOURCES:
        
        @task.sensor(
            task_id=f'wait_for_{source}_csv',
            poke_interval=60,              # Checks every 60 seconds
            timeout=3600,                  # Fails if nothing arrives in an hour
            mode='reschedule'              # Frees up local Docker resources while waiting
        )
        def check_s3_file_freshness(bucket_name: str, folder_prefix: str, **context):
            """Finds any CSV in the folder and verifies if the newest one landed today."""
            from airflow.sensors.base import PokeReturnValue
            
            s3_hook = S3Hook(aws_conn_id='aws_default')
            
            # 1. List all keys inside the folder path
            keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=folder_prefix)
            
            # 2. Filter out to only match CSV files (mimicking your wildcard *.csv)
            csv_keys = [k for k in keys if k.lower().endswith('.csv')] if keys else []
            
            if not csv_keys:
                logger.info(f"No CSV files found in {folder_prefix} yet. Waiting...")
                return PokeReturnValue(is_done=False)
            
            # 3. Look up metadata for these files to extract the timestamps
            timestamps = []
            for key in csv_keys:
                s3_object = s3_hook.get_key(key=key, bucket_name=bucket_name)
                timestamps.append(s3_object.last_modified)
            
            # Get the timestamp of the absolute newest file in that folder
            newest_file_timestamp = max(timestamps)
            
            # 4. Get the logical start time of today's DAG execution run
            dag_run_date = context['data_interval_start']
            
            # Check if that newest file was dropped/modified TODAY
            if newest_file_timestamp >= dag_run_date:
                logger.info(f"Success: New file update detected! Last modified at: {newest_file_timestamp}")
                return PokeReturnValue(is_done=True)
            
            logger.info(f"Stale directory: Newest file is from a previous run ({newest_file_timestamp}). Waiting for today's refresh...")
            return PokeReturnValue(is_done=False)

        # Execute our custom wildcard metadata sensor task
        s3_metadata_sensor = check_s3_file_freshness(
            bucket_name=S3_BUCKET,
            folder_prefix=f"incoming/{source}/"
        )
        sensor_tasks.append(s3_metadata_sensor)

        # 2. Call Databricks API to process the file into a Bronze Delta table
    run_databricks_pipeline = DatabricksRunNowOperator(
        task_id="trigger_databricks_pipeline",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_ID,
        notebook_params={
            "pipeline_run_id": "{{ run_id }}"
        }
    )


        # Connect tasks: start -> check S3 -> execute notebook -> end
    start_pipeline >> sensor_tasks >> run_databricks_pipeline >> end_pipeline
    
adventure_works_newLoad_bronze()