from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.standard.operators.empty import EmptyOperator
import pendulum



DATA_SOURCES = [
    "calendar", "customers", "product_category", 
    "product_sub_category", "returns", "sales", 
    "territories", "products"
]

S3_BUCKET = "airflow-spark-project"
DATABRICKS_JOB_ID = 22508717830898

@dag(
    dag_id='adventure_works_fullLoad_bronze',
    schedule='@daily',
    start_date=datetime(2026, 7, 18, tzinfo=pendulum.timezone("Asia/Kolkata")),
    catchup=False,
    max_active_runs=1,
    tags=['local_dev', 'bronze']
)
def adventure_works_fullLoad_bronze_pipeline():

    start_pipeline = EmptyOperator(task_id='start_pipeline')
    # end_pipeline = EmptyOperator(task_id='end_pipeline')
    
    # Core Optimization Task List
    sensor_tasks = []

    # 1. Sensors run in parallel to verify all datasets are safely landed on S3
    for source in DATA_SOURCES:
        s3_sensor = S3KeySensor(
            task_id=f'wait_for_{source}_csv',
            bucket_name=S3_BUCKET,
            bucket_key=f'incoming/{source}/*.csv', 
            wildcard_match=True,
            aws_conn_id='aws_default',
            poke_interval=60,
            timeout=3600,
            mode='reschedule' # Keeps local Docker footprint at near-zero
        )
        
        start_pipeline >> s3_sensor
        sensor_tasks.append(s3_sensor)

    # 2. Senior Move: Single Operator invocation instead of looping the API call
    run_databricks_pipeline = DatabricksRunNowOperator(
        task_id="trigger_databricks_pipeline",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_ID,
        notebook_params={
            "pipeline_run_id": "{{ run_id }}"
        }
    )

    # 3. Connect all sensors to the single trigger task, then close the pipeline
    start_pipeline >> sensor_tasks >> run_databricks_pipeline 

# Instantiate the DAG
adventure_works_fullLoad_bronze_pipeline()