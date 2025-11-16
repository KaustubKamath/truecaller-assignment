from datetime import datetime, timedelta
from pathlib import Path 

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# default arguments for all tasks
DEFAULT_ARGS = {
    "owner": "Kaustub",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id = "user_settings_etl_daily",
    default_args=DEFAULT_ARGS,
    description="Daily ETL for user settings aggregated into maps",
    schedule_interval='0 0 * * *',  # daily at midnight
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task 1: Start of DAG
    start = EmptyOperator(
        task_id = "start",
        dag = dag,
    )

    # Task 2: Check for data availability
    check_input_data = FileSensor(
        task_id="check_input_data_available",
        filepath="/opt/airflow/data/sample_input.csv",
        timeout=3600, 
        poke_interval=60, 
        mode = "poke",
        fs_conn_id = "local_file_path",
        soft_fail = False,
        dag = dag,
    )

    # Task 3: Run Spark transformations
    run_spark_job = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/airflow/dags/dags_spark/src/jobs/user_settings_aggregator.py",
        name="spark_submit_test",
        conn_id="spark_local",   # do not use spark_default (which had master=yarn)
        application_args=[
            "--input-path", "/opt/airflow/data/sample_input.csv",
            "--input-format", "csv",
            "--partition-column", "id", #need to implement
            "--output-path", "/opt/airflow/data/outputs",
            "--output-format", "json",
            ],
            
        conf={
            "spark.master": "local[*]",
            "spark.executor.memory": "8g",
            "spark.executor.cores": "4",
            "spark.executor.instances": "10", 
            "spark.driver.memory": "4g",
            "spark.sql.shuffle.partitions": "200",
            },
        verbose=True,
        dag = dag,
    )

    # Task 4: Send success notification
    send_success_notification = EmptyOperator(
        task_id = "send_success_notification",
        dag = dag,
    )

    # Task 5: End of DAG
    end = EmptyOperator(
        task_id = "end",
        dag = dag,
    )

    # Task dependencies
    (
        start
        >> check_input_data
        >> run_spark_job
        >> send_success_notification
        >> end
    )