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
        filepath="/opt/airflow/data/input/sample_input.csv",
        timeout=3600, 
        poke_interval=60, 
        mode="poke",
        fs_conn_id="fs_default",
        soft_fail=False,
        dag=dag,
    )

    # Task 3: Run Spark transformations
    run_spark_job = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/airflow/dags/src/jobs/user_settings_aggregator.py",
        name="user_settings_etl",
        conn_id="spark_default",
        application_args=[
            "--input-path", "/opt/airflow/data/input/sample_input.csv",
            "--input-format", "csv",
            "--partition-column", "id",
            "--output-path", "/opt/airflow/data/output",
            "--output-format", "json",
        ],
        conf={
            "spark.master": "local[*]",
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",
            "spark.sql.shuffle.partitions": "200",
        },
        verbose=True,
        dag=dag,
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