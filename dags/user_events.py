from datetime import datetime, timedelta
from pathlib import Path 

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor

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
        >> send_success_notification
        >> end
    )