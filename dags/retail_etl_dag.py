from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow/include")
from etl import extract, transform, load

DATA_DIR = "/opt/airflow/data/"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="retail_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for retail sales data",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["retail", "etl"],
) as dag:

    check_files = BashOperator(
        task_id="check_csv_files_exist",
        bash_command=f'ls {DATA_DIR}*.csv && echo "CSV files found" || (echo "No CSV files in {DATA_DIR}" && exit 1)',
    )

    def extract_task(**kwargs):
        df = extract(DATA_DIR)
        kwargs["ti"].xcom_push(key="raw_row_count", value=len(df))
        logger_msg = f"Extracted {len(df)} rows"
        print(logger_msg)

    def transform_task(**kwargs):
        df = extract(DATA_DIR)
        df_clean = transform(df)
        kwargs["ti"].xcom_push(key="clean_row_count", value=len(df_clean))
        print(f"Clean rows: {len(df_clean)}")

    def load_task(**kwargs):
        df = extract(DATA_DIR)
        df_clean = transform(df)
        load(df_clean)
        print("Load complete!")

    t_extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_task,
    )

    t_transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_task,
    )

    t_load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_task,
    )

    check_files >> t_extract >> t_transform >> t_load
