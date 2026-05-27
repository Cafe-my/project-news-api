from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

from src.job_raw import task_raw


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    "news_api_dag",
    description="DAG para processar dados da News API e armazenar no S3",
    start_date=datetime(2026, 1, 1),
    schedule='10 9 * * *',
    default_args=default_args,
    catchup=False # para não executar as execuções passadas caso a DAG seja iniciada depois da data de início
) as dag:
    task_process_raw = PythonOperator(
        task_id="job_raw",
        python_callable=task_raw
    )
    