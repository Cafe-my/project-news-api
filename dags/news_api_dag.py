from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

from src.job_raw import task_raw

with DAG(
    "news_api_dag",
    description="DAG para processar dados da News API e armazenar no S3",
    start_date=datetime(2026, 1, 1),
    schedule='@hourly',
    catchup=False # para não executar as execuções passadas caso a DAG seja iniciada depois da data de início
) as dag:
    task_process_raw = PythonOperator(
        task_id="job_raw",
        python_callable=task_raw
    )
    