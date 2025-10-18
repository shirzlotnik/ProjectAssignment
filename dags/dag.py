from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from tasks import extract, transform, load


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='complaince_etl',
    default_args=default_args,
    schedule_interval="0 12 * * *",
    catchup=False,
    max_active_runs=1
) as dag:

    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    task_load = PythonOperator(
        task_id='load',
        python_callable=load
    )


    task_extract >> task_transform >> task_load
