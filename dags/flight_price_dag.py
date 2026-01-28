from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

import sys
sys.path.insert(0, '/opt/airflow/scripts')

from data_ingestion import ingest_csv_to_mysql
from sync_to_postgres import sync_to_postgres


# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# dbt project path
DBT_PROJECT_DIR = '/opt/airflow/dbt_flight_analytics'
DBT_PROFILES_DIR = '/opt/airflow/dbt_flight_analytics'

# Define the DAG
with DAG(
    dag_id='flight_price_analysis_dbt',
    default_args=default_args,
    description='ETL pipeline for Bangladesh flight price analysis using dbt',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['flight', 'etl', 'analytics', 'dbt'],
) as dag:

    # Start task
    start = DummyOperator(task_id='start')

    # Task 1: Ingest CSV to MySQL (staging)
    ingest_task = PythonOperator(
        task_id='ingest_csv_to_mysql',
        python_callable=ingest_csv_to_mysql,
    )

    # Task 2: Sync raw data to PostgreSQL for dbt
    sync_task = PythonOperator(
        task_id='sync_to_postgres',
        python_callable=sync_to_postgres,
    )

    # Task 3: Run dbt models (validation + transformations)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR}',
    )

    # Task 4: Run dbt tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}',
    )

    # End task
    end = DummyOperator(task_id='end')

    # Define task dependencies
    # start -> ingest -> sync -> dbt_run -> dbt_test -> end
    start >> ingest_task >> sync_task >> dbt_run >> dbt_test >> end
