"""
Flight Price Analysis DAG
End-to-end data pipeline for Bangladesh flight price analysis

Author: Data Engineering Student
Date: 2024
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

import sys
sys.path.insert(0, '/opt/airflow/scripts')

from data_ingestion import ingest_csv_to_mysql
from data_validation import validate_data
from data_transformation import compute_kpis
from data_loading import load_to_postgres


# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='flight_price_analysis',
    default_args=default_args,
    description='ETL pipeline for Bangladesh flight price analysis',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['flight', 'etl', 'analytics'],
) as dag:

    # Start task
    start = DummyOperator(task_id='start')

    # Task 1: Ingest CSV to MySQL
    ingest_task = PythonOperator(
        task_id='ingest_csv_to_mysql',
        python_callable=ingest_csv_to_mysql,
    )

    # Task 2: Validate Data
    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
    )

    # Task 3: Compute KPIs
    transform_task = PythonOperator(
        task_id='compute_kpis',
        python_callable=compute_kpis,
    )

    # Task 4: Load to PostgreSQL
    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )

    # End task
    end = DummyOperator(task_id='end')

    # Define task dependencies
    # start -> ingest -> validate -> transform -> load -> end
    start >> ingest_task >> validate_task >> transform_task >> load_task >> end
