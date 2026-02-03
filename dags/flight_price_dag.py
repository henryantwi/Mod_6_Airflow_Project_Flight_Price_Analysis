from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.models.baseoperator import chain

import sys
sys.path.insert(0, '/opt/airflow/scripts')

# Load environment variables from .env for dynamic configuration
import os
from dotenv import load_dotenv, find_dotenv
# Attempt to locate and load a .env file (searches upward from CWD); override existing env if present
load_dotenv(dotenv_path=find_dotenv(usecwd=True), override=True)

from data_ingestion import ingest_csv_to_mysql
from data_validation import validate_data
from data_transformation import compute_kpis
from data_loading import load_to_postgres


# Resolve email recipients from environment
TO_USER_EMAIL_1 = os.getenv('TO_USER_EMAIL_1')
email_recipients = [TO_USER_EMAIL_1] if TO_USER_EMAIL_1 else []

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email': email_recipients,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
with DAG(
    dag_id='flight_price_analysis',
    default_args=default_args,
    description='ETL pipeline for Bangladesh flight price analysis',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
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

    # Task 5: Notify on success
    notify_success = EmailOperator(
        task_id='notify_success',
        to=email_recipients,
        subject='[Airflow] flight_price_analysis succeeded',
        html_content="""
                <h3>DAG Succeeded</h3>
                <p>The DAG 'flight_price_analysis' completed successfully on {{ ds }}.</p>
            """,
    )

    # Define task dependencies using chain for clarity
    chain(start, ingest_task, validate_task, transform_task, load_task, notify_success)
