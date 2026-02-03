from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator

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


def check_for_new_data(**context):
    """
    Check if new records were inserted.
    Returns task_id to branch to.
    """
    ti = context['ti']
    rows_inserted = ti.xcom_pull(task_ids='ingest_csv_to_mysql')
    
    if rows_inserted is None or rows_inserted == 0:
        print("No new records detected - skipping to no_change notification")
        return 'notify_no_change'
    
    print(f"New records detected ({rows_inserted}) - continuing pipeline")
    return 'validate_data'


# Define the DAG
with DAG(
    dag_id='flight_price_analysis',
    default_args=default_args,
    description='ETL pipeline for Bangladesh flight price analysis',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,  # Prevents auto-trigger on startup
    tags=['flight', 'etl', 'analytics'],
) as dag:

    # Start task
    start = DummyOperator(task_id='start')

    # Task 1: Ingest CSV to MySQL
    ingest_task = PythonOperator(
        task_id='ingest_csv_to_mysql',
        python_callable=ingest_csv_to_mysql,
    )

    # Task 2: Branch based on whether new data was ingested
    branch_task = BranchPythonOperator(
        task_id='check_for_new_data',
        python_callable=check_for_new_data,
        provide_context=True,
    )

    # Task 3: Validate Data (only if new data)
    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
    )

    # Task 4: Compute KPIs
    transform_task = PythonOperator(
        task_id='compute_kpis',
        python_callable=compute_kpis,
    )

    # Task 5: Load to PostgreSQL
    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )

    # Task 6a: Notify on success (after full processing)
    notify_success = EmailOperator(
        task_id='notify_success',
        to=email_recipients,
        subject='[Airflow] flight_price_analysis - New Data Processed',
        html_content="""
            <h3>DAG Succeeded - New Data Processed</h3>
            <p>The DAG 'flight_price_analysis' completed successfully on {{ ds }}.</p>
            <p>New records were ingested and processed through the full pipeline.</p>
        """,
    )

    # Task 6b: Notify when no changes (skip processing)
    notify_no_change = EmailOperator(
        task_id='notify_no_change',
        to=email_recipients,
        subject='[Airflow] flight_price_analysis - No New Data',
        html_content="""
            <h3>DAG Completed - No Changes</h3>
            <p>The DAG 'flight_price_analysis' ran on {{ ds }} but no new data was detected.</p>
            <p>The CSV file contains the same records as the database. No processing was performed.</p>
        """,
    )

    # End task to join branches
    end = DummyOperator(
        task_id='end',
        trigger_rule='none_failed_min_one_success',
    )

    # Define task dependencies
    # Path 1 (new data): start -> ingest -> branch -> validate -> transform -> load -> notify_success -> end
    # Path 2 (no change): start -> ingest -> branch -> notify_no_change -> end
    
    start >> ingest_task >> branch_task
    
    # Branch 1: Process new data
    branch_task >> validate_task >> transform_task >> load_task >> notify_success >> end
    
    # Branch 2: No new data, skip to notification
    branch_task >> notify_no_change >> end
