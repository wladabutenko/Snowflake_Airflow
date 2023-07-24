from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from core_functions import csv_to_raw_task, read_csv_to_raw_table, stage_to_master_table

"""Define some important parameters, to ensure that Airflow executes the DAGs
 at designated time intervals and an appropriate number of times"""

args = {
 'owner': 'Admin',
 'start_date': datetime.now(),
 # 'end_date': datetime(2018, 12, 30),
 'depends_on_past': False,
 'email': ['airflow@airflow.com'],
 'email_on_failure': False,
 'email_on_retry': False,
 'retries': 2,
 'retry_delay': timedelta(minutes=5)
}

# Define Airflow DAG

dag = DAG(
    'data_transfer_dag',
    description='CSV to RAW_TABLE DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 1),
    catchup=False  # Prevents backfilling for past dates.
)

# Define the tasks for the data transfer.
csv_to_raw_task = PythonOperator(
    task_id='csv_to_raw',
    python_callable=read_csv_to_raw_table,
    dag=dag
)

raw_to_stage_task = PythonOperator(
    task_id='raw_to_stage',
    python_callable=raw_to_stage_table,
    dag=dag
)

stage_to_master_task = PythonOperator(
    task_id='stage_to_master',
    python_callable=stage_to_master_table,
    dag=dag
)

# Define the task dependencies
csv_to_raw_task >> raw_to_stage_task >> stage_to_master_task
