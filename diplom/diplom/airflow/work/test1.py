from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import termios

default_args = {
    'owner': 'arnaud',
    'start_date': datetime(2019, 1, 1),
    'retry_delay': timedelta(minutes=5)
}
# Using the context manager alllows you not to duplicate the dag parameter in each operator
with DAG('S3_dag_test', default_args=default_args, schedule_interval='@once') as dag:

    start_task = DummyOperator(
            task_id='dummy_start'
    )

    upload_to_S3_task = PythonOperator(
            task_id='upload_file_to_S3',
            # python_callable=lambda _: print("Uploading file to S3")
    )

    # Use arrows to set dependencies between tasks
    start_task >> upload_to_S3_task
