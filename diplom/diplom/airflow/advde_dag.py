import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.utils import dates
import boto3
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def receive_message():
    sqs_client = boto3.client("sqs", region_name="eu-central-1")
    response = sqs_client.receive_message(
        QueueUrl="https://sqs.eu-central-1.amazonaws.com/622634767039/AdvdeQueue",
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10,
    )
    print(f"Number of messages received: {len(response.get('Messages', []))}")

    for message in response.get("Messages", []):
        print("message\n", message)
        print("\n")

        message_body = message["Body"]
        print(f"Message body: {json.loads(message_body)}")
        print(f"Receipt Handle: {message['ReceiptHandle']}")
        break

def end_operation():
    print(f"End of DAG: {datetime.date}")

with DAG(dag_id='advbde1_DAG',
         start_date=dates.days_ago(1),
         max_active_runs=1,
         schedule_interval=timedelta(minutes=5),
         default_args=default_args,
         catchup=False
         ) as dag:

    check_queue_task = PythonOperator(
        task_id='check_queue_task_id',
        python_callable=receive_message
    )

    final_point_task = PythonOperator(
        task_id='final_point_task_id',
        python_callable=end_operation
    )

check_queue_task >> final_point_task