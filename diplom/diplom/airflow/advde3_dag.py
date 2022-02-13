import logging
import shutil
from datetime import datetime

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

if not shutil.which("virtualenv"):
    log.warning(
        "The 'advde2_dag' DAG requires virtualenv, please install it."
    )
else:
    @dag(schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False, tags=['advde'])
    def advde3_taskflow():

        @task.virtualenv(
            use_dill=True,
            system_site_packages=False,
            requirements=['boto3'],
        )
        def receive_message():
            import boto3
            import json
            import logging

            #boto3.set_stream_logger('boto3.resources', logging.DEBUG)
            session = boto3.Session(
                aws_access_key_id='AKIAZB57MSK74V2NTKFO',
                aws_secret_access_key='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'
            )
            sqs_client = session.client('sqs', region_name="eu-central-1")

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
                return message_body

            return ""

        # @task.virtualenv(
        #     use_dill=True,
        #     system_site_packages=False,
        #     requirements=['ast']
        # )
        @task(multiple_outputs=True)
        def get_file_name(message):
            import ast
            from airflow.exceptions import AirflowSkipException
            if len(message) == 0:
                raise AirflowSkipException("task skipped: 'get_file_name'")

            print(f"accepted message: {message}")
            d = ast.literal_eval(message)
            return {"bucket": d["Records"][0]["s3"]["bucket"]["arn"], "file": d["Records"][0]["s3"]["object"]["key"]}

        @task()
        def end_operation(data):
            print(f"End of DAG: {datetime.date}")
            print(f"final data: {data}")

        message = receive_message()
        file_date = get_file_name(message=message)
        end_operation(file_date)


    tutorial_etl_dag = advde3_taskflow()
