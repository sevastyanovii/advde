import logging
import shutil
from datetime import datetime
import zipfile
import tempfile
from airflow.exceptions import AirflowSkipException

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

            # boto3.set_stream_logger('boto3.resources', logging.DEBUG)
            session = boto3.Session(
                aws_access_key_id='AKIAZB57MSK74V2NTKFO',
                aws_secret_access_key='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'
            )
            sqs_client = session.client('sqs', region_name="eu-central-1")

            response = sqs_client.receive_message(
                QueueUrl="https://sqs.eu-central-1.amazonaws.com/622634767039/AdvdeQueue",
                MaxNumberOfMessages=1,
                WaitTimeSeconds=3,
            )
            print(f"Original response: {response}")
            print(f"Number of messages received: {len(response.get('Messages', []))}")

            for message in response.get("Messages", []):
                print("message\n", message)
                print("\n")

                message_body = message["Body"]
                print(f"Message body: {json.loads(message_body)}")
                print(f"Receipt Handle: {message['ReceiptHandle']}")
                return message_body

            return ""

        @task(multiple_outputs=True)
        def get_file_name(message):
            import ast
            if len(message) == 0:
                raise AirflowSkipException("task skipped: 'get_file_name'")

            print(f"accepted message: {message}")
            d = ast.literal_eval(message)
            return {"bucket": d["Records"][0]["s3"]["bucket"]["arn"], "file": d["Records"][0]["s3"]["object"]["key"]}

        # @task
        # def download_file(file, todir):
        #     import boto3
        #     session = boto3.Session(
        #         aws_access_key_id='AKIAZB57MSK74V2NTKFO',
        #         aws_secret_access_key='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'
        #     )
        #     s3client = session.client('s3', region_name="eu-central-1")
        #     fn = todir + "/" + file["file"]
        #     print(f'Attempting to download file "{file["file"]}" to directory "{todir}"', f"full path: {fn}")
        #     s3client.download_file("advde-backet2", file["file"], fn)
        #     if zipfile.is_zipfile(fn):
        #         with zipfile.ZipFile(fn, 'r') as zip_ref:
        #             for name in zip_ref.namelist():
        #                 if name.endswith(".csv") and not name.startswith(".") and name.find("/") < 0:
        #                     zip_ref.extract(name, todir)
        #                     return todir + "/" + name
        #     else:
        #         print(f'File "{fn}" is not zipped. Will be skipped...')
        #     raise AirflowSkipException("task skipped: target file is not found")
        # @task
        # def download_file2(file, todir):
        #     import boto3
        #     import os
        #     import uuid
        #
        #     s3 = boto3.resource(service_name='s3',
        #                         aws_access_key_id='AKIAZB57MSK74V2NTKFO',
        #                         aws_secret_access_key='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')
        #     folder = str(uuid.uuid1())
        #     dir_name = os.path.abspath(todir)
        #     os.mkdir(os.path.join(dir_name, folder))
        #     full_file_name = os.path.join(dir_name, folder, file["file"])
        #     s3.Bucket("advde-backet2").download_file(Key=file["file"], Filename=full_file_name)
        #     print('downloaded 2 OK')
        #     return full_file_name

        @task
        def download_file3(file):
            import boto3
            s3 = boto3.resource(service_name='s3',
                                aws_access_key_id='AKIAZB57MSK74V2NTKFO',
                                aws_secret_access_key='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')
            tmpfile = tempfile.NamedTemporaryFile(prefix='aws')
            tmpfile.close()
            s3.Bucket("advde-backet2").download_file(Key='JC-201810-citibike-tripdata.csv.zip', Filename=tmpfile.name)
            print(f'downloaded to {tmpfile.name} "2" OK')
            return tmpfile.name

        @task()
        def end_operation(data):
            print(f"End of DAG: {datetime.date}")
            print(f"final data: {data}")

        message = receive_message()
        file_data = get_file_name(message=message)
        downloaded = download_file3(file_data)
        end_operation(downloaded)


    tutorial_etl_dag = advde3_taskflow()
