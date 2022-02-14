import logging
import shutil
from datetime import datetime
import zipfile
import tempfile
from airflow.exceptions import AirflowSkipException
import os

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

if not shutil.which("virtualenv"):
    log.warning(
        "The 'advde2_dag' DAG requires virtualenv, please install it."
    )
else:
    @dag(schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False, tags=['advde'])
    def advde3_taskflow():

        def create_temp_file():
            tmpfile = tempfile.NamedTemporaryFile(prefix='aws')
            tmpfile.close()
            return tmpfile.name

        def unzip_file(zf):
            if zipfile.is_zipfile(zf):
                with zipfile.ZipFile(zf) as zip_file:
                    for member in zip_file.namelist():
                        filename = os.path.basename(member)
                        # skip directories
                        if not filename:
                            continue

                        if member.endswith(".csv") and not member.startswith(".") and member.find("/") < 0:
                            # copy file (taken from zipfile's extract)
                            source = zip_file.open(member)
                            unzipped = create_temp_file()
                            target = open(unzipped, "wb")
                            with source, target:
                                shutil.copyfileobj(source, target)
                            print(f'unzipped {member} into {unzipped}')
                            return unzipped
                        print(f'File "{zf}" is not contain data. Will be skipped...')
                    else:
                        print(f'File "{zf}" is not zipped. Will be skipped...')

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

        @task
        def download_file3(file):
            import boto3
            s3 = boto3.resource(service_name='s3',
                                aws_access_key_id='AKIAZB57MSK74V2NTKFO',
                                aws_secret_access_key='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')
            tmpfile = tempfile.NamedTemporaryFile(prefix='aws')
            tmpfile.close()
            s3.Bucket("advde-backet2").download_file(Key=file, Filename=tmpfile.name)
            print(f'file {file} downloaded to {tmpfile.name} "2" OK')
            return tmpfile.name

        @task
        def unzip_file0(file):
            unzipped = unzip_file(file)
            if len(unzipped) == 0:
                raise AirflowSkipException()
            return unzipped

        @task.virtualenv(
            use_dill=True,
            system_site_packages=False,
            requirements=['clickhouse_driver', 'pandas'],
        )
        def load_data0(file):
            def load_data(csv_file):
                from clickhouse_driver import Client
                import pandas as pd

                client = Client(host='130.61.143.82', settings={'use_numpy': True})
                client.execute('DROP TABLE IF EXISTS advdedb.ride')
                client.execute(
                    """
                CREATE TABLE advdedb.ride (
                	  ride_id             String
                	, rideable_type       String
                	, started_at          DateTime
                	, ended_at            DateTime
                	, start_station_name  String
                	, start_station_id    String
                	, end_station_name    String
                	, end_station_id      String
                	, start_lat           Float64
                	, start_lng           Float64
                	, end_lat             Float64
                	, end_lng             Float64
                	, member_casual       String 
                 ) ENGINE = MergeTree ORDER BY (started_at)
                 """)

                data = pd.read_csv(csv_file)
                inserted = client.insert_dataframe('INSERT INTO advdedb.ride VALUES',
                                                   pd.DataFrame(data)
                                                   )
                return f'inserted: {inserted}'

            return load_data(file)

        @task()
        def end_operation(data):
            print(f"End of DAG: {datetime.date}")
            print(f"final data: {data}")

        message = receive_message()
        file_data = get_file_name(message=message)
        downloaded = download_file3(file_data["file"])
        unzipped = unzip_file0(downloaded)
        final_msg = load_data0(unzipped)
        end_operation(final_msg)


    tutorial_etl_dag = advde3_taskflow()
