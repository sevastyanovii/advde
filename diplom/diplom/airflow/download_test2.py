import boto3
import uuid
import os

TODIR = 'c:\\Users\\vanio\\temp'


def download_file2():
    s3 = boto3.resource(service_name='s3')
    folder = str(uuid.uuid1())
    path = os.path.join(TODIR, folder, 'file1.zip')
    os.mkdir(os.path.join(TODIR, folder))
    print(f'full path "{path}"')
    s3.Bucket("advde-backet2").download_file(Key='JC-201810-citibike-tripdata.csv.zip', Filename=path)
    print('downloaded 2 OK')


download_file2()


def os2():
    sub_folder = str(uuid.uuid1())
    name = '/home/airflow'
    print(os.path.abspath(name))
    print(os.path.dirname(name))
    os.mkdir(os.path.join(os.path.abspath(name), sub_folder))

os2()