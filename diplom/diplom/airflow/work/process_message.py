import boto3
import ast
import zipfile
import tempfile


def get_file(message):
    d = ast.literal_eval(message)
    return {"bucket": d["Records"][0]["s3"]["bucket"]["arn"], "file": d["Records"][0]["s3"]["object"]["key"]}


def download_file(file, todir):
    s3client = boto3.client("s3")
    fn = todir + "/" + file["file"]
    s3client.download_file("advde-backet2", file["file"], fn)
    if zipfile.is_zipfile(fn):
        with zipfile.ZipFile(fn, 'r') as zip_ref:
            for name in zip_ref.namelist():
                if name.endswith(".csv") and not name.startswith(".") and name.find("/") < 0:
                    zip_ref.extract(name, todir)
                    return True
        return False


def download_file2():
    s3 = boto3.resource(service_name='s3')
    s3.Bucket("advde-backet2").download_file(Key='JC-201810-citibike-tripdata.csv.zip',
                                             Filename='C:\\Users\\vanio\\temp\\file1.zip')
    print('downloaded 2 OK')


def download_file3():
    s3 = boto3.resource(service_name='s3')
    tmpfile = tempfile.NamedTemporaryFile(prefix='aws')
    tmpfile.close()
    s3.Bucket("advde-backet2").download_file(Key='JC-201810-citibike-tripdata.csv.zip', Filename=tmpfile.name)
    print(f'downloaded to {tmpfile.name} "2" OK')


def print_contents():
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(name='advde-backet2')
    for my_bucket_object in my_bucket.objects.all():
        print(my_bucket_object)


with open("event_msg.json", "r") as f:
    d = f.read()
    file = get_file(d)
    print(file)
    with tempfile.TemporaryDirectory(prefix='aws') as tmpdirname:
        print(download_file(file, tmpdirname))
        print(f'downloaded file "{file}" to "{tmpdirname}"')

print_contents()

download_file3()
