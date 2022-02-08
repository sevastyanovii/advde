import boto3
import ast
import zipfile


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


with open("./event_msg.json", "r") as f:
    d = f.read()
    file = get_file(d)
    print(file)
    print(download_file(file, "C:\\Users\\vanio\\temp"))
