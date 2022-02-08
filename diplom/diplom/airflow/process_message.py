import json
import ast


def get_file(message):
    # json_acceptable_string = message.replace("'", "\"")
    # d = json.loads(json_acceptable_string)
    d = ast.literal_eval(message)
    return {"bucket": d["Records"][0]["s3"]["bucket"]["arn"], "file": d["Records"][0]["s3"]["object"]["key"]}


def download_file(file, todir):
    import boto3

    # s3client = boto3.client("s3", region_name="eu-central-1")
    s3client = boto3.client("s3")
    # s3client.download_file(file["bucket"], file["file"], todir + "/" + file["file"])
    s3client.download_file("advde-backet2", file["file"], todir + "/" + file["file"])


# file = {}
with open("./event_msg.json", "r") as f:
    d = f.read()
    file = get_file(d)
    print(file)
    download_file(file, "C:\\Users\\vanio\\temp")
