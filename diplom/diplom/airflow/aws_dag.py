import boto3
import json


def receive_message():
    # boto3.set_stream_logger('botocore', level='DEBUG')

    session = boto3.Session(
        aws_access_key_id='AKIAZB57MSK74V2NTKFO',
        aws_secret_access_key='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'
    )

    # Then use the session to get the resource
    sqs_client = session.client('sqs', region_name="eu-central-1")

    # s3.Bucket('stackvidhya').upload_file('E:/temp/testfile.txt', 'file2_uploaded_by_boto3.txt')
    # sqs_client = boto3.client("sqs", region_name="eu-central-1")
    response = sqs_client.receive_message(
        QueueUrl="https://sqs.eu-central-1.amazonaws.com/622634767039/AdvdeQueue",
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10,
    )

    print(f"Number of messages received: {len(response.get('Messages', []))}")

    for message in response.get("Messages", []):
        print("message\n", message)
        print("\n")

        # for el in range(0..len(message)):
        #     print(el)
        message_body = message["Body"]
        print(f"Message body: {json.loads(message_body)}")
        print(f"Receipt Handle: {message['ReceiptHandle']}")


receive_message()
