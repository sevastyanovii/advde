import boto3
import json


def receive_message():
    # boto3.set_stream_logger('botocore', level='DEBUG')
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

        # for el in range(0..len(message)):
        #     print(el)
        message_body = message["Body"]
        print(f"Message body: {json.loads(message_body)}")
        print(f"Receipt Handle: {message['ReceiptHandle']}")


receive_message()
