import boto3
import os
import requests

def long_poll_sqs_queue(queue_url, post_url, wait_time_seconds=20):
    sqs = boto3.client('sqs')

    while True:
        print("Long polling for messages...")
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=['All'],
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=wait_time_seconds
        )

        messages = response.get('Messages', [])
        for message in messages:
            print(f"Received message: {message['Body']}")
            # Process the message as needed

            # Post the message to the post_url
            if not post_message_to_service(post_url, message['Body']):
                print("Failed to post message to the service, message will not be deleted from the queue")

            # Delete the message from the queue after processing
            receipt_handle = message['ReceiptHandle']
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

def post_message_to_service(service_url, message):
    try:
        response = requests.post(service_url, data=message)
        if response.status_code == 200:
            print(f"Message posted to {service_url}")
            return True
        else:
            print(f"Failed to post message to {service_url}, status code: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error posting message to {service_url}: {e}")
        return False

if __name__ == "__main__":
    # Get queue URL and post URL from environment variables
    queue_url = os.environ.get('SQS_QUEUE_URL')
    post_url = os.environ.get('POST_URL')
    long_poll_sqs_queue(queue_url, post_url)
