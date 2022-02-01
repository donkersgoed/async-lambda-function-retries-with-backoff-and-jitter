from datetime import datetime
import json
import os
import time

import boto3

sqs_client = boto3.client("sqs")
lambda_client = boto3.client("lambda")

MAX_VISIBILITY_TIMEOUT = 43200  # 12 hours

async_function_name = os.environ["ASYNC_FUNCTION_NAME"]
retry_queue_url = os.environ["RETRY_QUEUE_URL"]
event_max_age = int(os.environ["MAX_AGE"])
event_max_retries = int(os.environ["MAX_RETRIES"])
event_base_backoff = int(os.environ["BASE_BACKOFF"])


class InitialReceiveError(Exception):
    pass


def event_handler(event: dict, _context) -> None:
    returned_message_ids = []
    for sqs_record in event["Records"]:
        try:
            handle_record(sqs_record)
        except InitialReceiveError:
            returned_message_ids.append(sqs_record["messageId"])

    # Report messages for which the timeout has changed as failures,
    # so that they are put back onto the queue.
    return {
        "batchItemFailures": [
            {"itemIdentifier": identifier} for identifier in returned_message_ids
        ]
    }


def handle_record(record: dict) -> None:
    # Retrieve the ApproximateReceiveCount from the SQS Retry Queue
    sqs_approximate_receive_count = int(record["attributes"]["ApproximateReceiveCount"])
    # If ApproximateReceiveCount equals one, this is the first time we
    # fetched it from SQS, and it should be put back with a visibility timeout
    first_receive_from_sqs = sqs_approximate_receive_count == 1

    # Retrieve the message from SQS
    sqs_body = json.loads(record["body"])
    # Get the event the Lambda Function was called with
    lambda_function_payload = sqs_body["requestPayload"]

    if "_retry_metadata" not in lambda_function_payload:
        # If `_retry_metadata` is not present, this is the first time the
        # Lambda Function failed, and we're currently processing the first
        # retry
        event_retry_count = 1
        original_event_timestamp = int(
            datetime.timestamp(
                datetime.strptime(sqs_body["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
            )
        )
    else:
        # If `_retry_metadata` is present, this is the second or later time the
        # Lambda Function failed. We can get the retry count from the function.
        event_retry_count = lambda_function_payload["_retry_metadata"]["attempt"] + 1
        original_event_timestamp = lambda_function_payload["_retry_metadata"][
            "initial_timestamp"
        ]

    event_age = int(time.time() - original_event_timestamp)
    if event_age > event_max_age:
        move_message_to_dlq(lambda_function_payload, "max event age reached")
        return

    if event_retry_count > event_max_retries:
        move_message_to_dlq(lambda_function_payload, "max retries reached")
        return

    if first_receive_from_sqs:
        # Return the message to the queue with the required backoff
        return_sqs_message_with_backoff(record, event_retry_count)
        raise InitialReceiveError()
    else:
        # Update the payload and call the Lambda Function again
        retry_lambda_execution(lambda_function_payload, original_event_timestamp)


def move_message_to_dlq(lambda_function_payload, reason):
    original_payload = lambda_function_payload
    if "_retry_metadata" in lambda_function_payload:
        original_payload = lambda_function_payload["_original_payload"]

    print(f"Deleting message ({reason}): {original_payload}")


def return_sqs_message_with_backoff(record: dict, event_retry_count: int) -> None:
    # Example 1: base_backoff = 1, event_retry_count = 1
    # visibility_timeout = 1 * 2 ^ (1-1) = 1 * 2^0 = 1 * 1 = 1 second
    # Example 2: base_backoff = 1, event_retry_count = 2
    # visibility_timeout = 1 * 2 ^ (2-1) = 1 * 2^1 = 1 * 2 = 2 seconds
    # Example 3: base_backoff = 1, event_retry_count = 3
    # visibility_timeout = 1 * 2 ^ (3-1) = 1 * 2^2 = 1 * 4 = 4 seconds
    visibility_timeout = event_base_backoff * 2 ** (event_retry_count - 1)
    # jitter here

    # Never exceed MAX_VISIBILITY_TIMEOUT
    visibility_timeout = min(MAX_VISIBILITY_TIMEOUT, visibility_timeout)

    sqs_client.change_message_visibility(
        QueueUrl=retry_queue_url,
        ReceiptHandle=record["receiptHandle"],
        VisibilityTimeout=visibility_timeout,
    )

    return  # End of processing


def retry_lambda_execution(
    lambda_function_payload: dict, original_event_timestamp: int
) -> None:
    if "_retry_metadata" in lambda_function_payload:
        lambda_function_payload["_retry_metadata"]["attempt"] += 1
    else:
        original_payload = lambda_function_payload
        lambda_function_payload = {
            "_retry_metadata": {
                "attempt": 1,
                "initial_timestamp": original_event_timestamp,
            },
            "_original_payload": original_payload,
        }

    try:
        response = lambda_client.invoke(
            FunctionName=async_function_name,
            InvocationType="Event",
            Payload=json.dumps(lambda_function_payload).encode("utf-8"),
        )
    except Exception:
        print("Failed to invoke Lambda")
        move_message_to_dlq(lambda_function_payload)
        return

    if response["StatusCode"] != 202:
        move_message_to_dlq(lambda_function_payload, "failed to invoke Lambda")
