"""The Retry Handler. Receive messages from the Retry Queue."""

from datetime import datetime
import json
import os
import random
import time

import boto3

sqs_client = boto3.client("sqs")
lambda_client = boto3.client("lambda")

# Set visibility timeout to 12 hours minus 30 seconds
# Technically 43200 should work, but in practice it
# often leads to errors
MAX_VISIBILITY_TIMEOUT = 43170

async_function_name = os.environ["ASYNC_FUNCTION_NAME"]
retry_queue_url = os.environ["RETRY_QUEUE_URL"]
dlq_url = os.environ["DEAD_LETTER_QUEUE_URL"]
event_max_age = max(86400, int(os.environ["MAX_AGE"]))
event_max_retries = int(os.environ["MAX_RETRIES"])
event_base_backoff = min(1, int(os.environ["BASE_BACKOFF"]))


class InitialReceiveError(Exception):
    """Empty exception to indicate initial receives."""


class LambdaInvocationError(Exception):
    """Empty exception to indicate failed Lambda invocations."""


def event_handler(event: dict, _context) -> None:
    """Receive a batch of events and return failures."""
    returned_message_ids = []
    for sqs_record in event["Records"]:
        try:
            message_id = sqs_record["messageId"]
        except KeyError:
            print("Invalid event received: not an SQS record")

        try:
            handle_record(sqs_record)
        except InitialReceiveError:
            returned_message_ids.append(message_id)
        except LambdaInvocationError:
            returned_message_ids.append(message_id)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"Uncaught error for message ID {message_id}: {type(exc)}, {exc}")
            returned_message_ids.append(message_id)

    # Report messages for which the timeout has changed as failures,
    # so that they are put back onto the queue.
    return {
        "batchItemFailures": [
            {"itemIdentifier": identifier} for identifier in returned_message_ids
        ]
    }


def handle_record(record: dict) -> None:
    """Handle a SQS single record."""
    # Retrieve the ApproximateReceiveCount from the SQS Retry Queue
    sqs_approximate_receive_count = int(record["attributes"]["ApproximateReceiveCount"])
    # If ApproximateReceiveCount is equal or lower than 1 (defensive), this is the first time we
    # fetched it from SQS, and it should be put back with a visibility timeout
    first_receive_from_sqs = sqs_approximate_receive_count <= 1

    # Retrieve the message from SQS
    sqs_body = json.loads(record["body"])
    # Get the event the Lambda Function was called with
    lambda_function_payload = sqs_body["requestPayload"]

    if "_retry_metadata" not in lambda_function_payload:
        # If `_retry_metadata` is not present, this is the first time the
        # Lambda Function failed, and we're currently processing the first
        # retry.
        event_retry_count = 1
        original_event_timestamp = int(
            datetime.timestamp(
                datetime.strptime(sqs_body["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
            )
        )
    else:
        # If `_retry_metadata` is present, this is the second or later time the
        # Lambda Function failed. We can get the retry count from the metadata.
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

    # Update the payload and call the Lambda Function again
    retry_lambda_execution(lambda_function_payload, original_event_timestamp)


def move_message_to_dlq(lambda_function_payload, reason):
    """Send a message to the DLQ when retries have been exhausted or another error occurred."""
    original_payload = lambda_function_payload
    if "_retry_metadata" in lambda_function_payload:
        original_payload = lambda_function_payload["_original_payload"]

    sqs_client.send_message(
        QueueUrl=dlq_url,
        MessageBody=json.dumps(original_payload),
        MessageAttributes={"Reason": {"StringValue": reason, "DataType": "String"}},
    )


def return_sqs_message_with_backoff(record: dict, event_retry_count: int) -> None:
    """Calculate the backoff, apply jitter and set the visibility timeout of a message."""
    # Example 1: base_backoff = 1, event_retry_count = 1
    # visibility_timeout = 1 * 2 ^ (1-1) = 1 * 2^0 = 1 * 1 = 1 second
    # Example 2: base_backoff = 1, event_retry_count = 2
    # visibility_timeout = 1 * 2 ^ (2-1) = 1 * 2^1 = 1 * 2 = 2 seconds
    # Example 3: base_backoff = 1, event_retry_count = 3
    # visibility_timeout = 1 * 2 ^ (3-1) = 1 * 2^2 = 1 * 4 = 4 seconds
    visibility_timeout = event_base_backoff * 2 ** (event_retry_count - 1)

    # Add jitter by selecting a random value between the base backoff (generally 1)
    # and the visibility timeout generated above.
    timeout_with_jitter = round(random.uniform(event_base_backoff, visibility_timeout))

    # Never exceed MAX_VISIBILITY_TIMEOUT
    ## The time the message was initially received by SQS, in ms
    initial_sent_to_sqs_timestamp_ms = int(record["attributes"]["SentTimestamp"])
    ## The time the message was initially received by SQS, in seconds
    initial_sent_to_sqs_timestamp_seconds = int(initial_sent_to_sqs_timestamp_ms / 1000)
    ## The very last timestamp the message can be received from the queue
    max_visibility_time = initial_sent_to_sqs_timestamp_seconds + MAX_VISIBILITY_TIMEOUT

    # The seconds until the max_visibility_time
    seconds_until_timeout = max_visibility_time - int(time.time())
    timeout_capped = min(seconds_until_timeout, timeout_with_jitter)

    # Change the visibility of the SQS message
    sqs_client.change_message_visibility(
        QueueUrl=retry_queue_url,
        ReceiptHandle=record["receiptHandle"],
        VisibilityTimeout=timeout_capped,
    )


def retry_lambda_execution(
    lambda_function_payload: dict, original_event_timestamp: int
) -> None:
    """Wrap the event in a retry envelope and send it to the async Lambda Function again."""
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
    except Exception as exc:  # pylint: disable=broad-except
        # Raising a LambdaInvocationError will put the message back onto the queue,
        # allowing it to be retried later.
        raise LambdaInvocationError("Lambda invocation failed") from exc

    lambda_function_status_code = response["StatusCode"]
    if lambda_function_status_code != 202:
        # Raising a LambdaInvocationError will put the message back onto the queue, allowing
        # it to be retried later.
        raise LambdaInvocationError(
            f"Invalid response from Lambda invocation: {lambda_function_status_code}"
        )
