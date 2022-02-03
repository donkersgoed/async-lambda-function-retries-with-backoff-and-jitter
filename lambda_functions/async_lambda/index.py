"""The Async Lambda Function."""

import time

# To invoke this function from a MacOS terminal:
# aws lambda invoke --cli-binary-format raw-in-base64-out \
#   --function-name <function_name> --invocation-type Event --payload "{\"UUID\": \"`uuidgen`\"}" /dev/null


def event_handler(event, _context):
    """Receive an async event, print its ID and raise an error."""
    attempt = 1
    original_payload = event
    if "_retry_metadata" in event:
        original_payload = event["_original_payload"]
        attempt = event["_retry_metadata"]["attempt"] + 1

    uuid = original_payload["UUID"]
    now = int(time.time())
    print(f"UUID: {uuid}, Time: {now}, Attempt: {attempt}")
    raise RuntimeError("This function will always fail")
