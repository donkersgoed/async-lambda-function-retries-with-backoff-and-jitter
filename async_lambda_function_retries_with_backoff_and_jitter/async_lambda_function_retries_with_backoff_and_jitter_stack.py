"""Module for the main AsyncLambdaFunctionRetriesWithBackoffAndJitter Stack."""

# Standard library imports
# -

# Third party imports
from aws_cdk import (
    core as cdk,
    aws_lambda as lambda_,
    aws_lambda_destinations as lambda_destinations,
    aws_sqs as sqs,
)

# Local application/library specific imports
from async_lambda_function_retries_with_backoff_and_jitter.lambda_function import (
    LambdaFunction,
)


class AsyncLambdaFunctionRetriesWithBackoffAndJitterStack(cdk.Stack):
    """The AsyncLambdaFunctionRetriesWithBackoffAndJitter Stack."""

    def __init__(
        self,
        scope: cdk.Construct,
        construct_id: str,
        **kwargs,
    ) -> None:
        """Construct a new AsyncLambdaFunctionRetriesWithBackoffAndJitterStack."""
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here

        async_lambda_function = LambdaFunction(
            scope=self,
            construct_id="AsyncLambda",
            code=lambda_.Code.from_asset("lambda_functions/async_lambda"),
        )

        retry_queue = sqs.Queue(scope=self, id="RetryQueue")

        async_lambda_function.function.configure_async_invoke(
            retry_attempts=0,
            on_failure=lambda_destinations.SqsDestination(queue=retry_queue),
        )

        retry_handler = LambdaFunction(
            scope=self,
            construct_id="RetryHandler",
            code=lambda_.Code.from_asset("lambda_functions/retry_handler"),
            environment={
                "ASYNC_FUNCTION_NAME": async_lambda_function.function.function_name,
                "RETRY_QUEUE_URL": retry_queue.queue_url,
                "MAX_AGE": "3600",  # "86400",  # One day
                "MAX_RETRIES": "185",
                "BASE_BACKOFF": "1",
            },
            memory_size=512,
        )
        retry_queue.grant_consume_messages(retry_handler.function)
        async_lambda_function.function.grant_invoke(retry_handler.function)

        retry_handler.function.add_event_source_mapping(
            id="RetryEventSourceMapping",
            max_batching_window=cdk.Duration.seconds(1),
            event_source_arn=retry_queue.queue_arn,
            report_batch_item_failures=True,
        )
