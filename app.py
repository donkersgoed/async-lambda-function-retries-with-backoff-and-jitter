#!/usr/bin/env python3
"""The main app. Contains all the stacks."""

# Standard library imports
# -

# Third party imports
# -

# Local application/library specific imports
from aws_cdk import core as cdk
from async_lambda_function_retries_with_backoff_and_jitter.async_lambda_function_retries_with_backoff_and_jitter_stack import (
    AsyncLambdaFunctionRetriesWithBackoffAndJitterStack,
)

app = cdk.App()
AsyncLambdaFunctionRetriesWithBackoffAndJitterStack(
    scope=app,
    construct_id="AsyncLambdaFunctionRetriesWithBackoffAndJitterStack",
)

app.synth()
