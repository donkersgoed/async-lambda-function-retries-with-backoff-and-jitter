"""Module for the Lambda Function L3 Pattern."""


# Third party imports
from aws_cdk import (
    core as cdk,
    aws_iam as iam,
    aws_logs as logs,
    aws_lambda as lambda_,
)


class LambdaFunction(cdk.Construct):
    """CDK Construct for a Lambda Function and its supporting resources."""

    def __init__(
        self,
        scope: cdk.Construct,
        construct_id: str,
        code: lambda_.Code,
        environment: dict = None,
        memory_size: int = 128,
        **kwargs,
    ) -> None:
        """Construct a new LambdaFunction."""
        super().__init__(scope, construct_id, **kwargs)

        if not environment:
            environment = {}

        # Create a role for the Lambda Function
        function_role = iam.Role(
            scope=self,
            id="Role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )

        # Create the Lambda Function
        self.function = lambda_.Function(
            scope=self,
            architecture=lambda_.Architecture.ARM_64,
            id="Function",
            role=function_role,
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=code,
            handler="index.event_handler",
            environment=environment,
            memory_size=memory_size,
        )

        # Create the Lambda Function Log Group
        function_log_group = logs.LogGroup(
            scope=self,
            id="LogGroup",
            retention=logs.RetentionDays.ONE_MONTH,
            log_group_name=cdk.Fn.sub(
                "/aws/lambda/${Function}",
                {"Function": self.function.function_name},
            ),
        )

        # Give Lambda permission to write log streams, but not to create log groups
        log_policy = iam.Policy(
            scope=self,
            id="LogPolicy",
            document=iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        actions=["logs:PutLogEvents", "logs:CreateLogStream"],
                        effect=iam.Effect.ALLOW,
                        resources=[function_log_group.log_group_arn],
                    ),
                ]
            ),
        )
        log_policy.attach_to_role(function_role)
