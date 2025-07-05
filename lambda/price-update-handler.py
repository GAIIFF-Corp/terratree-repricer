import os
from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_ssm as ssm,
)
from constructs import Construct

class PriceUpdateLambdaStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Define the Lambda function
        price_lambda = lambda_.Function(
            self, "PriceUpdateHandler",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="price_update_handler.lambda_handler",
            code=lambda_.Code.from_asset("lambda"),  # expects lambda/price_update_handler.py
            environment={
                "DYNAMODB_TABLE": "terratree-products",
                "SPAPI_ACCESS_TOKEN": "REPLACE_WITH_TOKEN",  # Ideally fetched via Secrets Manager or SSM
                "MARKUP_PERCENTAGE": "15",
                "SELLER_ID": "REPLACE_WITH_SELLER_ID",
                "MARKETPLACE_ID": "ATVPDKIKX0DER"
            },
            timeout=cdk.Duration.seconds(30),
            memory_size=256
        )

        # Grant DynamoDB access
        table_arn = f"arn:aws:dynamodb:{self.region}:{self.account}:table/terratree-products"
        price_lambda.add_to_role_policy(iam.PolicyStatement(
            actions=["dynamodb:UpdateItem"],
            resources=[table_arn]
        ))

        # Allow Lambda to be triggered by EventBridge
        event_rule = events.Rule(
            self, "PriceChangeEventRule",
            event_pattern=events.EventPattern(
                source=["aws.partner/sellingpartnerapi.amazon.com"],
                detail_type=["ANY_OFFER_CHANGED"]
            )
        )
        event_rule.add_target(targets.LambdaFunction(price_lambda))
