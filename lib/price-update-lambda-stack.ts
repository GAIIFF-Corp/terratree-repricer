import { Stack, StackProps, Duration } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';

export class PriceUpdateLambdaStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Define the Lambda function
    const priceLambda = new lambda.Function(this, 'PriceUpdateHandler', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'price_update_handler.lambda_handler',
      code: lambda.Code.fromAsset('lambda'),
      environment: {
        DYNAMODB_TABLE: 'terratree-products',
        SPAPI_ACCESS_TOKEN: 'REPLACE_WITH_TOKEN', // Use Secrets Manager in production
        MARKUP_PERCENTAGE: '15',
        SELLER_ID: 'REPLACE_WITH_SELLER_ID',
        MARKETPLACE_ID: 'ATVPDKIKX0DER'
      },
      timeout: Duration.seconds(30),
      memorySize: 256
    });

    // Grant DynamoDB access
    const tableArn = `arn:aws:dynamodb:${this.region}:${this.account}:table/terratree-products`;
    priceLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['dynamodb:UpdateItem'],
      resources: [tableArn]
    }));

    // Allow Lambda to be triggered by EventBridge
    const eventRule = new events.Rule(this, 'PriceChangeEventRule', {
      eventPattern: {
        source: ['aws.partner/sellingpartnerapi.amazon.com'],
        detailType: ['ANY_OFFER_CHANGED']
      }
    });
    eventRule.addTarget(new targets.LambdaFunction(priceLambda));
  }
}
