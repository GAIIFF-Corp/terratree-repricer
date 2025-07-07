import { Stack, StackProps, Duration } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';

export class SpapiPollerStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Reference existing DynamoDB table
    const productsTable = dynamodb.Table.fromTableName(this, 'ProductsTable', 'terratree-products');

    // SP-API Poller Lambda
    const pollerLambda = new lambda.Function(this, 'SpapiPollerHandler', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'spapi_poller.lambda_handler',
      code: lambda.Code.fromAsset('lambda'),
      environment: {
        DYNAMODB_TABLE: 'terratree-products',
        SPAPI_ACCESS_TOKEN: 'REPLACE_WITH_TOKEN',
        MARKETPLACE_ID: 'ATVPDKIKX0DER'
      },
      timeout: Duration.minutes(5),
      memorySize: 512
    });

    // Grant DynamoDB access
    productsTable.grantReadWriteData(pollerLambda);

    // Schedule hourly execution
    const hourlyRule = new events.Rule(this, 'HourlyPollerTrigger', {
      schedule: events.Schedule.rate(Duration.hours(1))
    });
    
    hourlyRule.addTarget(new targets.LambdaFunction(pollerLambda));
  }
}