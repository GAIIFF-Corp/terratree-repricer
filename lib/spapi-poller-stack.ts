import { Stack, StackProps, Duration } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';

export class SpapiPollerStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Reference database secrets and DynamoDB table
    const dbSecret = secretsmanager.Secret.fromSecretNameV2(this, 'DatabaseSecret', 'terratree/production_db');
    const productsTable = dynamodb.Table.fromTableName(this, 'ProductsTable', 'terratree-products');

    // SP-API Poller Lambda
    const pollerLambda = new lambda.Function(this, 'SpapiPollerHandler', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'spapi_poller.lambda_handler',
      code: lambda.Code.fromAsset('lambda'),
      environment: {
        DYNAMODB_TABLE: 'terratree-products',
        SPAPI_ACCESS_TOKEN: 'REPLACE_WITH_TOKEN',
        MARKETPLACE_ID: 'ATVPDKIKX0DER',
        DB_SECRET_ARN: dbSecret.secretArn
      },
      timeout: Duration.minutes(5),
      memorySize: 512
    });

    // Grant DynamoDB and Secrets Manager access
    productsTable.grantReadWriteData(pollerLambda);
    dbSecret.grantRead(pollerLambda);

    // Schedule hourly execution
    const hourlyRule = new events.Rule(this, 'HourlyPollerTrigger', {
      schedule: events.Schedule.rate(Duration.hours(1))
    });
    
    hourlyRule.addTarget(new targets.LambdaFunction(pollerLambda));
  }
}