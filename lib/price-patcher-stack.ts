import { Stack, StackProps, Duration } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';

export class PricePatcherStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Reference secrets and DynamoDB table
    const dbSecret = secretsmanager.Secret.fromSecretNameV2(this, 'DatabaseSecret', 'terratree/production_db');
    const spapiSecret = secretsmanager.Secret.fromSecretNameV2(this, 'SpapiSecret', 'terratreeOrders/spapi');
    const productsTable = dynamodb.Table.fromTableName(this, 'ProductsTable', 'terratree-products');

    // Price Patcher Lambda
    const patcherLambda = new lambda.Function(this, 'PricePatcherHandler', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'price_patcher.lambda_handler',
      code: lambda.Code.fromAsset('lambda'),
      environment: {
        DYNAMODB_TABLE: 'terratree-products',
        MARKETPLACE_ID: 'ATVPDKIKX0DER',
        DB_SECRET_ARN: dbSecret.secretArn
      },
      timeout: Duration.minutes(5),
      memorySize: 512
    });

    // Grant DynamoDB and Secrets Manager access
    productsTable.grantReadWriteData(patcherLambda);
    dbSecret.grantRead(patcherLambda);
    spapiSecret.grantRead(patcherLambda);

    // Schedule hourly execution
    const hourlyRule = new events.Rule(this, 'HourlyPatcherTrigger', {
      schedule: events.Schedule.rate(Duration.hours(1))
    });
    
    hourlyRule.addTarget(new targets.LambdaFunction(patcherLambda));
  }
}