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

    // Reference secrets and DynamoDB table
    const dbSecret = secretsmanager.Secret.fromSecretNameV2(this, 'DatabaseSecret', 'terratree/production_db');
    const lwaAppIdSecret = secretsmanager.Secret.fromSecretNameV2(this, 'LwaAppIdSecret', 'lwa_app_id');
    const lwaClientSecret = secretsmanager.Secret.fromSecretNameV2(this, 'LwaClientSecret', 'lwa_client_secret');
    const refreshTokenSecret = secretsmanager.Secret.fromSecretNameV2(this, 'RefreshTokenSecret', 'refresh_token');
    const productsTable = dynamodb.Table.fromTableName(this, 'ProductsTable', 'terratree-products');

    // SP-API Poller Lambda
    const pollerLambda = new lambda.Function(this, 'SpapiPollerHandler', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'spapi_poller.lambda_handler',
      code: lambda.Code.fromAsset('lambda'),
      environment: {
        DYNAMODB_TABLE: 'terratree-products',
        MARKETPLACE_ID: 'ATVPDKIKX0DER',
        DB_SECRET_ARN: dbSecret.secretArn,
        LWA_APP_ID_SECRET_ARN: lwaAppIdSecret.secretArn,
        LWA_CLIENT_SECRET_ARN: lwaClientSecret.secretArn,
        REFRESH_TOKEN_SECRET_ARN: refreshTokenSecret.secretArn
      },
      timeout: Duration.minutes(5),
      memorySize: 512
    });

    // Grant DynamoDB and Secrets Manager access
    productsTable.grantReadWriteData(pollerLambda);
    dbSecret.grantRead(pollerLambda);
    lwaAppIdSecret.grantRead(pollerLambda);
    lwaClientSecret.grantRead(pollerLambda);
    refreshTokenSecret.grantRead(pollerLambda);

    // Schedule hourly execution
    const hourlyRule = new events.Rule(this, 'HourlyPollerTrigger', {
      schedule: events.Schedule.rate(Duration.hours(1))
    });
    
    hourlyRule.addTarget(new targets.LambdaFunction(pollerLambda));
  }
}