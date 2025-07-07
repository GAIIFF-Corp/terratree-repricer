import { Stack, StackProps, Duration } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';

export class PriceUpdateLambdaStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Reference secrets
    const dbSecret = secretsmanager.Secret.fromSecretNameV2(this, 'DatabaseSecret', 'terratree/production_db');
    const lwaAppIdSecret = secretsmanager.Secret.fromSecretNameV2(this, 'LwaAppIdSecret', 'lwa_app_id');
    const lwaClientSecret = secretsmanager.Secret.fromSecretNameV2(this, 'LwaClientSecret', 'lwa_client_secret');
    const refreshTokenSecret = secretsmanager.Secret.fromSecretNameV2(this, 'RefreshTokenSecret', 'refresh_token');

    // Create DynamoDB table
    const productsTable = new dynamodb.Table(this, 'ProductsTable', {
      tableName: 'terratree-products',
      partitionKey: { name: 'asin', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'marketplace_id', type: dynamodb.AttributeType.STRING }
    });

    // Define the Lambda function
    const priceLambda = new lambda.Function(this, 'PriceUpdateHandler', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'price_update_handler.lambda_handler',
      code: lambda.Code.fromAsset('lambda'),
      environment: {
        DYNAMODB_TABLE: 'terratree-products',
        MARKUP_PERCENTAGE: '15',
        MARKETPLACE_ID: 'ATVPDKIKX0DER',
        DB_SECRET_ARN: dbSecret.secretArn,
        LWA_APP_ID_SECRET_ARN: lwaAppIdSecret.secretArn,
        LWA_CLIENT_SECRET_ARN: lwaClientSecret.secretArn,
        REFRESH_TOKEN_SECRET_ARN: refreshTokenSecret.secretArn
      },
      timeout: Duration.seconds(30),
      memorySize: 256
    });

    // Grant DynamoDB and Secrets Manager access
    productsTable.grantWriteData(priceLambda);
    dbSecret.grantRead(priceLambda);
    lwaAppIdSecret.grantRead(priceLambda);
    lwaClientSecret.grantRead(priceLambda);
    refreshTokenSecret.grantRead(priceLambda);

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
