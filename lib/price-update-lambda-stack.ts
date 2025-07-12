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
    const spapiSecret = secretsmanager.Secret.fromSecretNameV2(this, 'SpapiSecret', 'terratreeOrders/spapi');

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
        DB_SECRET_ARN: dbSecret.secretArn
      },
      timeout: Duration.seconds(30),
      memorySize: 256
    });

    // Grant DynamoDB and Secrets Manager access
    productsTable.grantReadWriteData(priceLambda);
    dbSecret.grantRead(priceLambda);
    spapiSecret.grantRead(priceLambda);

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
