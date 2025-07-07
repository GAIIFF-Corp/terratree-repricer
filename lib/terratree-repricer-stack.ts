import { Stack, StackProps, Duration } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { PriceUpdateLambdaStack } from './price-update-lambda-stack';
import { SpapiPollerStack } from './spapi-poller-stack';

export class TerratreeRepricerStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Reference database secrets
    const dbSecret = secretsmanager.Secret.fromSecretNameV2(this, 'DatabaseSecret', 'terratree/production_db');

    // Reference existing S3 bucket for Glue script
    const scriptBucket = s3.Bucket.fromBucketName(this, 'GlueScriptBucket', 'terratreepricing');

    // IAM role for Glue
    const glueRole = new iam.Role(this, 'GlueJobRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
      ],
    });
    
    // Grant Secrets Manager access to Glue role
    dbSecret.grantRead(glueRole);

    // Glue job
    const glueJob = new glue.CfnJob(this, 'TerratreeGlueJob', {
      name: 'terratree-etl-job',
      role: glueRole.roleArn,
      command: {
        name: 'glueetl',
        scriptLocation: 's3://terratreepricing/etl/etl.py',
        pythonVersion: '3',
      },
      defaultArguments: {
        '--TempDir': `s3://${scriptBucket.bucketName}/temp/`,
        '--job-language': 'python',
        '--extra-py-files': '',
        '--DB_SECRET_ARN': dbSecret.secretArn,
        '--DYNAMODB_TABLE': 'terratree-products'
      },
      glueVersion: '4.0',
      maxRetries: 0,
      timeout: 30,
      numberOfWorkers: 2,
      workerType: 'G.1X'
    });

    // EventBridge rule to schedule job daily
    const glueRule = new events.Rule(this, 'DailyGlueTrigger', {
      schedule: events.Schedule.cron({ minute: '0', hour: '2' })
    });
    
    glueRule.addTarget(new targets.AwsApi({
      service: 'Glue',
      action: 'startJobRun',
      parameters: {
        JobName: glueJob.name!,
      },
      policyStatement: new iam.PolicyStatement({
        actions: ['glue:StartJobRun'],
        resources: ['*'],
      }),
    }));

    // Add the Price Update Lambda Stack
    new PriceUpdateLambdaStack(this, 'PriceUpdateLambda');
    
    // Add the SP-API Poller Lambda Stack
    new SpapiPollerStack(this, 'SpapiPoller');
  }
}

