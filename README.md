# Terratree Repricer CDK Project

This CDK project deploys infrastructure for automated Amazon product repricing using AWS Glue ETL jobs and Lambda functions.

## Architecture

- **Glue ETL Job**: Processes product data daily at 2 AM UTC
- **Lambda Function**: Handles real-time price updates from Amazon SP-API events
- **EventBridge**: Triggers Lambda on price change events
- **DynamoDB**: Stores product pricing information

## Prerequisites

- AWS CLI configured with appropriate permissions
- Node.js and npm installed
- CDK CLI installed (`npm install -g aws-cdk`)

## Configuration

Before deployment, update the following environment variables in the Lambda function:

- `SPAPI_ACCESS_TOKEN`: Your Amazon SP-API access token
- `SELLER_ID`: Your Amazon seller ID
- `MARKETPLACE_ID`: Target marketplace (default: ATVPDKIKX0DER for US)
- `MARKUP_PERCENTAGE`: Pricing markup percentage (default: 15%)

## Deployment

### Quick Deploy
```bash
./deploy.sh
```

### Manual Deploy
```bash
npm install
npm run build
npx cdk deploy
```

## Useful Commands

* `npm run build`   - compile typescript to js
* `npm run watch`   - watch for changes and compile
* `npm run test`    - perform the jest unit tests
* `npx cdk deploy`  - deploy this stack to your default AWS account/region
* `npx cdk diff`    - compare deployed stack with current state
* `npx cdk synth`   - emits the synthesized CloudFormation template
* `npx cdk destroy` - destroy the deployed stack

## Project Structure

```
├── bin/
│   └── terratree-repricer.ts     # CDK app entry point
├── lib/
│   ├── terratree-repricer-stack.ts      # Main stack with Glue job
│   └── price-update-lambda-stack.ts     # Lambda stack for price updates
├── lambda/
│   ├── price_update_handler.py          # Lambda function code
│   └── requirements.txt                 # Python dependencies
├── etl/
│   └── etl.py                          # Glue ETL script
└── test/
    └── terratree-repricer.test.ts      # Unit tests
```
## Repricing logic:

1. Check if we're already featured - Exit early if our seller ID (AERPN1UM8O1I4) is already the featured offer

2. Price 1 cent below featured offer - Set regular price to featured offer price minus $0.01

3. Business price 1 cent below regular - Set business price to regular price minus $0.01

4. Respect min/max constraints - Only reprice if above minimum price and below maximum price