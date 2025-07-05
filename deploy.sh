#!/bin/bash

# Terratree Repricer CDK Deployment Script

echo "Building TypeScript..."
npm run build

if [ $? -ne 0 ]; then
    echo "Build failed. Exiting."
    exit 1
fi

echo "Synthesizing CDK stack..."
npx cdk synth

if [ $? -ne 0 ]; then
    echo "CDK synth failed. Exiting."
    exit 1
fi

echo "Deploying CDK stack..."
npx cdk deploy --require-approval never

echo "Deployment complete!"