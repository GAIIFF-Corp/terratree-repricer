import json
import os
import boto3
from functools import lru_cache

@lru_cache(maxsize=3)
def get_secret_value(secret_arn):
    """Retrieve secret value from AWS Secrets Manager"""
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_arn)
    return response['SecretString']

def get_spapi_credentials():
    """Get SP-API credentials from Secrets Manager"""
    return {
        'lwa_app_id': get_secret_value(os.environ['LWA_APP_ID_SECRET_ARN']),
        'lwa_client_secret': get_secret_value(os.environ['LWA_CLIENT_SECRET_ARN']),
        'refresh_token': get_secret_value(os.environ['REFRESH_TOKEN_SECRET_ARN'])
    }