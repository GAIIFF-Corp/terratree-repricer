import json
import os
import boto3
from functools import lru_cache

@lru_cache(maxsize=1)
def get_spapi_credentials():
    """Get SP-API credentials from Secrets Manager"""
    try:
        client = boto3.client('secretsmanager')
        response = client.get_secret_value(SecretId='terratreeOrders/spapi')
        secrets = json.loads(response['SecretString'])
        
        return {
            'lwa_app_id': secrets['lwa_app_id'],
            'lwa_client_secret': secrets['lwa_client_secret'],
            'refresh_token': secrets['refresh_token']
        }
    except Exception as e:
        print(f"Error getting SP-API credentials from terratreeOrders/spapi: {str(e)}")
        raise