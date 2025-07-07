import json
import os
import boto3
import pymysql
from functools import lru_cache

@lru_cache(maxsize=1)
def get_db_secrets():
    """Retrieve database secrets from AWS Secrets Manager"""
    secret_arn = os.environ['DB_SECRET_ARN']
    
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_arn)
    
    return json.loads(response['SecretString'])

def get_db_connection():
    """Create database connection using secrets from Secrets Manager"""
    secrets = get_db_secrets()
    
    return pymysql.connect(
        host=secrets['host'],
        database=secrets.get('database', 'terratree-production'),
        user=secrets['username'],
        password=secrets['password'],
        port=secrets.get('port', 3306),
        cursorclass=pymysql.cursors.DictCursor
    )