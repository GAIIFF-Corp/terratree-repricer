import sys
import os
import json
import boto3
import pymysql
from decimal import Decimal
from awsglue.utils import getResolvedOptions

# Get Glue job arguments
args = getResolvedOptions(sys.argv, ['DB_SECRET_ARN', 'DYNAMODB_TABLE'])
DYNAMODB_TABLE = args['DYNAMODB_TABLE']
DB_SECRET_ARN = args['DB_SECRET_ARN']

def get_db_secrets():
    """Retrieve database secrets from AWS Secrets Manager"""
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=DB_SECRET_ARN)
    return json.loads(response['SecretString'])

# Get database credentials from Secrets Manager
secrets = get_db_secrets()

# Connect to RDS
connection = pymysql.connect(
    host=secrets['host'],
    user=secrets['username'],
    password=secrets['password'],
    database=secrets.get('database', 'terratree-production'),
    cursorclass=pymysql.cursors.DictCursor
)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(DYNAMODB_TABLE)

def lambda_handler(event=None, context=None):
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT 
                    t.product_id AS asin,
                    t.retail_price,
                    z.min_price,
                    z.max_price,
                    z.business_price,
                    z.sales_price AS currentPrice
                FROM TerratreeProductsUSA t
                LEFT JOIN ZoroFeedNew z 
                    ON TRIM(LOWER(t.seller_sku)) = TRIM(LOWER(z.sku))
                WHERE t.product_id IS NOT NULL;
            """
            cursor.execute(query)
            rows = cursor.fetchall()

            # Process in batches of 25 (DynamoDB batch limit)
            batch_size = 25
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                
                with table.batch_writer() as batch_writer:
                    for row in batch:
                        try:
                            item = {
                                'asin': str(row['asin']),
                                'marketplace_id': 'ATVPDKIKX0DER',
                                'retail_price': Decimal(str(row['retail_price'])) if row['retail_price'] is not None else Decimal('0.0'),
                                'min_price': Decimal(str(row['min_price'])) if row['min_price'] is not None else Decimal('0.0'),
                                'max_price': Decimal(str(row['max_price'])) if row['max_price'] is not None else Decimal('0.0'),
                                'business_price': Decimal(str(row['business_price'])) if row['business_price'] is not None else Decimal('0.0'),
                                'currentPrice': Decimal(str(row['currentPrice'])) if row['currentPrice'] is not None else Decimal('0.0'),
                            }
                            batch_writer.put_item(Item=item)
                        except Exception as e:
                            print(f"Error writing item {row['asin']}: {e}")
                
                print(f"Processed batch {i//batch_size + 1}/{(len(rows) + batch_size - 1)//batch_size}")

            print(f"Successfully synced {len(rows)} items to DynamoDB.")
    except Exception as e:
        print(f"Error during ETL: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    lambda_handler()
