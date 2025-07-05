import os
import boto3
import pymysql
from decimal import Decimal

# Environment Variables
RDS_HOST = os.environ['RDS_HOST']
RDS_USER = os.environ['RDS_USER']
RDS_PASSWORD = os.environ['RDS_PASSWORD']
RDS_DB = os.environ['RDS_DB']
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE', 'terratree-products')

# Connect to RDS
connection = pymysql.connect(
    host=RDS_HOST,
    user=RDS_USER,
    password=RDS_PASSWORD,
    database=RDS_DB,
    cursorclass=pymysql.cursors.DictCursor
)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(DYNAMODB_TABLE)

def lambda_handler(event=None, context=None):
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT t.product_id AS asin,
                       t.retail_price,
                       z.min_price,
                       z.max_price,
                       z.sales_price AS currentPrice
                FROM TerratreeProductsUSA t
                JOIN ZoroFeedNew z ON t.product_id = z.sku
                WHERE t.product_id IS NOT NULL AND z.sku IS NOT NULL
            """
            cursor.execute(query)
            rows = cursor.fetchall()

            for row in rows:
                item = {
                    'asin': row['asin'],
                    'retail_price': Decimal(str(row['retail_price'])) if row['retail_price'] is not None else Decimal('0.0'),
                    'min_price': Decimal(str(row['min_price'])) if row['min_price'] is not None else Decimal('0.0'),
                    'max_price': Decimal(str(row['max_price'])) if row['max_price'] is not None else Decimal('0.0'),
                    'currentPrice': Decimal(str(row['currentPrice'])) if row['currentPrice'] is not None else Decimal('0.0'),
                }
                table.put_item(Item=item)

            print(f"Successfully synced {len(rows)} items to DynamoDB.")
    except Exception as e:
        print(f"Error during ETL: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    lambda_handler()
