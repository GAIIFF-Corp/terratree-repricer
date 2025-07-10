import sys
import json
import boto3
import pymysql
import logging
from decimal import Decimal
from awsglue.utils import getResolvedOptions


# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def get_db_connection(credentials):
    """Establish MySQL connection with timeout handling"""
    try:
        conn = pymysql.connect(
            host=credentials["host"],
            user=credentials["username"],
            password=credentials["password"],
            database=credentials.get("dbname", "terratree-production"),
            connect_timeout=5,
            read_timeout=30,
            cursorclass=pymysql.cursors.SSDictCursor,
        )
        logger.info("Successfully connected to MySQL database")
        return conn
    except pymysql.Error as e:
        logger.error(f"MySQL connection failed: {e}")
        raise


def main():
    args = getResolvedOptions(sys.argv, ["DB_SECRET_ARN", "DYNAMODB_TABLE"])
    db_secret_arn = args["DB_SECRET_ARN"]
    dynamodb_table = args["DYNAMODB_TABLE"]

    logger.info("Starting ETL job")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"PyMySQL version: {pymysql.__version__}")
    logger.info(f"Boto3 version: {boto3.__version__}")

    try:
        # Initialize AWS clients
        secrets_client = boto3.client("secretsmanager")
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(dynamodb_table)

        # Fetch database credentials
        secret = secrets_client.get_secret_value(SecretId=db_secret_arn)
        credentials = json.loads(secret["SecretString"])
        logger.info("Retrieved database credentials from Secrets Manager")

        # Connect to MySQL
        conn = get_db_connection(credentials)

        # Batch processing parameters
        batch_size = 25
        total_processed = 0

        with conn.cursor() as cursor:
            query = """
            SELECT
                t.product_id AS asin,
                COALESCE(t.retail_price, 0.0) AS retail_price,
                COALESCE(z.min_price, 0.0) AS min_price,
                COALESCE(z.max_price, 0.0) AS max_price,
                COALESCE(z.business_price, 0.0) AS business_price,
                COALESCE(z.sales_price, 0.0) AS currentPrice
            FROM TerratreeProductsUSA t
            LEFT JOIN ZoroFeedNew z
                ON LOWER(TRIM(t.seller_sku)) = LOWER(TRIM(z.sku))
            WHERE t.product_id IS NOT NULL
            """
            logger.info(f"Executing query: {query}")
            cursor.execute(query)
            logger.info("Query executed successfully")

            while True:
                # Fetch rows in batches
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    logger.info("No more rows to process")
                    break

                # Process individual items
                for row in rows:
                    try:
                        item = {
                            "asin": str(row["asin"]),
                            "marketplace_id": "ATVPDKIKX0DER",
                            "retail_price": Decimal(str(row["retail_price"])),
                            "min_price": Decimal(str(row["min_price"])),
                            "max_price": Decimal(str(row["max_price"])),
                            "business_price": Decimal(str(row["business_price"])),
                            "currentPrice": Decimal(str(row["currentPrice"])),
                        }
                        table.put_item(Item=item)
                        total_processed += 1
                    except Exception as e:
                        logger.error(f"Error processing row: {row} - {str(e)}")

                logger.info(
                    f"Processed batch of {len(rows)} items. Total: {total_processed}"
                )

        logger.info(f"Successfully processed {total_processed} records")

    except Exception as e:
        logger.exception(f"Job failed with error: {str(e)}")
    finally:
        if "conn" in locals() and conn.open:
            conn.close()
            logger.info("MySQL connection closed")
        logger.info("ETL job completed")


if __name__ == "__main__":
    main()
