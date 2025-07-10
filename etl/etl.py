import sys
import json
import boto3
import pymysql
import logging
from decimal import Decimal
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


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
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "DB_SECRET_ARN", "DYNAMODB_TABLE"])
    
    # Initialize Spark
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
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

        # Read data using Spark JDBC
        df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:mysql://{credentials['host']}:3306/{credentials.get('dbname', 'terratree-production')}") \
            .option("user", credentials["username"]) \
            .option("password", credentials["password"]) \
            .option("query", """
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
            """) \
            .load()
        
        # Add marketplace_id column
        df = df.withColumn("marketplace_id", lit("ATVPDKIKX0DER"))
        
        logger.info(f"Loaded {df.count()} records from database")
        
        # Convert to DynamicFrame and write to DynamoDB
        dynf = DynamicFrame.fromDF(df, glueContext, "dframe")
        
        glueContext.write_dynamic_frame.from_options(
            frame=dynf,
            connection_type="dynamodb",
            connection_options={
                "dynamodb.output.tableName": dynamodb_table,
                "dynamodb.throughput.write.percent": "1.0"
            }
        )
        
        logger.info("Successfully wrote all records to DynamoDB using Glue DynamicFrame")

        job.commit()

    except Exception as e:
        logger.exception(f"Job failed with error: {str(e)}")
    finally:
        logger.info("ETL job completed")
        if 'sc' in locals():
            sc.stop()


if __name__ == "__main__":
    main()
