"""
POC: SAP HANA → S3 → Snowflake (Large Table Ready)

Best practices applied:
- HANA extraction with optional numeric/date partitioning
- Write Parquet to S3 (staging layer)
- Snowflake COPY INTO from S3
- Logging, secure credentials via AWS Secrets Manager
- Incremental load placeholder
"""

import logging, os, json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from snowflake.snowpark import Session
import boto3
from botocore.exceptions import ClientError

# -----------------------
# Logging
# -----------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hana-snowflake-poc")

# -----------------------
# AWS Secrets helper
# -----------------------
def get_secret(secret_name, region="us-east-1"):
    client = boto3.client("secretsmanager", region_name=region)
    try:
        resp = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logger.error(f"Unable to fetch secret {secret_name}: {e}")
        raise e
    return json.loads(resp["SecretString"])

# -----------------------
# Load HANA credentials
# -----------------------
HANA_SECRET = os.environ.get("HANA_SECRET_NAME", "hana/credentials")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
hana_creds = get_secret(HANA_SECRET, AWS_REGION)

HANA_HOST = hana_creds["host"]
HANA_PORT = hana_creds.get("port", "30015")
HANA_USER = hana_creds["username"]
HANA_PASSWORD = hana_creds["password"]

HANA_SCHEMA = os.environ.get("SRC_SCHEMA", "SALES")
HANA_TABLE = os.environ.get("SRC_TABLE", "ORDERS")
PARTITION_COL = os.environ.get("PARTITION_COL", "ORDER_DATE")  # numeric or date
LOAD_TYPE = os.environ.get("LOAD_TYPE", "full")  # full/incremental

# -----------------------
# S3 staging
# -----------------------
STAGING_PREFIX = os.environ.get("STAGING_PREFIX", "s3://mybucket/staging/")
TARGET_PREFIX = f"{STAGING_PREFIX}{HANA_SCHEMA}/{HANA_TABLE}/run_date={datetime.utcnow().strftime('%Y-%m-%d')}/"
TARGET_FILESIZE_MB = int(os.environ.get("PARTITION_FILESIZE_MB", "150")) * 1024 * 1024

# -----------------------
# Spark session
# -----------------------
spark = (
    SparkSession.builder
    .appName("hana-to-s3")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

jdbc_url = f"jdbc:sap://{HANA_HOST}:{HANA_PORT}"

# -----------------------
# Read HANA table
# -----------------------
logger.info(f"Reading HANA table {HANA_SCHEMA}.{HANA_TABLE}")

df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", f"{HANA_SCHEMA}.{HANA_TABLE}")
    .option("user", HANA_USER)
    .option("password", HANA_PASSWORD)
    .load()
)

logger.info(f"Extracted {df.count()} rows from HANA")

# -----------------------
# Write to S3 (Parquet)
# -----------------------
rows = df.count()
target_parts = max(1, int(rows / (TARGET_FILESIZE_MB / 1000)))
logger.info(f"Writing {rows} rows to S3 in ~{target_parts} partitions")

df.repartition(target_parts).write.mode("overwrite").parquet(TARGET_PREFIX)
logger.info(f"Wrote data to {TARGET_PREFIX}")

# -----------------------
# Load Snowflake credentials
# -----------------------
SF_SECRET = os.environ.get("SF_SECRET_NAME", "snowflake/connection")
sf_creds = get_secret(SF_SECRET, AWS_REGION)

conn_params = {
    "account": sf_creds["account"],
    "user": sf_creds["user"],
    "password": sf_creds["password"],
    "role": sf_creds.get("role", "ETL_ROLE"),
    "warehouse": sf_creds.get("warehouse", "ETL_WH"),
    "database": sf_creds.get("database", "DWH"),
    "schema": sf_creds.get("schema", "PUBLIC")
}

session = Session.builder.configs(conn_params).create()

# -----------------------
# COPY INTO Snowflake staging table
# -----------------------
STAGING_TABLE = os.environ.get("SF_STAGING_TABLE", "RAW.HANA_ORDERS_TEMP")
logger.info(f"Copying into Snowflake table {STAGING_TABLE}")

copy_sql = f"""
COPY INTO {STAGING_TABLE}
FROM '{TARGET_PREFIX}'
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE'
"""

session.sql(copy_sql).collect()
logger.info(f"Loaded data into Snowflake staging table {STAGING_TABLE}")

# -----------------------
# Validate row count
# -----------------------
cnt = session.table(STAGING_TABLE).count()
logger.info(f"Snowflake table {STAGING_TABLE} now has {cnt} rows")

# -----------------------
# Cleanup
# -----------------------
spark.stop()
session.close()
logger.info("POC completed successfully")
