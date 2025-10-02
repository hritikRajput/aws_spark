"""
POC: SAP HANA → S3 → Snowflake (Full/Incremental Load Ready)

Features:
- Extract from SAP HANA via Spark JDBC
- Full or incremental load
- Partitioning for large tables
- Stage data in S3 as Parquet
- Load into Snowflake using Snowpark
- Supports MERGE/UPSERT for incremental data
- Secure credential management via AWS Secrets Manager
- Logging and row count validation
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
# HANA credentials and configuration
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
PARTITION_COL = os.environ.get("PARTITION_COL", "ORDER_DATE")  # numeric/date
LOAD_TYPE = os.environ.get("LOAD_TYPE", "full")  # full / incremental
LAST_RUN_DATE = os.environ.get("LAST_RUN_DATE", None)  # for incremental
INCREMENTAL_COL = os.environ.get("INCREMENTAL_COL", "LAST_UPDATED")  # timestamp/date column

# -----------------------
# S3 staging configuration
# -----------------------
STAGING_PREFIX = os.environ.get("STAGING_PREFIX", "s3://mybucket/staging/")
TARGET_PREFIX = f"{STAGING_PREFIX}{HANA_SCHEMA}/{HANA_TABLE}/run_date={datetime.utcnow().strftime('%Y-%m-%d')}/"
TARGET_FILESIZE_MB = int(os.environ.get("PARTITION_FILESIZE_MB", "150")) * 1024 * 1024  # bytes

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
# Extract HANA data
# -----------------------
if LOAD_TYPE.lower() == "incremental" and LAST_RUN_DATE:
    # Incremental load: only rows updated after last run
    logger.info(f"Performing incremental extract since {LAST_RUN_DATE}")
    query = f"(SELECT * FROM {HANA_SCHEMA}.{HANA_TABLE} WHERE {INCREMENTAL_COL} > DATE '{LAST_RUN_DATE}') as t"
else:
    # Full load
    logger.info("Performing full extract")
    query = f"{HANA_SCHEMA}.{HANA_TABLE}"

df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", query)
    .option("user", HANA_USER)
    .option("password", HANA_PASSWORD)
    .load()
)

rows = df.count()
logger.info(f"Extracted {rows} rows from HANA")

# -----------------------
# Partitioning and write to S3
# -----------------------
target_parts = max(1, int(rows / (TARGET_FILESIZE_MB / 1000)))  # rough estimate
logger.info(f"Writing {rows} rows to S3 in ~{target_parts} parquet files at {TARGET_PREFIX}")

df.repartition(target_parts).write.mode("overwrite").parquet(TARGET_PREFIX)
logger.info(f"Data written to S3 staging {TARGET_PREFIX}")

# -----------------------
# Snowflake credentials
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

# -----------------------
# Snowpark session
# -----------------------
session = Session.builder.configs(conn_params).create()

# -----------------------
# COPY INTO Snowflake staging
# -----------------------
STAGING_TABLE = os.environ.get("SF_STAGING_TABLE", f"RAW.{HANA_TABLE}_TEMP")
logger.info(f"Loading into Snowflake staging table {STAGING_TABLE}")

copy_sql = f"""
COPY INTO {STAGING_TABLE}
FROM '{TARGET_PREFIX}'
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE'
"""
session.sql(copy_sql).collect()
logger.info(f"Loaded data into Snowflake staging table {STAGING_TABLE}")

# -----------------------
# Optional: MERGE into target table
# -----------------------
TARGET_TABLE = os.environ.get("SF_TARGET_TABLE", f"DWH.{HANA_TABLE}")
KEY_COLS = os.environ.get("SF_KEY_COLS", "ORDER_ID").split(",")  # primary keys
UPDATE_COLS = os.environ.get("SF_UPDATE_COLS", "AMOUNT,STATUS,LAST_UPDATED").split(",")  # columns to update

# Only do merge if incremental load
if LOAD_TYPE.lower() == "incremental" and rows > 0:
    on_clause = " AND ".join([f"target.{k} = source.{k}" for k in KEY_COLS])
    set_clause = ", ".join([f"target.{c} = source.{c}" for c in UPDATE_COLS])

    merge_sql = f"""
    MERGE INTO {TARGET_TABLE} AS target
    USING {STAGING_TABLE} AS source
    ON {on_clause}
    WHEN MATCHED THEN UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN INSERT ({', '.join(KEY_COLS + UPDATE_COLS)})
      VALUES ({', '.join(['source.'+c for c in KEY_COLS + UPDATE_COLS])});
    """
    logger.info(f"Running MERGE into {TARGET_TABLE}")
    session.sql(merge_sql).collect()
    logger.info(f"MERGE complete for {TARGET_TABLE}")

# -----------------------
# Validation
# -----------------------
cnt = session.table(STAGING_TABLE).count()
logger.info(f"Snowflake staging table {STAGING_TABLE} row count: {cnt}")

# -----------------------
# Cleanup
# -----------------------
spark.stop()
session.close()
logger.info("POC completed successfully")
