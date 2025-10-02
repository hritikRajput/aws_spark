"""
SAP HANA → AWS S3 Extractor (Single Table)

This script extracts data from ONE SAP HANA table into S3 staging
using PySpark and JDBC. It supports:
    - Full or Incremental Loads
    - Partitioning by DATE or NUMERIC column for parallelism
    - Automatic file size optimization (Parquet output)

SECURITY:
    HANA credentials are retrieved securely from AWS Secrets Manager.
    No passwords are stored in environment variables.

OUTPUT:
    Data is written as Parquet files into S3 paths like:
    s3://mybucket/staging/SCHEMA/TABLE/date=YYYY-MM-DD/
    or
    s3://mybucket/staging/SCHEMA/TABLE/run_date=YYYY-MM-DD/

USAGE:
    Set these environment variables before running:
      HANA_SECRET_NAME   = Secret in AWS Secrets Manager (JSON with host/user/pass/port)
      AWS_REGION         = AWS region of the secret
      SRC_SCHEMA         = Schema in HANA (e.g., "SALES")
      SRC_TABLE          = Table in HANA (e.g., "ORDERS")
      LOAD_TYPE          = "incremental" or "full"
      PARTITION_COL      = Column for partitioning (date or numeric)
      STAGING_PREFIX     = Target S3 path prefix (e.g., "s3://bucket/staging/")
      EXPECTED_SIZE_GB   = Estimated table size in GB (default=10)
      PARTITION_FILESIZE_MB = Desired file size in MB (default=150)

Example spark-submit:
    spark-submit --jars /path/to/ngdbc.jar hana_extract_single.py
"""

import os, yaml, logging, base64
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import boto3
from botocore.exceptions import ClientError

# -----------------------
# Setup Logging
# -----------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hana-extract")

# -----------------------
# Helper: Get SAP HANA credentials from AWS Secrets Manager
# -----------------------
def get_secret(secret_name, region_name="us-east-1"):
    """
    Fetch HANA credentials stored in AWS Secrets Manager.

    Secret should look like:
    {
        "username": "hana_user",
        "password": "hana_pass",
        "host": "hana.example.com",
        "port": "30015"
    }
    """
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        resp = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logger.error(f"Unable to fetch secret {secret_name}: {e}")
        raise e

    # Decode secret JSON string
    if "SecretString" in resp:
        secret_str = resp["SecretString"]
    else:
        secret_str = base64.b64decode(resp["SecretBinary"]).decode("utf-8")

    return yaml.safe_load(secret_str)

# -----------------------
# Load HANA Credentials
# -----------------------
HANA_SECRET_NAME = os.environ.get("HANA_SECRET_NAME", "hana/credentials")
secrets = get_secret(HANA_SECRET_NAME, region_name=os.environ.get("AWS_REGION", "us-east-1"))

HANA_HOST = secrets["host"]
HANA_PORT = secrets.get("port", "30015")
HANA_USER = secrets["username"]
HANA_PASSWORD = secrets["password"]

jdbc_base = f"jdbc:sap://{HANA_HOST}:{HANA_PORT}"

# -----------------------
# Spark Session
# -----------------------
spark = (
    SparkSession.builder
    .appName("hana-extract-single")
    .config("spark.sql.shuffle.partitions", "200")  # Default shuffle parallelism
    .getOrCreate()
)

# -----------------------
# Table Parameters (Single Table Only)
# -----------------------
src_schema = os.environ.get("SRC_SCHEMA", "SALES")
src_table = os.environ.get("SRC_TABLE", "ORDERS")
load_type = os.environ.get("LOAD_TYPE", "incremental")   # incremental / full
partition_col = os.environ.get("PARTITION_COL", "ORDER_DATE")  # e.g., ORDER_DATE or EMP_ID
expected_size_gb = int(os.environ.get("EXPECTED_SIZE_GB", "10"))

# S3 staging location
staging_base = os.environ.get("STAGING_PREFIX", "s3://mybucket/staging/")
target_prefix = f"{staging_base}{src_schema}/{src_table}/run_date={datetime.utcnow().strftime('%Y-%m-%d')}/"
target_file_size = int(os.environ.get("PARTITION_FILESIZE_MB", "150")) * 1024 * 1024

logger.info(f"Extracting {src_schema}.{src_table} (load_type={load_type}, partition_col={partition_col})")

# -----------------------
# Utility: Get min/max of numeric column
# -----------------------
def get_numeric_bounds(schema, table, column):
    """
    Query HANA for min/max of numeric column to enable parallel extraction.
    """
    query = f"(select min({column}) as minv, max({column}) as maxv from {schema}.{table}) as t"
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_base)
        .option("dbtable", query)
        .option("user", HANA_USER)
        .option("password", HANA_PASSWORD)
        .load()
    )
    row = df.collect()[0]
    return row["minv"], row["maxv"]

# -----------------------
# Build Base Query
# -----------------------
dbtable_expr = f"{src_schema}.{src_table}"

# Incremental checkpoint placeholder
lower_ts = None
if load_type == "incremental":
    lower_ts = os.environ.get(f"CHECKPOINT_{src_schema}_{src_table}", None)

# -----------------------
# Strategy A: Date Partitioning
# -----------------------
if partition_col and not partition_col.isdigit():
    end_date = datetime.utcnow().date()
    # For incremental, start from checkpoint; otherwise, last N days
    if load_type == "incremental" and lower_ts:
        start_date = datetime.strptime(lower_ts, "%Y-%m-%d").date()
    else:
        start_date = end_date - timedelta(days=int(os.environ.get("BATCH_DAYS", "7")))

    current = start_date
    while current <= end_date:
        day_str = current.strftime("%Y-%m-%d")
        partition_query = (
            f"(select * from {src_schema}.{src_table} "
            f"where {partition_col} >= DATE '{day_str}' "
            f"and {partition_col} < DATE '{(current + timedelta(days=1)).strftime('%Y-%m-%d')}') as t"
        )

        # Pull one day's data
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_base)
            .option("dbtable", partition_query)
            .option("user", HANA_USER)
            .option("password", HANA_PASSWORD)
            .load()
        )

        if df.rdd.isEmpty():
            logger.info(f"No rows for {day_str}")
            current += timedelta(days=1)
            continue

        # Estimate partitions for output files
        rough_rows = df.count()
        avg_row_size_bytes = max(500, int((expected_size_gb * 1024**3) / max(1, rough_rows)))
        target_parts = max(1, int((rough_rows * avg_row_size_bytes) / target_file_size))

        logger.info(f"Writing {rough_rows} rows for {day_str} into ~{target_parts} parquet files")

        out_path = f"{staging_base}{src_schema}/{src_table}/date={day_str}/"
        df.repartition(target_parts).write.mode("overwrite").parquet(out_path)

        logger.info(f"Wrote partition to {out_path}")
        current += timedelta(days=1)

# -----------------------
# Strategy B: Numeric Partitioning OR Single Read
# -----------------------
else:
    if partition_col:  # numeric partition column
        minv, maxv = get_numeric_bounds(src_schema, src_table, partition_col)
        if minv is None or maxv is None:
            # fallback to single read
            df = (
                spark.read.format("jdbc")
                .option("url", jdbc_base)
                .option("dbtable", dbtable_expr)
                .option("user", HANA_USER)
                .option("password", HANA_PASSWORD)
                .load()
            )
        else:
            # Partition the JDBC read into multiple ranges
            num_partitions = min(200, max(1, int((maxv - minv) / 100000) + 1))
            logger.info(f"Numeric bounds {partition_col}: {minv}..{maxv}, partitions={num_partitions}")
            df = (
                spark.read.format("jdbc")
                .option("url", jdbc_base)
                .option("dbtable", dbtable_expr)
                .option("user", HANA_USER)
                .option("password", HANA_PASSWORD)
                .option("partitionColumn", partition_col)
                .option("lowerBound", str(minv))
                .option("upperBound", str(maxv))
                .option("numPartitions", str(num_partitions))
                .load()
            )
    else:
        # Small tables → single read
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_base)
            .option("dbtable", dbtable_expr)
            .option("user", HANA_USER)
            .option("password", HANA_PASSWORD)
            .load()
        )

    if df.rdd.isEmpty():
        logger.info(f"No data found for {src_schema}.{src_table}")
    else:
        rows = df.count()
        target_parts = max(1, int(rows / (target_file_size / 1000)))
        logger.info(f"Writing {rows} rows into {target_parts} parquet files")

        df.repartition(target_parts).write.mode("overwrite").parquet(target_prefix)
        logger.info(f"Wrote to {target_prefix}")

# -----------------------
# Cleanup
# -----------------------
spark.stop()
















"""
Snowflake Loader (Single Table)

This script loads parquet files from an external stage (e.g. S3) into
Snowflake using Snowpark Python. It supports:
    1. COPY INTO a staging table
    2. Row count validation
    3. MERGE (upsert) into a target DWH table

SECURITY:
    Snowflake credentials are provided via environment variables.
    In production, consider using key pair auth or OAuth instead of passwords.

OUTPUT:
    Data is merged from staging into the final DWH table.

USAGE:
    Set these environment variables before running:
      SF_ACCOUNT      = Snowflake account (e.g., "abc-xy12345")
      SF_USER         = Snowflake user
      SF_PASSWORD     = Password (⚠️ not recommended for prod)
      SF_ROLE         = Role to use (default = "ETL_ROLE")
      SF_WAREHOUSE    = Warehouse to use (default = "ETL_WH")
      SF_DATABASE     = Database name (default = "DWH")
      SF_SCHEMA       = Schema name (default = "PUBLIC")

Example:
    python snowpark_load_single.py
"""

import os, logging
from snowflake.snowpark import Session

# -----------------------
# Logging setup 
# -----------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("snowpark-load")

# -----------------------
# Snowflake Connection
# -----------------------
conn_params = {
    "account": os.environ["SF_ACCOUNT"],
    "user": os.environ["SF_USER"],
    "password": os.environ.get("SF_PASSWORD"),  # ⚠️ Use keypair/OAuth in production
    "role": os.environ.get("SF_ROLE", "ETL_ROLE"),
    "warehouse": os.environ.get("SF_WAREHOUSE", "ETL_WH"),
    "database": os.environ.get("SF_DATABASE", "DWH"),
    "schema": os.environ.get("SF_SCHEMA", "PUBLIC"),
}
session = Session.builder.configs(conn_params).create()

# -----------------------
# Helper: Copy parquet files into Snowflake table
# -----------------------
def copy_into_table(stage_location, sf_table,
                    file_format="(type=PARQUET AUTO_DETECT=TRUE)",
                    on_error="continue"):
    """
    Load parquet files from S3/external stage into a Snowflake table.

    Args:
        stage_location (str): Path to staged files (e.g., s3://bucket/path/ or @mystage/path).
        sf_table (str): Target Snowflake table name.
        file_format (str): File format definition (default = PARQUET with schema auto-detect).
        on_error (str): How to handle errors (default = 'continue').
    """
    copy_sql = f"""
    COPY INTO {sf_table}
    FROM '{stage_location}'
    FILE_FORMAT = ({file_format})
    ON_ERROR = {on_error}
    """
    logger.info("Running COPY: " + copy_sql)
    res = session.sql(copy_sql).collect()
    logger.info(f"COPY result: {res}")
    return res

# -----------------------
# Helper: Validate row counts
# -----------------------
def validate_load(sf_table, expected_rows=0):
    """
    Count rows in table after COPY for sanity check.
    """
    cnt = session.table(sf_table).count()
    logger.info(f"Table {sf_table} now has {cnt} rows (expected >= {expected_rows})")
    return cnt

# -----------------------
# Helper: Merge staging into target table
# -----------------------
def merge_upsert(staging_table, target_table, key_cols, update_cols):
    """
    Perform an UPSERT (MERGE) from staging table into target table.

    Args:
        staging_table (str): Name of the staging table (already loaded).
        target_table (str): Target DWH table to merge into.
        key_cols (list): Primary key columns for matching.
        update_cols (list): Columns to update when matched.
    """
    # Build ON clause for matching rows
    on_clause = " AND ".join([f"target.{k} = source.{k}" for k in key_cols])
    # Build SET clause for updates
    set_clause = ", ".join([f"target.{c} = source.{c}" for c in update_cols])

    merge_sql = f"""
    MERGE INTO {target_table} AS target
    USING {staging_table} AS source
    ON {on_clause}
    WHEN MATCHED THEN UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN INSERT ({', '.join(update_cols + key_cols)})
      VALUES ({', '.join(['source.'+c for c in update_cols + key_cols])});
    """

    logger.info("Running MERGE...")
    session.sql(merge_sql).collect()
    logger.info("MERGE complete")

# -----------------------
# Example ETL flow (one table)
# -----------------------

# Location of parquet files extracted from HANA and staged to S3
stage_prefix = "s3://my-bucket/hana_staging/HANA_ORDERS/"

# Staging table in Snowflake (raw schema)
raw_table = "RAW.HANA_ORDERS_TEMP"

# Target fact table
target_table = "DWH.FACT_ORDERS"

# 1. Copy parquet data into RAW staging table
copy_into_table(stage_prefix, raw_table)

# 2. Validate row count (just log it here)
rows = validate_load(raw_table, expected_rows=0)

# 3. Merge into production table
merge_upsert(
    staging_table=raw_table,
    target_table=target_table,
    key_cols=["ORDER_ID"],
    update_cols=["AMOUNT", "STATUS", "UPDATED_AT"]
)

logger.info("Snowflake load complete ✅")

