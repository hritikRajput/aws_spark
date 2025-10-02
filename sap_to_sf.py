"""
HANA → S3 → Snowflake Loader (Single Table)

- Fully config-driven via JSON file in S3
- Extract from HANA using PySpark + JDBC
- Load into Snowflake using Snowpark Python
- Supports full/incremental loads, partitioning, file-size optimization
- Credentials securely fetched from AWS Secrets Manager
"""

import json, logging, base64
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import boto3
from botocore.exceptions import ClientError
from snowflake.snowpark import Session

# -----------------------
# Logging
# -----------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hana-snowflake-etl")

# -----------------------
# Helpers
# -----------------------

def get_secret(secret_name, region_name="us-east-1"):
    """Fetch secrets from AWS Secrets Manager"""
    session = boto3.session.Session()
    client = session.client("secretsmanager", region_name=region_name)
    try:
        resp = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logger.error(f"Unable to fetch secret {secret_name}: {e}")
        raise e

    if "SecretString" in resp:
        secret_str = resp["SecretString"]
    else:
        secret_str = base64.b64decode(resp["SecretBinary"]).decode("utf-8")

    return json.loads(secret_str)

def load_config(s3_bucket, s3_key, region_name="us-east-1"):
    """Load JSON config from S3"""
    s3 = boto3.client("s3", region_name=region_name)
    obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    return json.load(obj["Body"])

# -----------------------
# Load pipeline config
# -----------------------
CONFIG_BUCKET = "mybucket"
CONFIG_KEY = "configs/hana_snowflake_pipeline.json"
REGION = "us-east-1"

config = load_config(CONFIG_BUCKET, CONFIG_KEY, REGION)

# HANA extraction config
src_schema = config["SRC_SCHEMA"]
src_table = config["SRC_TABLE"]
load_type = config.get("LOAD_TYPE", "incremental")
partition_col = config.get("PARTITION_COL", "ORDER_DATE")
expected_size_gb = config.get("EXPECTED_SIZE_GB", 10)
batch_days = config.get("BATCH_DAYS", 7)
staging_base = config.get("STAGING_PREFIX", f"s3://{CONFIG_BUCKET}/staging/")
partition_file_size_mb = config.get("PARTITION_FILESIZE_MB", 150)
hana_secret_name = config.get("HANA_SECRET_NAME", "hana/credentials")

# Snowflake config
sf_secret_name = config.get("SNOWFLAKE_SECRET_NAME", "snowflake/connection")
sf_stage_path = config["SF_STAGE_PATH"]  # e.g., s3://my-bucket/hana_staging/HANA_ORDERS/
sf_staging_table = config["SF_STAGING_TABLE"]  # e.g., RAW.HANA_ORDERS_TEMP
sf_target_table = config["SF_TARGET_TABLE"]    # e.g., DWH.FACT_ORDERS
sf_key_cols = config.get("SF_KEY_COLS", ["ORDER_ID"])
sf_update_cols = config.get("SF_UPDATE_COLS", ["AMOUNT","STATUS","UPDATED_AT"])

# -----------------------
# Fetch credentials
# -----------------------
hana_creds = get_secret(hana_secret_name, REGION)
HANA_HOST = hana_creds["host"]
HANA_PORT = hana_creds.get("port", "30015")
HANA_USER = hana_creds["username"]
HANA_PASSWORD = hana_creds["password"]
jdbc_base = f"jdbc:sap://{HANA_HOST}:{HANA_PORT}"

sf_creds = get_secret(sf_secret_name, REGION)
sf_conn_params = {
    "account": sf_creds["account"],
    "user": sf_creds["user"],
    "password": sf_creds["password"],
    "role": sf_creds.get("role", "ETL_ROLE"),
    "warehouse": sf_creds.get("warehouse", "ETL_WH"),
    "database": sf_creds.get("database", "DWH"),
    "schema": sf_creds.get("schema", "PUBLIC"),
}

# -----------------------
# Spark Session
# -----------------------
spark = (
    SparkSession.builder
    .appName("hana-extract-single")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

# -----------------------
# Helper: numeric bounds for partitioning
# -----------------------
def get_numeric_bounds(schema, table, column):
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
# -----------------------
# HANA → S3 Extraction
# -----------------------
target_prefix = f"{staging_base}{src_schema}/{src_table}/run_date={datetime.utcnow().strftime('%Y-%m-%d')}/"
target_file_size = partition_file_size_mb * 1024 * 1024
dbtable_expr = f"{src_schema}.{src_table}"
lower_ts = config.get("CHECKPOINT_DATE") if load_type == "incremental" else None

# Date partitioning
if partition_col and not partition_col.isdigit():
    end_date = datetime.utcnow().date()
    if load_type=="incremental" and lower_ts:
        start_date = datetime.strptime(lower_ts, "%Y-%m-%d").date()
    else:
        start_date = end_date - timedelta(days=batch_days)

    current = start_date
    while current <= end_date:
        day_str = current.strftime("%Y-%m-%d")
        partition_query = (
            f"(select * from {src_schema}.{src_table} "
            f"where {partition_col} >= DATE '{day_str}' "
            f"and {partition_col} < DATE '{(current + timedelta(days=1)).strftime('%Y-%m-%d')}') as t"
        )
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

        rough_rows = df.count()
        avg_row_size_bytes = max(500, int((expected_size_gb * 1024**3) / max(1, rough_rows)))
        target_parts = max(1, int((rough_rows * avg_row_size_bytes) / target_file_size))
        out_path = f"{staging_base}{src_schema}/{src_table}/date={day_str}/"
        df.repartition(target_parts).write.mode("overwrite").parquet(out_path)
        logger.info(f"Wrote {rough_rows} rows to {out_path} in ~{target_parts} files")
        current += timedelta(days=1)

# Numeric partitioning or single read fallback
else:
    if partition_col:
        minv, maxv = get_numeric_bounds(src_schema, src_table, partition_col)
        if minv is None or maxv is None:
            df = spark.read.format("jdbc").option("url", jdbc_base).option("dbtable", dbtable_expr)\
                .option("user", HANA_USER).option("password", HANA_PASSWORD).load()
        else:
            num_partitions = min(200, max(1, int((maxv - minv) / 100000) + 1))
            df = spark.read.format("jdbc").option("url", jdbc_base).option("dbtable", dbtable_expr)\
                .option("user", HANA_USER).option("password", HANA_PASSWORD)\
                .option("partitionColumn", partition_col).option("lowerBound", str(minv))\
                .option("upperBound", str(maxv)).option("numPartitions", str(num_partitions)).load()
    else:
        df = spark.read.format("jdbc").option("url", jdbc_base).option("dbtable", dbtable_expr)\
            .option("user", HANA_USER).option("password", HANA_PASSWORD).load()

    if not df.rdd.isEmpty():
        rows = df.count()
        target_parts = max(1, int(rows / (target_file_size / 1000)))
        df.repartition(target_parts).write.mode("overwrite").parquet(target_prefix)
        logger.info(f"Wrote {rows} rows to {target_prefix} in ~{target_parts} files")

# -----------------------
# Snowflake Loader
# -----------------------
session = Session.builder.configs(sf_conn_params).create()

def copy_into_table(stage_location, sf_table,
                    file_format="(type=PARQUET AUTO_DETECT=TRUE)", on_error="continue"):
    copy_sql = f"""
    COPY INTO {sf_table}
    FROM '{stage_location}'
    FILE_FORMAT = ({file_format})
    ON_ERROR = {on_error}
    """
    logger.info(f"Running COPY: {copy_sql}")
    res = session.sql(copy_sql).collect()
    logger.info(f"COPY result: {res}")
    return res

def validate_load(sf_table, expected_rows=0):
    cnt = session.table(sf_table).count()
    logger.info(f"Table {sf_table} now has {cnt} rows (expected >= {expected_rows})")
    return cnt

def merge_upsert(staging_table, target_table, key_cols, update_cols):
    on_clause = " AND ".join([f"target.{k} = source.{k}" for k in key_cols])
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

# Execute Snowflake load
copy_into_table(sf_stage_path, sf_staging_table)
validate_load(sf_staging_table)
merge_upsert(sf_staging_table, sf_target_table, sf_key_cols, sf_update_cols)

logger.info("ETL pipeline complete")

# Cleanup
spark.stop()
session.close()
