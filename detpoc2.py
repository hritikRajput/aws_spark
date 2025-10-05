"""
Connecting to SAP HANA-
HANA JDBC driver (ngdbc.jar) for Spark.
HANA host, port, username, password.
The table name and schema you want to extract.
Optional: incremental load key (like ORDER_DATE) if you want to extract only new/changed rows.
PySpark environment to read HANA via JDBC.
Key considerations:
Partitioning: For large tables, use date or numeric partition column to parallelize reads.
Security: Never hardcode passwords — either use environment variables or AWS Secrets Manager.
File format: Parquet is preferred for efficiency, even if temporary, because Snowflake can read Parquet efficiently.

Loading into Snowflake using Snowpark-
Snowflake credentials: account, user, password, role, warehouse, database, schema.
Snowpark Python library installed (snowflake-snowpark-python).
Target Snowflake table: staging table first, then final table.
Key considerations:
you could load directly from memory (PySpark DataFrame → Snowpark DataFrame → Snowflake) if table size is small.
For large tables, staging in S3 is safer — Snowpark can do COPY INTO from S3.
Logging for success/failure.
"""


"""
POC: SAP HANA → S3 → Snowflake (Full/Incremental Load Ready)
This script extracts data from SAP HANA using Spark, stages it in S3, and then loads it into Snowflake using Snowpark.

It supports:
- Full load (copy entire table)
- Incremental load (copy only changed rows)
- Writing to S3 as Parquet (efficient file format)
- Loading to Snowflake staging table via COPY INTO
- Optional MERGE/UPSERT into final target table
- Secure credential storage in AWS Secrets Manager
"""

import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from snowflake.snowpark import Session
import boto3

# Logging setup
# This will print INFO level logs (like row counts, status updates).
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hana-snowflake-poc")

# Load config file
# Reads all required settings (HANA/S3/Snowflake configs) from config.json.
with open("config.json") as f:
    config = json.load(f)

AWS_REGION = config["aws_region"]

# Function to fetch secrets (passwords, usernames, etc.) from AWS Secrets Manager.
def get_secret(secret_name, region=AWS_REGION):
    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp["SecretString"])

# Load SAP HANA connection details from secrets + config.
hana_cfg = config["hana"]
hana_creds = get_secret(hana_cfg["secret_name"])

HANA_SCHEMA = hana_cfg["schema"]
HANA_TABLE = hana_cfg["table"]
PARTITION_COL = hana_cfg.get("partition_col", "ORDER_DATE")
INCREMENTAL_COL = hana_cfg.get("incremental_col", "LAST_UPDATED")

# JDBC connection string to connect Spark → SAP HANA
jdbc_url = f"jdbc:sap://{hana_creds['host']}:{hana_creds.get('port', '30015')}"

# Load type: full or incremental
LOAD_TYPE = config.get("load_type", "full")
LAST_RUN_DATE = config.get("last_run_date", None)

# S3 is used as an intermediate staging layer.
s3_cfg = config["s3"]
STAGING_PREFIX = s3_cfg["staging_prefix"]  # e.g. "s3://mybucket/staging/"

# Each run writes to a new folder with today's date
TARGET_PREFIX = (
    f"{STAGING_PREFIX}{HANA_SCHEMA}/{HANA_TABLE}/"
    f"run_date={datetime.utcnow().strftime('%Y-%m-%d')}/"
)

# Approximate target file size (helps control number of Parquet files)
TARGET_FILESIZE_MB = int(s3_cfg.get("partition_filesize_mb", 150)) * 1024 * 1024

# Load Snowflake credentials from Secrets Manager
sf_cfg = config["snowflake"]
sf_creds = get_secret(sf_cfg["secret_name"])

# Connection parameters for Snowpark
conn_params = {
    "account": sf_creds["account"],
    "user": sf_creds["user"],
    "password": sf_creds["password"],
    "role": sf_creds.get("role", "ETL_ROLE"),
    "warehouse": sf_creds.get("warehouse", "ETL_WH"),
    "database": sf_creds.get("database", "DWH"),
    "schema": sf_creds.get("schema", "PUBLIC"),
}

# Tables in Snowflake
STAGING_TABLE = sf_cfg.get("staging_table", f"RAW.{HANA_TABLE}_TEMP")
TARGET_TABLE = sf_cfg.get("target_table", f"DWH.{HANA_TABLE}")
KEY_COLS = sf_cfg.get("key_cols", "ORDER_ID").split(",")
UPDATE_COLS = sf_cfg.get("update_cols", "AMOUNT,STATUS,LAST_UPDATED").split(",")

# Spark is used to connect to HANA and move data → S3.
spark = (
    SparkSession.builder
    .appName("hana-to-s3")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

# Build query based on load type (full vs incremental).
if LOAD_TYPE.lower() == "incremental" and LAST_RUN_DATE:
    logger.info(f"Performing incremental extract since {LAST_RUN_DATE}")
    query = f"(SELECT * FROM {HANA_SCHEMA}.{HANA_TABLE} WHERE {INCREMENTAL_COL} > DATE '{LAST_RUN_DATE}') as t"
else:
    logger.info("Performing full extract")
    query = f"{HANA_SCHEMA}.{HANA_TABLE}"

# Use Spark JDBC to read data from HANA into a DataFrame.
df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", query)
    .option("user", hana_creds["username"])
    .option("password", hana_creds["password"])
    .load()
)

rows = df.count()
logger.info(f"Extracted {rows} rows from HANA")

# Save the HANA data into S3 as Parquet files (efficient, compressed).
logger.info(f"Writing {rows} rows to S3 at {TARGET_PREFIX}")
df.write.mode("overwrite").parquet(TARGET_PREFIX)

# Snowflake session
session = Session.builder.configs(conn_params).create()

# COPY INTO Snowflake staging
# Load the Parquet files from S3 into Snowflake staging table.
logger.info(f"Loading into Snowflake staging table {STAGING_TABLE}")

copy_sql = f"""
COPY INTO {STAGING_TABLE}
FROM '{TARGET_PREFIX}'
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE'
"""
session.sql(copy_sql).collect()


# MERGE into target (incremental only)
# If incremental load: merge staging data into target table (UPSERT).
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

# FULL LOAD handling
if LOAD_TYPE.lower() == "full" and rows > 0:
    logger.info(f"Performing full load into target table {TARGET_TABLE}")
    #Remove all existing rows from target table
    #Insert all data from staging into target
    full_sql = f"""
    TRUNCATE TABLE {TARGET_TABLE};
    INSERT INTO {TARGET_TABLE}
    SELECT * FROM {STAGING_TABLE};
    """
    session.sql(full_sql).collect()
    logger.info("Full load completed successfully")


# Validation
# Count rows in staging table to confirm load worked.
cnt = session.table(STAGING_TABLE).count()
logger.info(f"Snowflake staging table {STAGING_TABLE} row count: {cnt}")

# Cleanup
# Stop Spark and close Snowpark session.
spark.stop()
session.close()
logger.info("POC completed successfully")
