import json
import logging
from datetime import datetime
import boto3
from pyspark.sql import SparkSession
from snowflake.snowpark import Session

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hana-to-snowflake-poc")

# AWS Secrets Manager helper
def get_secret(secret_name, region_name="us-west-2"):
    client = boto3.client("secretsmanager", region_name=region_name)
    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp["SecretString"])

# Configuration — EDIT THESE
AWS_REGION = "us-west-2"
HANA_SECRET_NAME = "sbx/hana/credentials"         
SNOWFLAKE_SECRET_NAME = "sbx/snowflake/credentials"  

HANA_SCHEMA = "ECC_HANA"
HANA_TABLE = "KNA1"
PARTITION_COL = ""       # Numeric column or date column for parallel read
NUM_PARTITIONS = 10           # Adjust based on table size
LOAD_TYPE = "full"

SF_DATABASE = "DBSN_000_DEV01"
SF_SCHEMA = "RAW"
SF_TARGET_TABLE = f"{SF_DATABASE}.{SF_SCHEMA}.{HANA_TABLE}"

# Retrieve secrets
hana_creds = get_secret(HANA_SECRET_NAME)
sf_creds = get_secret(SNOWFLAKE_SECRET_NAME)


# Initialize Spark session

spark = (
    SparkSession.builder
    .appName("hana-to-snowflake-poc")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

logger.info("Spark session ready")

# HANA JDBC connection
jdbc_url = hana_creds["db_url"]
hana_user = hana_creds["db_username"]
hana_password = hana_creds["db_password"]
jdbc_driver = hana_creds["jdbc_driver_name"]

logger.info(f"Reading table {HANA_SCHEMA}.{HANA_TABLE} from HANA using partitioned Spark read...")


# Partitioned read — scalable for big data
df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", f"{HANA_SCHEMA}.{HANA_TABLE}")
    .option("user", hana_user)
    .option("password", hana_password)
    .option("driver", jdbc_driver)
    .option("partitionColumn", PARTITION_COL)  # numeric/date column
    .option("lowerBound", "1")                 # EDIT lower bound of partition column
    .option("upperBound", "1000000")           # EDIT upper bound of partition column
    .option("numPartitions", NUM_PARTITIONS)
    .load()
)

row_count = df.count()
logger.info(f"Extracted {row_count} rows from HANA (partitioned)")


# Snowflake connection using Snowpark
sf_conn = {
    "account": sf_creds["account"],
    "user": sf_creds["user"],
    "password": sf_creds["password"],
    "role": sf_creds.get("role", "ETL_ROLE"),
    "warehouse": sf_creds.get("warehouse", "ETL_WH"),
    "database": SF_DATABASE,
    "schema": SF_SCHEMA,
}

session = Session.builder.configs(sf_conn).create()
logger.info("Snowpark session created")

# Write to Snowflake directly using Snowpark — no collect()
logger.info(f"Writing Spark DataFrame directly to Snowflake table {SF_TARGET_TABLE}...")

# Uses Spark Snowflake connector:
# df.write.format("snowflake"): Spark DataFrame → Snowflake.
# .mode("overwrite"): for full load, drops and recreates the table.
# No intermediate S3 staging needed; Spark handles data transfer internally.
df.write \
    .format("snowflake") \
    .options(**{
        "sfURL": sf_creds["account"] + ".snowflakecomputing.com",
        "sfDatabase": SF_DATABASE,
        "sfSchema": SF_SCHEMA,
        "sfWarehouse": sf_creds.get("warehouse", "ETL_WH"),
        "dbtable": HANA_TABLE,
        "user": sf_creds["user"],
        "password": sf_creds["password"],
        "role": sf_creds.get("role", "ETL_ROLE")
    }) \
    .mode("overwrite" if LOAD_TYPE == "full" else "append") \
    .save()

logger.info(f"Data loaded into Snowflake table {SF_TARGET_TABLE}")


# Validation
sf_count = session.table(SF_TARGET_TABLE).count()
logger.info(f"Snowflake table row count: {sf_count}")

# ---------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------
session.close()
spark.stop()
logger.info("POC completed successfully!")
