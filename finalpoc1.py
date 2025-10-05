# json - for reading configuration files and AWS Secrets Manager responses.
# logging - monitor ETL steps, row counts, and errors.
# datetime - timestamp folders for S3 staging.
# pyspark.sql.SparkSession - read HANA tables efficiently, supports parallelism.
# snowflake.snowpark.Session - interact with Snowflake using Python DataFrames and SQL.
# boto3 - fetch credentials from AWS Secrets Manager securely.
import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession           
from snowflake.snowpark import Session        
import boto3                                

# LOGGING SETUP
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hana-to-snowflake-poc")

AWS_REGION = "us-west-2"

# Fetches secrets (HANA username/password, Snowflake credentials) securely.
# Returns a Python dictionary from the JSON string stored in Secrets Manager
def get_secret(secret_name, region=AWS_REGION):
    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp["SecretString"])

#Fetches HANA credentials (host, username, password, JDBC driver) and Snowflake credentials (account, user, password, warehouse, database, schema)
hana_creds = get_secret("HANA_SECRET_NAME") #edit secret name here
sf_creds = get_secret("SNOWFLAKE_SECRET_NAME") #edit secret name here

# Extracts HANA connection details from the secret
hana_host = hana_creds["db_url"]
hana_user = hana_creds["db_username"]
hana_password = hana_creds["db_password"]
jdbc_driver = hana_creds["jdbc_driver_name"]

# Prepares a dictionary to create a Snowpark session
conn_params = {
    "account": sf_creds["account"],
    "user": sf_creds["user"],
    "password": sf_creds["password"],
    "warehouse": sf_creds.get("warehouse", "ETL_WH"),
    "database": sf_creds.get("database", "DWH"),
    "schema": sf_creds.get("schema", "PUBLIC"),
}

# Table Configuration
# PARTITION_COL: numeric column used to partition HANA reads (for parallelism).
# INCREMENTAL_COL: tracks last processed changes - only rows newer than this column are extracted.
# STAGING_TABLE: temporary table in Snowflake to hold new incremental data before merging into target table.
HANA_SCHEMA = "MYSCHEMA"
HANA_TABLE = "MYTABLE"
PARTITION_COL = "ID"                    
INCREMENTAL_COL = "LAST_UPDATED"     
LOAD_TYPE = "incremental"              
TARGET_TABLE = "MYSCHEMA.MYTABLE"      
STAGING_TABLE = f"{TARGET_TABLE}_STG"  

# SPARK SESSION
spark = SparkSession.builder \
    .appName("HANA_to_Snowflake_poc") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# SNOWPARK SESSION
session = Session.builder.configs(conn_params).create()

# Determine Last Incremental Value
# Checks latest processed value of INCREMENTAL_COL in the target table.
# If the table is empty or doesn’t exist, starts from a very old default date.
# Ensures only new or updated rows are extracted from HANA.
try:
    last_max_val = session.table(TARGET_TABLE).select(INCREMENTAL_COL).max().collect()[0][0]
    if last_max_val is None:
        last_max_val = "1900-01-01 00:00:00"
except Exception:
    last_max_val = "1900-01-01 00:00:00"

logger.info(f"Last incremental value: {last_max_val}")

# HANA JDBC URL
# JDBC driver allows Spark to read data from HANA like a database connection
jdbc_url = f"jdbc:sap://{hana_host}"

# Determine Partition Bounds
# Partitioning helps Spark read large tables in parallel batches
# Fetches min/max values for the partition column for incremental rows.
# Allows batching / parallel reads in Spark.
# Avoids scanning the entire table unnecessarily.
df_bounds = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"(SELECT MIN({PARTITION_COL}) AS min_id, MAX({PARTITION_COL}) AS max_id "
                        f"FROM {HANA_SCHEMA}.{HANA_TABLE} "
                        f"WHERE {INCREMENTAL_COL} > '{last_max_val}') AS bounds") \
    .option("user", hana_user) \
    .option("password", hana_password) \
    .option("driver", jdbc_driver) \
    .load()

# If no new rows to process, exit
if df_bounds.count() == 0:
    logger.info("No new data to process.")
    spark.stop()
    session.close()
    exit(0)

# Extract min and max ID for batching
min_id = df_bounds.collect()[0]["min_id"]
max_id = df_bounds.collect()[0]["max_id"]
logger.info(f"Processing partition IDs from {min_id} to {max_id}")

# Define batch size for partitioning
batch_size = 50000
batches = [(start, min(start + batch_size - 1, max_id)) for start in range(min_id, max_id + 1, batch_size)]

# Create Staging Table
session.sql(f"CREATE TABLE IF NOT EXISTS {STAGING_TABLE} LIKE {TARGET_TABLE}").collect()

# Process Each Partition
for start_id, end_id in batches:
    try:
        logger.info(f"Processing partition {start_id} to {end_id}")
        
        # Build a query for current partition and incremental rows
        query = f"(SELECT * FROM {HANA_SCHEMA}.{HANA_TABLE} " \
                f"WHERE {PARTITION_COL} BETWEEN {start_id} AND {end_id} " \
                f"AND {INCREMENTAL_COL} > '{last_max_val}') AS t"

        # Read data from HANA into Spark DataFrame
        df_batch = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", hana_user) \
            .option("password", hana_password) \
            .option("driver", jdbc_driver) \
            .load()
        
        # Convert Spark DF to list of dictionaries for Snowpark
        records = [row.asDict() for row in df_batch.collect()]
        if records:
            # Create Snowpark DataFrame
            sp_df = session.create_dataframe(records)
            # Append to staging table
            sp_df.write.mode("append").save_as_table(STAGING_TABLE)
            logger.info(f"Inserted {len(records)} rows into staging")

    except Exception as e:
        logger.error(f"Partition {start_id}-{end_id} failed: {str(e)}")

# Merge Staging into Target
# MERGE logic:
# Matches existing rows based on KEY_COLS.
# Updates only UPDATE_COLS for matched rows.
# Inserts new rows if not matched.
KEY_COLS = ["ID"]
UPDATE_COLS = ["COL1", "COL2", "LAST_UPDATED"]

on_clause = " AND ".join([f"target.{k} = source.{k}" for k in KEY_COLS])
set_clause = ", ".join([f"target.{c} = source.{c}" for c in UPDATE_COLS])

merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING {STAGING_TABLE} AS source
ON {on_clause}
WHEN MATCHED THEN UPDATE SET {set_clause}
WHEN NOT MATCHED THEN INSERT ({', '.join(KEY_COLS + UPDATE_COLS)})
VALUES ({', '.join(['source.'+c for c in KEY_COLS + UPDATE_COLS])})
"""
session.sql(merge_sql).collect()
logger.info(f"Merged data into {TARGET_TABLE}")

# CLEANUP
spark.stop()
session.close()
logger.info("Incremental Big Data POC completed successfully")
