import json
import logging
from datetime import datetime
import boto3
from pyspark.sql import SparkSession
from snowflake.snowpark import Session

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hana-to-snowflake-batch")

#Secrets Helper
# Fetches HANA and Snowflake credentials from AWS Secrets Manager.
# Returns credentials as a Python dictionary.
def get_secret(secret_name, region_name="us-east-1"):
    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

#Config
# Partition column used to split HANA table into parallel reads.
# NUM_PARTITIONS controls Spark parallelism.
# BATCH_SIZE: number of rows to insert at a time into Snowflake.

AWS_REGION = "us-west-2"
HANA_SECRET_NAME = "sbx/hana/credentials"  
SNOWFLAKE_SECRET_NAME = "sbx/snowflake/credentials" 

HANA_SCHEMA = "ECC_HANA" 
HANA_TABLE = "KNA1"       
PARTITION_COL = "" 
NUM_PARTITIONS = 10        

SF_DATABASE = "DBSN_000_DEV01" 
SF_SCHEMA = "RAW"              
SF_TARGET_TABLE = f"{SF_DATABASE}.{SF_SCHEMA}.{HANA_TABLE}"

LOAD_TYPE = "full"
BATCH_SIZE = 50000  

#Fetch Secrets
logger.info("Fetching credentials...")
hana_creds = get_secret(HANA_SECRET_NAME)
sf_creds = get_secret(SNOWFLAKE_SECRET_NAME)

jdbc_url = hana_creds["db_url"]
hana_user = hana_creds["db_username"]
hana_password = hana_creds["db_password"]
jdbc_driver = hana_creds["jdbc_driver_name"]

sf_conn = {
    "account": sf_creds["account"],
    "user": sf_creds["user"],
    "password": sf_creds["password"],
    "role": sf_creds.get("role", "ETL_ROLE"),
    "warehouse": sf_creds.get("warehouse", "ETL_WH"),
    "database": SF_DATABASE,
    "schema": SF_SCHEMA,
}

#Spark Session
spark = (
    SparkSession.builder
    .appName("hana-to-snowflake-batch")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)
logger.info("Spark session created")

#Determine min/max for partitioning
min_max_query = f"(SELECT MIN({PARTITION_COL}) AS min_val, MAX({PARTITION_COL}) AS max_val FROM {HANA_SCHEMA}.{HANA_TABLE}) AS tmp"
min_max_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", min_max_query) \
    .option("user", hana_user) \
    .option("password", hana_password) \
    .option("driver", jdbc_driver) \
    .load()
min_val, max_val = min_max_df.first()
logger.info(f"Partition column {PARTITION_COL} min={min_val}, max={max_val}")

#Extract HANA Table in Partitions
logger.info("Reading data from HANA in partitions...")
df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"{HANA_SCHEMA}.{HANA_TABLE}") \
    .option("user", hana_user) \
    .option("password", hana_password) \
    .option("driver", jdbc_driver) \
    .option("partitionColumn", PARTITION_COL) \
    .option("lowerBound", min_val) \
    .option("upperBound", max_val) \
    .option("numPartitions", NUM_PARTITIONS) \
    .load()

logger.info(f"Extracted {df.count()} rows from HANA with partitioning")

#Snowpark Session
session = Session.builder.configs(sf_conn).create()
logger.info("Snowpark session created")

#Batch Insert into Snowflake
columns = df.columns
logger.info(f"Preparing batch insert for {len(columns)} columns")

# Converts each Spark partition (RDD) into batches of rows.
# Uses Snowpark batch insert to write efficiently to Snowflake.
# Avoids sending the entire DataFrame at once, preventing memory overflow.
# BATCH_SIZE controls the number of rows per insert.
def insert_partition(iterator):
    batch = []
    for row in iterator:
        batch.append(tuple(row))
        if len(batch) >= BATCH_SIZE:
            session.create_dataframe(batch, schema=columns).write.mode("append").save_as_table(SF_TARGET_TABLE)
            batch = []
    # insert remaining rows
    if batch:
        session.create_dataframe(batch, schema=columns).write.mode("append").save_as_table(SF_TARGET_TABLE)

logger.info("Starting batch insert into Snowflake...")
df.rdd.foreachPartition(insert_partition)
logger.info(f"Batch insert completed into {SF_TARGET_TABLE}")

#Validation
sf_count = session.table(SF_TARGET_TABLE).count()
logger.info(f"Snowflake target table row count: {sf_count}")

#Cleanup
session.close()
spark.stop()
logger.info("POC with batch insert completed successfully")
