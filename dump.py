import boto3, json
from snowflake.snowpark import Session

# Fetch Snowflake credentials from AWS Secrets Manager
def get_snowflake_creds(secret_name="snowflake/connection", region="us-east-1"):
    client = boto3.client("secretsmanager", region_name=region)
    secret = client.get_secret_value(SecretId=secret_name)
    return json.loads(secret["SecretString"])

creds = get_snowflake_creds()

# Build connection parameters
conn_params = {
    "account": creds["account"],
    "user": creds["user"],
    "password": creds["password"],   # ⚠️ safer than env vars, but can replace with key pair
    "role": creds.get("role", "ETL_ROLE"),
    "warehouse": creds.get("warehouse", "ETL_WH"),
    "database": creds.get("database", "DWH"),
    "schema": creds.get("schema", "PUBLIC"),
}

# Create Snowpark session
session = Session.builder.configs(conn_params).create()