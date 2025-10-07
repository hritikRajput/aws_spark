# =============================================================
# ETL: SAP HANA → Snowflake (Snowpark) using AWS Secrets Manager
# Secure key-pair authentication
# =============================================================

import boto3
import json
import base64
import pandas as pd
import sqlalchemy
from cryptography.hazmat.primitives import serialization
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, to_date


# -------------------------------------------------------------
# 1️⃣  Fetch Secrets from AWS Secrets Manager
# -------------------------------------------------------------
def get_secret(secret_name, region="us-east-1"):
    """Fetch and return a secret from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region)
    secret_value = client.get_secret_value(SecretId=secret_name)
    if "SecretString" in secret_value:
        return json.loads(secret_value["SecretString"])
    else:
        return json.loads(base64.b64decode(secret_value["SecretBinary"]))


# Load secrets (names from your environment)
hana_secret = get_secret("hana_secret_name")
snowflake_secret = get_secret("snowflake_secret_name")
snowflake_key_secret = get_secret("snowflake_secret_key_name")

# -------------------------------------------------------------
# 2️⃣  Prepare SAP HANA Connection
# -------------------------------------------------------------
hana_user = hana_secret["username"]
hana_password = hana_secret["password"]
hana_host = hana_secret["host"]
hana_port = hana_secret.get("port", "30015")
hana_schema = hana_secret.get("schema", "SALES")

hana_connection_str = f"hana://{hana_user}:{hana_password}@{hana_host}:{hana_port}"
engine = sqlalchemy.create_engine(hana_connection_str)

query = f"SELECT ID, NAME, CREATED_AT, ACTIVE FROM {hana_schema}.CUSTOMERS"
hana_df = pd.read_sql(query, engine)
print(f"✅ Extracted {len(hana_df)} rows from SAP HANA")

# -------------------------------------------------------------
# 3️⃣  Decode Snowflake Private Key
# -------------------------------------------------------------
# The private key is stored as PEM text in Secrets Manager
private_key_pem = snowflake_key_secret["private_key"]
private_key_passphrase = snowflake_key_secret.get("private_key_passphrase")

p_key = serialization.load_pem_private_key(
    private_key_pem.encode("utf-8"),
    password=private_key_passphrase.encode("utf-8") if private_key_passphrase else None,
)

private_key_b64 = base64.b64encode(
    p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
).decode("utf-8")

# -------------------------------------------------------------
# 4️⃣  Create Snowpark Session
# -------------------------------------------------------------
connection_parameters = {
    "account": snowflake_secret["account"],
    "user": snowflake_secret["user"],
    "role": snowflake_secret["role"],
    "warehouse": snowflake_secret["warehouse"],
    "database": snowflake_secret["database"],
    "schema": snowflake_secret["schema"],
    "private_key": private_key_b64,
}

session = Session.builder.configs(connection_parameters).create()

# -------------------------------------------------------------
# 5️⃣  Load HANA Data into Snowpark and Transform
# -------------------------------------------------------------
snow_df = session.create_dataframe(hana_df)

transformed_df = (
    snow_df.filter(col("ACTIVE") == True)
           .with_column("CREATED_DATE", to_date(col("CREATED_AT")))
           .drop("ACTIVE")
)

# -------------------------------------------------------------
# 6️⃣  Write to Snowflake
# -------------------------------------------------------------
transformed_df.write.mode("overwrite").save_as_table("CUSTOMERS_STAGING")
print("✅ Data successfully written to Snowflake: CUSTOMERS_STAGING")

# -------------------------------------------------------------
# 7️⃣  Validate and Close
# -------------------------------------------------------------
count_df = session.sql("SELECT COUNT(*) AS RECORD_COUNT FROM CUSTOMERS_STAGING")
count_df.show()

session.close()
print("🏁 ETL job completed successfully.")
