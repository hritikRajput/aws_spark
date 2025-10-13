"""
=====================================================================
 PySpark → Snowflake load using Key-Pair Authentication
=====================================================================

💡 PURPOSE
-----------
Use Spark for distributed extraction (already have hana_df) and the
Snowflake Spark Connector to load data into Snowflake efficiently.
The connector automatically:
  - Writes each Spark partition to a temporary internal stage
  - Runs a COPY INTO command
  - Cleans up stage files

⚙️  AUTH METHOD
---------------
Key-Pair authentication (recommended for service accounts).
SSO/externalbrowser is *not supported* in the Spark connector.

📘  DOC REFERENCES
------------------
- Snowflake Spark Connector: https://docs.snowflake.com/en/developer-guide/spark-connector
- Key-Pair Auth setup: https://docs.snowflake.com/en/user-guide/key-pair-auth
"""

from pyspark.sql import SparkSession
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import os, re

# -------------------------------------------------------------
# 1️⃣  Spark Session
# -------------------------------------------------------------
spark = SparkSession.builder.appName("HanaToSnowflake").getOrCreate()

# Assume hana_df already exists (extracted from SAP HANA)
# e.g. hana_df = spark.read.jdbc(...)

# -------------------------------------------------------------
# 2️⃣  Load private key (PEM) for Snowflake Key-Pair authentication
# -------------------------------------------------------------
key_path = "/secure/path/rsa_key.p8"
private_key_passphrase = os.environ.get("PRIVATE_KEY_PASSPHRASE")

with open(key_path, "rb") as key_file:
    p_key = serialization.load_pem_private_key(
        key_file.read(),
        password=private_key_passphrase.encode() if private_key_passphrase else None,
        backend=default_backend()
    )

# Convert to PEM string as required by the Spark connector
pkb = p_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
).decode("utf-8")

# Clean headers/footers
pkb = re.sub(r"-*(BEGIN|END) PRIVATE KEY-*\n", "", pkb).replace("\n", "")

# -------------------------------------------------------------
# 3️⃣  Snowflake connection options
# -------------------------------------------------------------
sfOptions = {
    "sfURL": "<account>.snowflakecomputing.com",
    "sfUser": "<user>",
    "pem_private_key": pkb,
    "sfDatabase": "<DATABASE>",
    "sfSchema": "<SCHEMA>",
    "sfWarehouse": "<WAREHOUSE>",
    "sfRole": "<ROLE>"
}

# -------------------------------------------------------------
# 4️⃣  Optional: repartition to control parallelism & file size
# -------------------------------------------------------------
# Rule of thumb: aim for compressed files ~100–250 MB
hana_df = hana_df.repartition(100)

# -------------------------------------------------------------
# 5️⃣  Write to Snowflake
# -------------------------------------------------------------
hana_df.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "TARGET_TABLE") \
    .mode("append") \
    .save()

# -------------------------------------------------------------
# ✅  PROS
# -------------------------------------------------------------
# - Distributed parallel read/write (fast for 20M+ rows)
# - Handles staging + COPY automatically
# - Fully supported, production-ready
# - Key-pair auth avoids storing passwords

# ⚠️  CONS
# -------------------------------------------------------------
# - SSO not supported
# - Requires Spark infrastructure (e.g., Databricks, EMR, K8s)
# - Needs Snowflake connector JARs

# 🔁  ALTERNATIVES
# -------------------------------------------------------------
# - Use password-based auth (simpler, less secure)
# - Use Snowpark Python (next solution) if only SSO allowed
