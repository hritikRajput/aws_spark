"""
=====================================================================
 Snowpark (Python) → Snowflake load using SSO (externalbrowser)
=====================================================================

💡 PURPOSE
-----------
Extract data from SAP HANA using Python (hdbcli), save locally as
Parquet in parallel, upload to Snowflake internal stage using
Snowpark's session.file.put(), and load via COPY INTO.

⚙️  AUTH METHOD
---------------
SSO with authenticator='externalbrowser' (supported in Snowpark).

📘  DOC REFERENCES
------------------
- Snowpark file.put(): https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/api/fileoperations
- COPY INTO: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table
- hdbcli (SAP HANA client): https://pypi.org/project/hdbcli/
"""

from snowflake.snowpark import Session
import pandas as pd
from hdbcli import dbapi
import concurrent.futures, os, math, uuid

# -------------------------------------------------------------
# 1️⃣  Configuration
# -------------------------------------------------------------
HANA_CONN = {
    "address": "hana.host",
    "port": 30015,
    "user": "HANA_USER",
    "password": "HANA_PASSWORD"
}

TARGET_TABLE = "TARGET_SCHEMA.TARGET_TABLE"
LOCAL_TMP_DIR = "/tmp/hana_export"
os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

# -------------------------------------------------------------
# 2️⃣  Create Snowpark session (SSO)
# -------------------------------------------------------------
session = Session.builder.configs({
    "account": "<ACCOUNT>",
    "user": "<USER>",
    "authenticator": "externalbrowser",
    "warehouse": "<WAREHOUSE>",
    "database": "<DATABASE>",
    "schema": "<SCHEMA>"
}).create()

# -------------------------------------------------------------
# 3️⃣  Get min/max ID to define partitions
# -------------------------------------------------------------
def get_min_max_key():
    conn = dbapi.connect(**HANA_CONN)
    cur = conn.cursor()
    cur.execute("SELECT MIN(ID), MAX(ID) FROM SCHEMA.TABLE")
    mn, mx = cur.fetchone()
    cur.close(); conn.close()
    return mn, mx

mn, mx = get_min_max_key()
total = mx - mn + 1
num_parts = 16                       # tune based on data size and cores
part_size = math.ceil(total / num_parts)
ranges = [(mn + i*part_size, min(mx, mn + (i+1)*part_size - 1), i) for i in range(num_parts)]

# -------------------------------------------------------------
# 4️⃣  Extract data chunks in parallel and save as Parquet
# -------------------------------------------------------------
def extract_to_parquet(start_id, end_id, part_idx):
    conn = dbapi.connect(**HANA_CONN)
    sql = f"SELECT * FROM SCHEMA.TABLE WHERE ID BETWEEN {start_id} AND {end_id}"
    df = pd.read_sql(sql, conn)
    conn.close()
    file_path = os.path.join(LOCAL_TMP_DIR, f"part_{part_idx:04d}.parquet")
    df.to_parquet(file_path, index=False)
    return file_path

local_files = []
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    futures = [executor.submit(extract_to_parquet, s, e, i) for s, e, i in ranges]
    for f in concurrent.futures.as_completed(futures):
        local_files.append(f.result())

# -------------------------------------------------------------
# 5️⃣  Upload to Snowflake internal stage & load with COPY INTO
# -------------------------------------------------------------
stage_name = f"TEMP_STAGE_{uuid.uuid4().hex[:6]}"
session.sql(f"CREATE OR REPLACE TEMP STAGE {stage_name}").collect()

session.file.put(
    f"{LOCAL_TMP_DIR}/*.parquet",
    f"@{stage_name}",
    parallel=8,
    auto_compress=False
)

session.sql(f"""
COPY INTO {TARGET_TABLE}
FROM @{stage_name}
FILE_FORMAT = (TYPE = 'PARQUET')
ON_ERROR = 'ABORT_STATEMENT'
""").collect()

session.sql(f"DROP STAGE IF EXISTS {stage_name}").collect()

# -------------------------------------------------------------
# ✅  PROS
# -------------------------------------------------------------
# - SSO supported (externalbrowser / Okta / AzureAD)
# - No Spark or cluster dependency
# - Internal stage only (no external cloud bucket)
# - Easy to run anywhere with Python

# ⚠️  CONS
# -------------------------------------------------------------
# - Manual parallel extraction (less scalable than Spark)
# - Limited by local machine/network throughput
# - Must manage staging files manually
# - Slower for very large data volumes (>20M rows)

# 🔁  ALTERNATIVES
# -------------------------------------------------------------
# - Use Spark (previous solution) for large-scale ETL
# - Use Snowpipe if incremental/continuous load is needed
# - Use cloud object store (S3/Azure) if data > hundreds of GB
