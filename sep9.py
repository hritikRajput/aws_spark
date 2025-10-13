import logging
from datetime import datetime
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, to_date
from hdbcli import dbapi

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')

hana_secret={"db_password": "1q9j0571Bq", "db_username": "IT_AWS_GLUE", "db_url": "", "jdbc_driver_name": "com.sap.db.jdbc.Driver"}

snowflake_connection_parameters = {
"account": "AGILENT_PRDBOEX",
"user": "",
"authenticator": "externalbrowser",
"warehouse": "BW_001_DV1",
"database": "BW_001_DEV",
"schema": "SLV_DATA"
}
hana_host = ""
hana_port = 30041
hana_user = hana_secret["db_username"]
hana_pass = hana_secret["db_password"]
hana_schema = "ecc_hana"
table_name = "kna1"

chunk_size = 10000
target_prefix = "RITIK_HANA_TO_SF_"

def hana_to_snowflake_type(hana_type):
    hana_type = hana_type.upper()
    if hana_type in ("VARCHAR", "NVARCHAR", "CHAR", "NCHAR", "ALPHANUM", "STRING"):
        return "STRING"
    elif hana_type in ("INTEGER", "INT", "TINYINT", "SMALLINT", "BIGINT"):
        return "INTEGER"
    elif hana_type in ("DECIMAL", "SMALLDECIMAL", "REAL", "DOUBLE", "FLOAT"):
        return "FLOAT"
    elif hana_type in ("DATE"):
        return "DATE"
    elif hana_type == "TIME":
        return "TIME"
    elif hana_type in ("SECONDDATE", "TIMESTAMP", "LONGDATE"):
        return "TIMESTAMP_NTZ"
    elif hana_type == "BOOLEAN":
        return "BOOLEAN"
    else:
        return "STRING"


def ensure_target_table_exists(sf_session, table_name, hana_cursor):
    full_table_name = f"{target_prefix}{table_name.upper()}"
    # Check if the table exists in Snowflake
    exists = sf_session.sql(f"SHOW TABLES LIKE '{full_table_name}'").collect()
    if exists:
        logging.info(f"Target table '{full_table_name}' already exists.")
        return
    logging.info(f"Creating target table '{full_table_name}'")

    # Build column definitions based on HANA cursor description
    column_defs = []
    for col in hana_cursor.description:
        col_name = col[0]
        hana_type_code = col[1]
        # Some HANA drivers provide type name in col[1], some only code; get type name string from cursor.description. We must rely on col[1] or default to STRING.
        hana_type_name = str(hana_type_code) if isinstance(hana_type_code, str) else ""
        snow_type = hana_to_snowflake_type(hana_type_name) 
        column_defs.append(f'"{col_name}" {snow_type}')
    ddl=f"""CREATE OR REPLACE TABLE {full_table_name} (\n {', '.join(column_defs)}\n)"""
    sf_session.sql(ddl).collect()
    logging.info(f"Created table: {full_table_name}")

def process_table_full_load(table_name, chunk_size, sf_session, hana_cursor):
    try:
        logging.info(f"Processing table: {table_name}")
        query = f"SELECT * FROM {hana_schema}.{table_name} LIMIT 10"
        hana_cursor.execute(query)
        columns = [desc[0] for desc in hana_cursor.description]
        ensure_target_table_exists(sf_session, table_name, hana_cursor)
        while True:
            rows = hana_cursor.fetchmany(chunk_size)
            if not rows:
                break
            converted_rows = [dict(zip(columns, row)) for row in rows]
            snow_df = sf_session.create_dataframe(converted_rows, schema=columns)
            target_table = f"{target_prefix}{table_name.upper()}"
            snow_df.write.mode("append").save_as_table(target_table)
  
        logging.info(f"Finished table: {table_name}")
    except Exception as e:
        logging.error(f"Error processing table {table_name}: {e}")

def main():
    # Create Snowpark session
    sf_session = Session.builder.configs(snowflake_connection_parameters).create()
    # Create HANA connection and cursor
    hana_conn = dbapi.connect(
        address=hana_host,
        port=hana_port,
        user=hana_user,
        password=hana_pass
        )
    hana_cursor = hana_conn.cursor()
    
    process_table_full_load(table_name, chunk_size, sf_session, hana_cursor)
    logging.info("All tables processed successfully.")

if __name__ == "__main__":
    main()