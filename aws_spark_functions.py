import json
import time
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from boto3.dynamodb.conditions import Attr, Key
from datetime import datetime
import pyspark
from pyspark.conf import SparkConf
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import re
import logging
import pandas as pd
from hdbcli import dbapi
from awsglue.job import Job
from pyspark.sql.functions import split, col
from awsglue.utils import getResolvedOptions
import sys
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from py4j.java_gateway import java_import
from pprint import pprint
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector


# Create boto3 clients
client_sm = boto3.client("secretsmanager", region_name="us-west-2")

# Set logging level
logging.getLogger().setLevel(logging.INFO)

# Spark/Iceberg configuration for Spark Session
conf = SparkConf() \
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .set("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
    .set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Import the Snowflake Utils Java class
java_import(sc._gateway.jvm, "net.snowflake.spark.snowflake.Utils")
Utils = sc._gateway.jvm.Utils

# Function to generate Snowflake private key
def generate_snowflake_ppk(certificate_string, private_key_passphrase):
    try:
        pkb1 = bytes(certificate_string, 'utf-8')
        p_key = serialization.load_pem_private_key(
            pkb1,
            password=bytes(private_key_passphrase, 'utf-8'),
            backend=default_backend()
        )

        logging.info("Return RSAPrivateKey object for Python connector")
        logging.info("Return base64 string for Spark")

        pkb = p_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption()
        )

        pkb = pkb.decode("UTF-8")
        pkb = re.sub(r"(\n|\r|-----BEGIN PRIVATE KEY-----|-----END PRIVATE KEY-----)", "", pkb)
        return pkb, p_key

    except Exception as e:
        logging.error(f"Exception in generate_snowflake_ppk: {e}")
        raise Exception(f"Exception in generate_snowflake_ppk: {e}")

# Function to generate Snowflake table names and DDLs
def generate_snowflake_tblnames_ddls(df, row, sfOptions, operation_type="generate_ddl"):
    type_mapping = {
        "StringType": "VARCHAR",
        "IntegerType": "INT",
        "LongType": "BIGINT",
        "DoubleType": "DOUBLE",
        "FloatType": "FLOAT",
        "BooleanType": "BOOLEAN",
        "TimestampType": "TIMESTAMP",
        "DateType": "DATE",
        "BinaryType": "BINARY"
    }

    if operation_type != "generate_ddl":
        df = spark.createDataFrame([], StructType([]))

    if row["LOAD_TYPE"] in ["FULL-REFRESH", "INCR", "FULL"]:
        if row["SOURCE1_SCHEMA_NAME"].strip() in ["ECC_HANA", "CRM_HANA"]:
            source_schema_name = row["SOURCE1_SCHEMA_NAME"][:3]

            if row["TARGET1_OBJECT_NAME"] is None or row["TARGET1_OBJECT_NAME"].strip() in ["", "None"]:
                iceberg_table_name = source_schema_name + "_" + replace_special_characters_with_underscore(
                    row["SOURCE1_OBJECT_NAME"]
                )
            else:
                iceberg_table_name = row["TARGET1_OBJECT_NAME"].upper()

            snowflake_table_stg = iceberg_table_name + "_STG"

        else:
            if row["TARGET1_OBJECT_NAME"] is not None or row["TARGET1_OBJECT_NAME"].strip() not in ["", "None"]:
                iceberg_table_name = source_schema_name + "_" + replace_special_characters_with_underscore(
                    row["SOURCE1_OBJECT_NAME"]
                )
                iceberg_table_name = iceberg_table_name.upper()
            else:
                iceberg_table_name = row["TARGET1_OBJECT_NAME"].upper()

            snowflake_table_stg = iceberg_table_name + "_STG"
            source_schema_name = row["SOURCE1_SCHEMA_NAME"]


    elif row["LOAD_TYPE"] == "RECON-FULL":
        if row["SOURCE1_SCHEMA_NAME"] in ["ECC HANA", "CRM HANA"]:
            source_schema_name = row["SOURCE1_SCHEMA_NAME"]
            if row["TARGET1_OBJECT_NAME"] is None or row["TARGET1_OBJECT_NAME"].strip() in ["", "None"]:
                iceberg_table_name = source_schema_name + "_" + replace_special_characters_with_underscore(row["SOURCE1_OBJECT_NAME"])
            else:
                iceberg_table_name = row["TARGET1_OBJECT_NAME"]
            iceberg_table_name = iceberg_table_name.upper()
            snowflake_table_stg = iceberg_table_name + "_RECON"
        else:
            if row["TARGET1_OBJECT_NAME"] is None or row["TARGET1_OBJECT_NAME"].strip() in ["", "None"]:
                iceberg_table_name = source_schema_name + "_" + replace_special_characters_with_underscore(row["SOURCE1_OBJECT_NAME"])
            else:
                iceberg_table_name = row["TARGET1_OBJECT_NAME"]
            iceberg_table_name = iceberg_table_name.upper()
            snowflake_table_stg = iceberg_table_name + "_RECON"
            source_schema_name = row["SOURCE1_SCHEMA_NAME"]
        snowflake_final_tbl_schema = sfOptions["sfSchema"]
        logging.info(f"recon table schema name: {sfOptions['sfTemp_Schema']}")

    logging.info(f"iceberg_table_name: {iceberg_table_name}")
    logging.info(f"snowflake_table_stg: {snowflake_table_stg}")
    pprint(sfOptions)

    if row["LOAD_TYPE"] == "RECON-FULL":
        logging.info(f"load type is {row['LOAD_TYPE']} hence no need to get column names")
        snowflake_column_names_df_pandas = []
        sfOptions["sfSchema"] = snowflake_final_tbl_schema
    else:
        logging.info(f"load type is {row['LOAD_TYPE']} hence need to get column names")
        # Check if the Iceberg table exists in Snowflake and get list of columns
        snowflake_column_names_df = spark.read \
            .format("snowflake") \
            .options(**sfOptions) \
            .option("query", f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{sfOptions['sfSchema']}'
                AND TABLE_NAME = '{iceberg_table_name.upper()}'
            """) \
            .load()
        snowflake_column_names_df_pandas = snowflake_column_names_df.toPandas()
        logging.info(f"snowflake_column_names_df_pandas:\n{snowflake_column_names_df_pandas}")
    logging.info("initialize all variables")
    drop_ddl = ""
    create_ddl = ""
    iceberg_ddl = ""
    logging.info("print df head")
    print(df.head(0))
    schema_fields = df.schema.fields
    print("schema field: " + str(schema_fields))
    print(df.head(5))
    if df.head(1):
        # Drop staging table DDL
        drop_ddl = f"DROP TABLE IF EXISTS {sfOptions['sfTemp_Schema']}.{snowflake_table_stg};"
        # Create staging table DDL
        create_ddl = f"CREATE TABLE IF NOT EXISTS {sfOptions['sfTemp_Schema']}.{snowflake_table_stg} (\n"
        for field in df.schema.fields:
            snowflake_type = map_spark_to_snowflake_type(field.dataType)
            create_ddl += f"    {field.name} {snowflake_type},\n"
        create_ddl = create_ddl.rstrip(",\n") + "\n);"
        # Create Iceberg Table DDL
        iceberg_ddl = f"CREATE ICEBERG TABLE IF NOT EXISTS {sfOptions['sfSchema']}.{iceberg_table_name} (\n"
        for field in df.schema.fields:
            snowflake_type = map_spark_to_snowflake_type(field.dataType)
            iceberg_ddl += f"    {field.name} {snowflake_type},\n"
        iceberg_ddl = iceberg_ddl.rstrip(",\n") + "\n)"
        iceberg_ddl += f"""
        CATALOG = 'SNOWFLAKE'
        EXTERNAL_VOLUME = '{row['snowflake_extvol']}'
        BASE_LOCATION = 'silver/data/structured/{source_schema_name.lower()}/{iceberg_table_name.lower()}'\n
        CHANGE_TRACKING = TRUE;"""
        logging.info(f"drop_ddl: {drop_ddl}")
        logging.info(f"create_ddl: {create_ddl}")
        logging.info(f"iceberg_ddl: {iceberg_ddl}")
        logging.info(f"snowflake_table_stg: {snowflake_table_stg}")
        logging.info(f"iceberg_table_name: {iceberg_table_name}")

    return drop_ddl, create_ddl, iceberg_ddl, snowflake_table_stg, iceberg_table_name, snowflake_column_names_df_pandas

def get_glue_job_name_and_worker_count():
    # Get the job name from job arguments
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job_name = args['JOB_NAME']

    # Initialize Glue client
    client = boto3.client('glue')

    # Get job details
    response = client.get_job(JobName=job_name)
    worker_count = response['Job']['NumberOfWorkers']

    logging.info(f"Running Job Name: {job_name}, Number of Workers: {worker_count}")
    return job_name, worker_count

# Function: Run Snowflake query using Spark
def run_snowflake_query_to_df_spark(sfOptions, query):
    """
    # Executes a Snowflake SQL query using Spark
    # Returns a DataFrame for SELECT queries.
    # Returns number of affected rows for DML queries (INSERT, UPDATE, DELETE).
    """
    try:
        logging.info(f"Executing query via Spark Snowflake connector: {query}")
        df = spark.read \
            .format("snowflake") \
            .options(**sfOptions) \
            .option("query", query) \
            .load()
        df=df.toPandas()
        print(df.head(2))
        return df

    except Exception as e:
        logging.exception("An error occurred in function run_snowflake_query_to_df_spark: " + str(e))
        raise


def run_snowflake_query_to_df(sfOptions, query):
    """
    Executes a Snowflake SQL query. Returns a DataFrame for SELECT queries,
    and number of affected rows for DML queries (UPDATE, INSERT, DELETE).
    """
    try:
        conn = snowflake.connector.connect(
            user=sfOptions['sfUser'],
            authenticator='SNOWFLAKE_JWT',
            private_key=sfOptions['pem_private_key'],
            account=sfOptions['sfAccount'],
            warehouse=sfOptions['sfWarehouse'],
            database=sfOptions['sfDatabase'],
            schema=sfOptions['sfSchema']
        )

        logging.info(f"Executing query: {query}")
        cursor = conn.cursor()
        cursor.execute(query)

        query_clean = query.strip().lower()
        if query_clean.strip().lower().startswith("select") or query_clean.lower().startswith("with"):
            df = cursor.fetch_pandas_all()
            print("Print top 2 rows")
            print(df.head(2))
            return df
        else:
            affected_rows = cursor.rowcount
            logging.info(f"Query executed successfully. Rows affected: {affected_rows}!")
            return affected_rows
    except Exception as e:
        logging.exception("An error occurred in function run_snowflake_query_to_df: " + str(e))
        raise

    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# Function to replace special character with underscore in string
def replace_special_characters_with_underscore(string_value):
    # Replace special characters with underscores
    modified_string = re.sub(r'[^A-Za-z0-9_]', '_', string_value)
    # Remove leading and trailing underscores
    modified_string = re.sub(r'^_+|_+$', '', modified_string)
    return modified_string.strip()


# Function to infer data type
def infer_data_type(value):
    if value is None or value == '':
        return StringType()
    try:
        int(value)
        return IntegerType()
    except ValueError:
        pass
    try:
        float(value)
        return DoubleType()
    except ValueError:
        pass
    return StringType()


# Function to convert values to appropriate types
def convert_value(value, data_type):
    if value is None or value == '':
        return None
    if data_type == IntegerType():
        return int(value)
    if data_type == DoubleType():
        return float(value)
    return value


def fetch_athena_config(database_name, account_env_number):
    athena_query_config = "select * from integration.aws_etl_framework_load_config"
    df_config = execute_athena_query_with_results_pandas(athena_query_config, 'integration', account_env_number)
    print(df_config.head())
    return df_config


# Supportive Function of Func generate_athena_iceberg_table_ddl
def map_to_athena_type(data_type):
    if isinstance(data_type, StringType):
        return "STRING"
    elif isinstance(data_type, IntegerType):
        return "INT"
    elif isinstance(data_type, DoubleType):
        return "DOUBLE"
    elif isinstance(data_type, FloatType):
        return "FLOAT"
    elif isinstance(data_type, BooleanType):
        return "BOOLEAN"
    elif isinstance(data_type, TimestampType):
        return "TIMESTAMP"
    elif isinstance(data_type, DateType):
        return "DATE"
    elif isinstance(data_type, ByteType):
        return "TINYINT"
    elif isinstance(data_type, ShortType):
        return "SMALLINT"
    elif isinstance(data_type, LongType):
        return "BIGINT"
    elif isinstance(data_type, DecimalType):
        return f"DECIMAL({data_type.precision}, {data_type.scale})"
    elif isinstance(data_type, ArrayType):
        element_type = map_to_athena_type(data_type.elementType)
        return f"ARRAY<{element_type}>"
    elif isinstance(data_type, MapType):
        key_type = map_to_athena_type(data_type.keyType)
        value_type = map_to_athena_type(data_type.valueType)
        return f"MAP<{key_type}, {value_type}>"
    elif isinstance(data_type, StructType):
        fields = [f"{field.name}:{map_to_athena_type(field.dataType)}" for field in data_type.fields]
        return f"STRUCT<{', '.join(fields)}>"
    elif isinstance(data_type, BinaryType):
        return "BINARY"
    else:
        return "STRING"
    

# Function to form Athena table name
def form_athena_table_name(source_schema_name, source_table_name, target_schema_name):
    """
    Forms Athena schema and table names based on the source schema and table names.
    """
    # Convert input schema and table names to lowercase
    source_schema_name = source_schema_name.lower()
    source_table_name = source_table_name.lower()

    # Remove special characters from target table name
    #source_table_name = source_table_name.replace("/", "_").replace("\\", "")
    source_table_name = replace_special_characters_with_underscore(source_table_name)

    # Replace source schema name if it matches ECC HANA or CRM HANA
    if source_schema_name == "ecc hana":
        source_schema_name = "ecc"
    elif source_schema_name == "crm hana":
        source_schema_name = "crm"

    # Form the Athena table and schema names
    athena_table_name = f"{source_schema_name}_{source_table_name}"
    athena_schema_name = target_schema_name

    return athena_schema_name, athena_table_name


# Function to delete additional DataFrame columns not present in Athena table
def delete_additional_df_columns(athena_schema_name, athena_table_name, df, account_env_number):
    """
    Deletes columns from a DataFrame that do not exist in the Athena table.
    """
    athena_table_columns_query = f"""
        SELECT LISTAGG(column_name, ',') WITHIN GROUP (ORDER BY ordinal_position) AS column_names
        FROM information_schema.columns
        WHERE table_schema = '{athena_schema_name}'
          AND table_name = '{athena_table_name}'
    """

    logging.info(f"Start executing Athena table columns query: {athena_table_columns_query}")
    athena_table_columns_df = execute_athena_query_with_results_pandas(
        athena_table_columns_query, 'integration', account_env_number
    )
    logging.info("Completed execution of Athena table columns query")

    # Extract column names from Athena result
    athena_columns = athena_table_columns_df['column_names'].iloc[0].split(',')

    # Get DataFrame columns (lowercase and sanitized)
    df_columns_lower = [replace_special_characters_with_underscore(col.lower()) for col in df.columns]

    # Find matching columns while preserving order
    matching_columns_athena_format = [
        col for col in athena_columns if col in df_columns_lower
    ]
    matching_column_df_format = [df_columns_lower[col] for col in matching_columns_athena_format]

    #convert matching_columns_df to string, add double quotes before and after each column name
    matching_column_df_format = [f'"{col}"' for col in matching_column_df_format]
    matching_df = ', '.join(matching_column_df_format)

    # Convert matching columns to comma-separated string
    matching_columns_athena_format_str = ",".join(matching_columns_athena_format)

    # Delete columns from DataFrame that exist in df but not in Athena
    columns_to_delete = [col for col in df.columns if col.lower() not in athena_columns]
    logging.info(f"Columns to delete: {columns_to_delete}")
    new_df = df.drop(columns=columns_to_delete)

    return new_df



# Function to retrieve column info from HANA database
def retrieve_columns_from_hana(source_table_name, source_schema_name, db_url, db_username, db_password):
    """
    Retrieves column names and primary key columns from a HANA table.
    """
    # Getting HANA host name and port number
    hana_host_name = db_url.split("//")[1].split(":")[0]
    hana_port_num = int(db_url.split(":")[2].split("/")[0])

    # Establish a connection to the HANA database
    connection = dbapi.connect(
        address=hana_host_name,
        port=hana_port_num,
        user=db_username,
        password=db_password
    )

    # Define query to fetch table and primary key columns
    query = f"""
        SELECT
            STRING_AGG(B.COLUMN_NAME, ',' ORDER BY B.POSITION) AS PRIMARYKEY_COLUMNS,
            STRING_AGG(A.COLUMN_NAME, ',' ORDER BY A.POSITION) AS TABLE_COLUMN_NAMES
        FROM TABLE_COLUMNS A
        LEFT JOIN SYS.CONSTRAINTS B
            ON A.TABLE_NAME = B.TABLE_NAME
           AND A.SCHEMA_NAME = B.SCHEMA_NAME
           AND A.COLUMN_NAME = B.COLUMN_NAME
           AND B.IS_PRIMARY_KEY = 'TRUE'
        WHERE A.TABLE_NAME = '{source_table_name}'
          AND A.SCHEMA_NAME = '{source_schema_name}';
    """

    logging.info(f"Executing HANA metadata query for table {source_table_name}")
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    connection.close()

    return result



# Execute the query
cursor = connection.cursor()
sql_exec_status = cursor.execute(query)

# Fetch the results into a DataFrame
result = cursor.fetchall()
hana_columns_df = pd.DataFrame(result, columns=['PRIMARYKEY_COLUMNS', 'TABLE_COLUMN_NAMES'])

# Print the structure of hana_columns_df for debugging
logging.info("Print the structure of hana_columns_df for debugging")
print(hana_columns_df.head())

# Extract the primary key columns and table columns as comma-separated strings
primary_key_columns = hana_columns_df['PRIMARYKEY_COLUMNS'].iloc[0]
table_columns = hana_columns_df['TABLE_COLUMN_NAMES'].iloc[0]

# Print the primary key columns and table columns
print("Primary Key Columns:", primary_key_columns)
print("Table Columns:", table_columns)

# Close the connection
cursor.close()
connection.close()

return primary_key_columns, table_columns


# Prepare the Athena DML statement
def prepare_athena_write_statements(athena_columns_df, table_columns_df, table_columns_str,
                                    primary_key_str, target_schema_name, target_table_name):

    # Check if athena_columns_df is empty
    if athena_columns_df.empty:
        # Convert table columns list to lower case and remove quotes
        table_columns_list = table_columns_str.split(',')
        table_columns_list = [col.replace('"', '').lower() for col in table_columns_list]
        logging.info("prepare_athena_write_statements :- HANA Table columns: " + str(table_columns_list))

        # Use table columns list as matching columns
        matching_columns = set(table_columns_list)
    else:
        # Convert athena_columns_df column names to lower case and remove quotes
        athena_columns_str = athena_columns_df['column_names'].iloc[0]
        athena_columns_list = [col.replace('"', '').lower() for col in athena_columns_str.split(',')]
        logging.info("Athena columns: " + str(athena_columns_list))

        # Convert table_columns_list to lower case and remove quotes
        table_columns_list = table_columns_str.split(',')
        table_columns_list = [replace_special_characters_with_underscore(col.lower()) for col in table_columns_list]

        # Append "row_id" to the hana column list
        table_columns_list.append("row_id")
        logging.info("prepare_athena_write_statements :- HANA Table columns: " + str(table_columns_list))

        # Convert primary_key_str to a list of strings
        primary_key_list = primary_key_str.split(',')
        primary_key_list = [col.replace('"', '').lower().strip() for col in primary_key_list]
        logging.info("prepare_athena_write_statements :- Primary key columns: " + str(primary_key_list))

        # Find matching columns
        matching_columns = set(table_columns_list).intersection(set(athena_columns_list))
        logging.info("prepare_athena_write_statements :- Matching columns: " + str(matching_columns))

    # Filter out unwanted columns
    filtered_columns = [col for col in matching_columns if col not in ['delete_flag', 'insert_ts', 'update_ts']]
    logging.info("Filtered columns: " + str(filtered_columns))

    # Add logic to update update_ts with current timestamp in case of UPDATE
    update_set_clause = ', '.join([f'target.{col} = source.{col}' for col in filtered_columns] +
                                  ['target.update_ts = current_timestamp', "target.delete_flag = 'N'"])

    # Add logic to insert both update_ts and insert_ts with current timestamp in case of INSERT
    insert_columns = filtered_columns + ['insert_ts', 'update_ts']
    insert_values = [f'source.{col}' for col in filtered_columns] + ['current_timestamp', 'current_timestamp']

    # Generate the ON clause dynamically based on primary_key_list
    on_clause = ' AND '.join([f'target.{col} = source.{col}' for col in primary_key_list])

    # Generate the MERGE query using the filtered columns list
    merge_query = f"""
    MERGE INTO glue_catalog.{target_schema_name}.{target_table_name} AS target
    USING dataFrameTempView AS source
    ON {on_clause}
    WHEN MATCHED THEN
        UPDATE SET {update_set_clause}
    WHEN NOT MATCHED THEN
        INSERT ({','.join(insert_columns)})
        VALUES ({','.join(insert_values)});
    """
    logging.info("Athena Merge query: " + merge_query)

    # Generate INSERT query using the filtered columns list
    insert_query = f"""
    INSERT INTO {target_schema_name}.{target_table_name} ({','.join(insert_columns)})
    SELECT * FROM dataFrameTempView;
    """

    return merge_query


def generate_hana_select_statement(load_type, hana_columns_list, source_table_name,
                                   source_schema_name, hana_recon_query,
                                   table_columns_decimal_float):
    def add_alias(col):
        processed_col = replace_special_characters_with_underscore(col.lower())
        if '.' in col:
            return f'{col} AS {processed_col}'
        else:
            return f'{col}'

    if load_type == 'RECON-FULL':
        logging.info("generate_hana_select_statement for load type: HANA Select query: ")
        # Generate the HANA select statement dynamically based on matching columns
        hana_select_statement = f"""
        SELECT {', '.join([add_alias(col) for col in hana_columns_list])}
        FROM {source_schema_name}.{source_table_name}
        """
        return hana_select_statement



# Function to execute a query in HANA
def execute_hana_query(hana_query, db_url, db_username, db_password, jdbc_driver_name,
                       source_table_name, source_schema_name, predicates, load_type):
    try:
        logging.info("HANA Query Starts = " + str(datetime.now()))
        logging.info("HANA SQL: " + hana_query)
        logging.info("Starting HANA Query")

        # Define connection properties
        properties = {
            "user": db_username,
            "password": db_password,
            "driver": jdbc_driver_name
        }

        # If predicates are not provided, fetch them dynamically
        if not predicates:
            logging.info("Stored procedure execution starts = " + str(datetime.now()))

            # Getting HANA host name and port number
            hana_host_name = db_url.split("//")[1].split(":")[0]
            hana_port_num = db_url.split(":")[2].split("/")[0]
            logging.info(f"Creating HANA hdbcli Connection = {str(datetime.now())}")

            conn = dbapi.connect(
                address=hana_host_name,
                port=int(hana_port_num),
                user=db_username,
                password=db_password
            )

            cursor = conn.cursor()
            sql_exec_status = cursor.execute(hana_query)
            logging.info("Stored procedure execution SQL status: " + str(sql_exec_status))

            # Execute predicate retrieval query
            predicates_filters = f"""(
                SELECT STRING_AGG(PREDICATES, '||') AS PREDICATES
                FROM "EBI"."T_HANA_REPLICATION_FRAMEWORK"
                WHERE SCHEMA_NAME='{source_schema_name}'
                AND TABLE_NAME='{source_table_name}'
                AND LOAD_TYPE='{load_type}'
            ) AS subquery"""

            logging.info("Predicate retrieval query: " + predicates_filters)

            # Read predicates into Spark DataFrame
            predicates_df = spark.read.jdbc(
                url=db_url,
                table=predicates_filters,
                properties=properties
            )

            logging.info("Predicates DataFrame:")
            predicates_df.show(truncate=False)

            # Extract the predicate string into a list
            predicates_list_df = predicates_df.withColumn(
                "PREDICATES_LIST",
                split(col("PREDICATES"), ",")
            )
            logging.info("Predicates list DataFrame:")
            predicates_list_df.show(truncate=False)

            # Convert DataFrame column to Python list
            predicates_list = predicates_list_df.select("PREDICATES_LIST") \
                .rdd.flatMap(lambda x: x).collect()
            logging.info("Predicates list (raw): " + str(predicates_list))

            # Flatten list of lists and clean up whitespace
            flat_predicates_list = [item.strip() for sublist in predicates_list for item in sublist]
            logging.info("Flat predicates list: " + str(flat_predicates_list))

            return flat_predicates_list

        elif predicates == 'NA':
            logging.info("Data extraction query starts = " + str(datetime.now()))
            hana_df = spark.read.jdbc(
                url=db_url,
                properties=properties,
                table=hana_query
            )
            logging.info("Data extraction query completed = " + str(datetime.now()))
            return hana_df

        else:
            logging.info("Data extraction query starts = " + str(datetime.now()))
            hana_df = spark.read.jdbc(
                url=db_url,
                properties=properties,
                table=hana_query,
                predicates=predicates  # use passed predicates
            )
            logging.info("Data extraction query completed = " + str(datetime.now()))
            return hana_df

    except Exception as e:
        logging.error(f"HANA query failed with error: {e}")
        raise


# Function to replace special characters in Spark DataFrame column names
def replace_special_characters_with_underscore_in_spark_dataframe_column_names(df):
    for col_name in df.columns:
        new_col_name = re.sub(r'[^a-zA-Z0-9_]', '_', col_name)
        df = df.withColumnRenamed(col_name, new_col_name)
    return df






def replace_special_characters_with_underscore_in_spark_dataframe_column_names(df):
    new_column_names = [replace_special_characters_with_underscore(col_name) for col_name in df.columns]
    df = df.toDF(*new_column_names)
    return df


# Function to get the next run_id
def get_next_run_id(database_name, account_env_number):
    query = "SELECT COALESCE(MAX(CAST(run_id AS INTEGER)), 0) + 1 AS next_run_id FROM integration.aws_etl_framework_load_log"
    result_df = execute_athena_query_with_results_pandas(query, database_name, account_env_number)
    if result_df is not None and not result_df.empty:
        return result_df['next_run_id'].iloc[0]
    else:
        logging.error("Failed to retrieve next run_id")
        return None


def get_current_job_run_id(job_name):
    glue_client = boto3.client('glue')
    response = glue_client.get_job_runs(JobName=job_name)
    for job_run in response['JobRuns']:
        if job_run['JobRunState'] == 'RUNNING':
            return job_run['Id']
    return None


def update_load_ctrl_log_entry(sfOptions, row_array, IN_TBL_NAME, IN_LOAD_STAGE):
    try:
        if hasattr(row_array, 'to_dict'):
            row_array = row_array.to_dict()

        if row_array.get('job_run_id') is None:
            logging.error("Cannot create recon log entry without a valid job_run_id")

        # Temporarily set the non-spark private key
        sfOptions['pem_private_key'] = row_array['snowflake_ppk_non_spark']

        # Construct the CALL statement with individual parameters
        update_load_ctrl = f"""CALL INTEGRATION.SP_ETL_FRAMEWORK_TBL_LOADS(
            '{IN_TBL_NAME}',
            '{IN_LOAD_STAGE}',
            '{row_array.get("main_job_run_id", "")}',
            '{row_array.get("job_run_id", "")}',
            '{row_array.get("LOAD_TYPE", "")}',
            '{row_array.get("SOURCE1_SCHEMA_NAME", "")}',
            '{row_array.get("SOURCE1_OBJECT_NAME", "")}',
            '{row_array.get("TARGET1_SCHEMA_NAME", "")}',
            '{row_array.get("TARGET1_OBJECT_NAME", "")}',
            '{row_array.get("INCREMENTAL_FIELD", "")}',
            '{row_array.get("INCREMENTAL_FIELD_MAX_VALUE", "")}',
            '{row_array.get("PARTITIONED_INCREMENTAL_FIELD_MAX_VALUE", "")}',
            '{row_array.get("DELETE_INCREMENTAL_FIELD_MAX_VALUE", "")}',
            '{row_array.get("BATCH_GROUP", "")}',
            '{row_array.get("WORKER_COUNT", "")}',
            '{row_array.get("LOAD_STATUS", "")}',
            '{row_array.get("DELETE_PROCESS_STATUS", "")}',
            '{row_array.get("RECON_PROCESS_STATUS", "")}',
            '{row_array.get("error_details", "")}',
            '{row_array.get("SOURCE1_PRIMARY_KEY", "")}',
            '{row_array.get("predicates_list_archive", "")}',
            '{row_array.get("Partition_count", 0)}',
            '{row_array.get("no_of_records_processed", 0)}',
            '{row_array.get("LOAD_END_TIME", "")}',
            '{row_array.get("DELETE_START_TIME", "")}',
            '{row_array.get("DELETE_END_TIME", "")}',
            '{row_array.get("RECON_START_TIME", "")}',
            '{row_array.get("RECON_END_TIME", "")}',
            '{row_array.get("SNOWFLAKE_START_TIME", "")}',
            '{row_array.get("SNOWFLAKE_END_TIME", "")}',
            '{row_array.get("HANA_START_TIME", "")}',
            '{row_array.get("HANA_END_TIME", "")}',
            '{row_array.get("DELTA_RECORD_COUNT", 0)}',
            '{row_array.get("DELTA_RECORD_SAMPLES", "")}',
            '{row_array.get("SOURCE1_DELETE_TABLE_SCHEMA_NAME", "")}',
            '{row_array.get("SOURCE1_DELETE_TABLE_NAME", "")}',
            '{row_array.get("TARGET1_DELETE_TABLE_SCHEMA_NAME", "")}',
            '{row_array.get("TARGET1_DELETE_TABLE_NAME", "")}',
            '{row_array.get("DATA_RETENTION_DAYS_ARCHIVE", "")}'
        )"""

        logging.info("Executing Snowflake procedure with parameters.")
        run_snowflake_query_to_df(sfOptions, update_load_ctrl)

        # Restore the original private key
        sfOptions['pem_private_key'] = row_array['snowflake_ppk']

    except Exception as e:
        logging.error(f"Failed to execute query: {e}")
        filtered_row_array = {k: v for k, v in row_array.items() if k not in ['snowflake_ppk', 'snowflake_ppk_non_spark']}
        print("Filtered row_array (excluding sensitive keys):", filtered_row_array)
        raise Exception("update_load_ctrl_log_entry failed: " + str(e))


def update_worker_type_no_of_worker_glue_job(job_name, new_worker_type, new_number_of_workers):
    """
    Updates the worker type and number of workers for an AWS Glue job.

    Parameters:
        job_name (str): The name of the Glue job to update.
        new_worker_type (str): The new type of Glue worker to use (e.g., 'G.2X').
        new_number_of_workers (int): The new number of workers to assign to the Glue job.

    Returns:
        dict: A response from the AWS Glue service that includes the status of the update operation.

    Raises:
        Exception: If the job retrieval fails or if the AWS Glue client encounters an error.

    Author:
        Vinay
    """import boto3
import time
import logging
from botocore.exceptions import ClientError, BotoCoreError

def update_worker_type_no_of_worker_glue_job(job_name, new_worker_type, new_number_of_workers):
    """
    Updates the worker type and number of workers for an AWS Glue job.

    Parameters:
        job_name (str): The name of the Glue job to update.
        new_worker_type (str): The new type of Glue worker to use (e.g., 'G.2X').
        new_number_of_workers (int): The new number of workers to assign to the Glue job.

    Returns:
        dict: A response from the AWS Glue service that includes the status of the update operation.

    Raises:
        Exception: If the job retrieval fails or if the AWS Glue client encounters an error.

    Author:
        Vinay
    """

    glue = boto3.client('glue')

    try:
        # Get the current job definition
        response = glue.get_job(JobName=job_name)
        job_definition = response['Job']

        # Prepare update parameters
        job_update_params = {
            'Role': job_definition['Role'],
            'ExecutionProperty': job_definition['ExecutionProperty'],
            'Command': job_definition['Command'],
            'DefaultArguments': job_definition['DefaultArguments'],
            'WorkerType': new_worker_type,
            'NumberOfWorkers': new_number_of_workers,
        }

        # Include optional settings if present, carefully omitting MaxCapacity
        optional_fields = ['MaxRetries', 'Timeout', 'Connections', 'SecurityConfiguration', 'GlueVersion']
        for field in optional_fields:
            if field in job_definition:
                job_update_params[field] = job_definition[field]

        # Update the job
        update_response = glue.update_job(JobName=job_name, JobUpdate=job_update_params)
        return update_response

    except ClientError as e:
        raise Exception(f"Failed to retrieve or update job settings: {str(e)}")
    except BotoCoreError as e:
        raise Exception(f"An error occurred with the AWS SDK: {str(e)}")


try:
    update_response = update_worker_type_no_of_worker_glue_job(job_name, new_worker_type, new_number_of_workers)
    print("Update Successful:", update_response)
except Exception as e:
    print("Error:", e)


def trigger_glue_jobs(job_name, worker_type, num_workers):
    glue_client = boto3.client('glue')
    job_ids = []

    response = glue_client.start_job_run(
        JobName=job_name,
        Arguments={
            '--WorkerType': worker_type,  # Ensure parameter accepted by the job
            '--NumberOfWorkers': str(num_workers)
        }
    )

    job_ids.append(response['JobRunId'])
    print(f"Triggered Glue job {response['JobRunId']} with ({worker_type}) workers and ({num_workers}) nodes.")
    return job_ids


def check_jobs_status(job_name, job_ids):
    time.sleep(20)  # Initial wait time before first check
    glue_client = boto3.client('glue')

    for job_id in job_ids:
        status = glue_client.get_job_run(JobName=job_name, RunId=job_id)['JobRun']['JobRunState']

        while status not in ['SUCCEEDED', 'FAILED', 'STOPPED']:
            print(f"Job [{job_id}] is {status}. Checking again in 10 seconds.")
            time.sleep(20)
            status = glue_client.get_job_run(JobName=job_name, RunId=job_id)['JobRun']['JobRunState']

        print(f"Job [{job_id}] has completed with status ({status}).")


# Function to send email notifications
def send_email(sns_topic_arn, email_subject, email_body):
    logging.info("send_email starts")
    logging.info("sns_topic_arn: " + sns_topic_arn)
    logging.info("email_subject: " + email_subject)
    logging.info("email_body: " + email_body)

    sns_client = boto3.client('sns')
    try:
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject=email_subject,
            Message=email_body
        )
        logging.info("The email sent successfully.")
    except Exception as e:
        logging.error(f"Exception in send_email. Exception: {e}")


# Function to execute Spark SQL query with retries
def execute_spark_sql(query, max_attempts=2):
    """
    Execute a Spark SQL query with retry logic.

    Args:
        query (str): SQL query string.
        max_attempts (int): Number of times to retry the query.

    Returns:
        DataFrame: Result of the executed Spark SQL query.
    """
    attempt = 0

    while attempt < max_attempts:
        try:
            attempt += 1
            logging.info(f"Attempt {attempt}: Executing query.")
            result_df = spark.sql(query)
            logging.info("Query executed successfully.")
            return result_df
        except Exception as e:
            logging.error(f"Attempt {attempt}: Error executing query - {str(e)}")
            if attempt >= max_attempts:
                logging.error("Max attempts reached, failing the query execution.")
                raise Exception("Query execution failed after several retries.") from e
            logging.info("Retrying the query...")


def fetch_athena_ctrl_primary_keys(schema_name, object_name, account_env_number):
    """
    Fetches primary keys from an Athena table and formats them as a quoted, comma-separated string.

    Args:
        object_name (str): Name of the source object in the database.
        schema_name (str): Name of the database schema.
        account_env_number (str): Environment account number for database access.

    Returns:
        str: A comma-separated list of quoted primary keys.
    """
    query = f"""
        SELECT column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_schema = '{schema_name}'
        AND tc.table_name = '{object_name}'
        AND tc.constraint_type = 'PRIMARY KEY'
    """

    df = execute_athena_query_with_results_pandas(query, schema_name, account_env_number)
    if df is not None and not df.empty:
        keys = ", ".join([f'"{col}"' for col in df['column_name']])
        return keys
    else:
        logging.warning("No primary keys found for the specified table.")
        return ""

def get_running_job_ids_by_batch_group(job_name, batch_group):
    """
    Retrieves running job run IDs for a specified AWS Glue job filtered by batch group.

    Parameters:
        job_name (str): The name of the Glue job.
        batch_group (str): The batch group to filter the job runs.

    Returns:
        list: A list of job run IDs for the specified batch group.
    """
    # Initialize the Glue client
    glue_client = boto3.client('glue')

    # Get all job runs for the specified job
    try:
        response = glue_client.get_job_runs(JobName=job_name)
    except Exception as e:
        print(f"Error retrieving job runs: {e}")
        return None

    running_jobs = []

    # Filter and process job runs
    for job_run in response['JobRuns']:
        if job_run['JobRunState'] == 'RUNNING':
            # Access the '--batch_group' argument
            current_batch_group = job_run['Arguments'].get('--batch_group', None)
            if current_batch_group == batch_group:
                running_jobs.append(job_run['Id'])
                break  # stop after finding the first matching job

    return running_jobs


def prepare_athena_client_merge_statements(athena_columns_list, primary_key_str, target_schema_name, target_table_name):
    """
    Prepares an Athena MERGE SQL statement for syncing data from a source table to a target table.

    Parameters:
        athena_columns_list (list): List of columns in Athena.
        primary_key_str (str): Comma-separated list of primary keys.
        target_schema_name (str): Target schema name.
        target_table_name (str): Target table name.

    Returns:
        str: The generated MERGE SQL statement.
    """

    # Convert primary_key_str to a list of strings
    primary_key_list = primary_key_str.split(',')
    primary_key_list = [col.replace('"', '').lower().strip() for col in primary_key_list]
    logging.info("prepare_athena_write_statements: Primary key columns: " + str(primary_key_list))

    # Find matching columns
    matching_columns = athena_columns_list
    logging.info("prepare_athena_write_statements: Matching columns: " + str(matching_columns))

    # Filter out unwanted columns
    filtered_columns = [col for col in matching_columns if col not in ['delete_flag', 'insert_ts', 'update_ts']]
    logging.info("Filtered columns: " + str(filtered_columns))

    # Add logic to update existing rows with current timestamp in case of UPDATE
    update_set_clause = ", ".join([f"target.{col}=source.{col}" for col in filtered_columns]) + \
                        ", update_ts=current_timestamp, delete_flag='N'"

    # Add logic to insert both update_ts and insert_ts with current timestamp in case of INSERT
    insert_columns = filtered_columns + ['insert_ts', 'update_ts']
    insert_values = [f"source.{col}" for col in filtered_columns] + ['current_timestamp', 'current_timestamp']

    # Generate the ON clause dynamically based on primary_key_list
    on_clause = ' AND '.join([f"target.{col} = source.{col}" for col in primary_key_list])

    # Generate the MERGE query using the filtered columns list
    merge_query = f"""
    MERGE INTO {target_schema_name}.{target_table_name} AS target
    USING brz_data.{target_table_name} AS source
    ON {on_clause}
    WHEN MATCHED THEN
        UPDATE SET {update_set_clause}
    WHEN NOT MATCHED THEN
        INSERT ({', '.join(insert_columns)})
        VALUES ({', '.join(insert_values)})
    """

    logging.info("Athena Merge query: " + merge_query)

    # Also prepare a simpler INSERT query if needed
    insert_query = f"""
    INSERT INTO {target_schema_name}.{target_table_name} ({', '.join(insert_columns)})
    SELECT * FROM dataFrameTempView
    """

    return merge_query


def retrieve_columns_from_hana2(source_table_name, source_schema_name, db_url, db_username, db_password):
    """
    Retrieve primary key columns and all table columns from SAP HANA.
    """
    try:
        # Getting HANA Host name and Port Number
        hana_host_name = db_url.split("//")[1].split(":")[0]
        hana_port_num = int(db_url.split(":")[2].split("/")[0])

        # Establish a connection to the HANA database using dbapi
        connection = dbapi.connect(
            address=hana_host_name,
            port=hana_port_num,
            user=db_username,
            password=db_password
        )

        # Define the query with placeholders for table and schema name
        query = f"""
        SELECT
            '' || STRING_AGG(B.COLUMN_NAME, ', ' ORDER BY B.POSITION) || '' AS PRIMARYKEY_COLUMNS,
            '' || STRING_AGG(A.COLUMN_NAME, ', ' ORDER BY A.POSITION) || '' AS TABLE_COLUMN_NAMES
        FROM TABLE_COLUMNS A
        LEFT JOIN SYS.CONSTRAINTS B
        ON A.TABLE_NAME = B.TABLE_NAME
        AND A.SCHEMA_NAME = B.SCHEMA_NAME
        AND A.COLUMN_NAME = B.COLUMN_NAME
        AND B.IS_PRIMARY_KEY = 'TRUE'
        WHERE A.TABLE_NAME = '{source_table_name}'
        AND A.SCHEMA_NAME = '{source_schema_name}'
        """

        # Execute the query
        cursor = connection.cursor()
        cursor.execute(query)

        # Fetch the results into a DataFrame
        result = cursor.fetchall()
        if result:
            hana_columns_df = pd.DataFrame(result, columns=['PRIMARYKEY_COLUMNS', 'TABLE_COLUMN_NAMES'])

            # Extract the primary key columns and table columns as comma-separated strings
            primary_key_columns = hana_columns_df['PRIMARYKEY_COLUMNS'].iloc[0]
            table_columns = hana_columns_df['TABLE_COLUMN_NAMES'].iloc[0]

            # Close the cursor and the connection
            cursor.close()
            connection.close()

            return primary_key_columns, table_columns
        else:
            # Close the cursor and the connection if result is empty
            cursor.close()
            connection.close()
            return None, None

    except Exception as e:
        return None, None


def generate_delete_query_based_primary(schema_name, target_table_name, delete_table_name,
                                        primary_keys, delete_incremental_field_max_value,
                                        athena_schema_name_archive):
    """
    Generate a delete SQL query for HANA based on primary keys and max incremental field.
    """
    primary_key_conditions = ",".join(primary_keys)
    primary_key_select = ",".join(primary_keys)

    query = f"""
    UPDATE {schema_name}.{target_table_name}
    SET delete_flag = 'Y', update_ts = current_timestamp
    WHERE ({primary_key_conditions}) IN (
        SELECT {primary_key_select}
        FROM {athena_schema_name_archive}.{delete_table_name}
        WHERE row_id > {delete_incremental_field_max_value}
    )
    """

    return query


def execute_hana_query_hdbcli(hana_query, db_url, db_username, db_password, jdbc_driver_name):
    """
    Execute a HANA SQL query and return results as DataFrame.
    """
    try:
        logging.info("HANA Query Starts " + str(datetime.now()))
        logging.info("HANA SQL: " + hana_query)
        logging.info("Starting HANA query...")

        logging.info("Data extraction query starts " + str(datetime.now()))
        hana_host_name = db_url.split("//")[1].split(":")[0]
        hana_port_num = int(db_url.split(":")[2].split("/")[0])

        conn = dbapi.connect(
            address=hana_host_name,
            port=hana_port_num,
            user=db_username,
            password=db_password
        )

        cursor = conn.cursor()
        cursor.execute(hana_query)
        rows = cursor.fetchall()
        logging.info("Data extraction query completed " + str(datetime.now()))

        # Create DataFrame with specified column names
        column_names = ['HANA_RECON_INCR', 'HANA_RECON_FULL', 'ATHENA_HASH_QUERY']
        df = pd.DataFrame(rows, columns=column_names)
        print(df.columns)

        hana_recon_query = str(df.loc[0, 'HANA_RECON_INCR'])
        athena_recon_query = str(df.loc[0, 'ATHENA_HASH_QUERY'])

        return hana_recon_query, athena_recon_query

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None


def execute_archive_table_delete_hana_query(hana_query, db_url, db_username, db_password, jdbc_driver_name):
    """
    Execute HANA query for deleting records from archive table.
    """
    try:
        logging.info("HANA Query Starts " + str(datetime.now()))
        logging.info("HANA SQL: " + hana_query)
        logging.info("Starting HANA Query: ")

        hana_host_name = db_url.split("//")[1].split(":")[0]
        hana_port_num = int(db_url.split(":")[2].split("/")[0])

        conn = dbapi.connect(
            address=hana_host_name,
            port=hana_port_num,
            user=db_username,
            password=db_password
        )

        cursor = conn.cursor()
        cursor.execute(hana_query)
        conn.commit()
        cursor.close()
        conn.close()

        logging.info("Archive delete query executed successfully.")
        return True

    except Exception as e:
        logging.error(f"Error executing archive delete query: {e}")
        return False

import sys
import boto3
import logging
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import TimestampType

# ------------------------------------------------------------
# Function 1: Retrieve columns and primary keys from HANA
# ------------------------------------------------------------
def retrieve_columns_from_hana2(source_table_name, source_schema_name, db_url, db_username, db_password):
    """
    Retrieve primary key columns and all table columns from SAP HANA.
    """
    try:
        # Getting HANA Host name and Port Number
        hana_host_name = db_url.split("//")[1].split(":")[0]
        hana_port_num = int(db_url.split(":")[2].split("/")[0])

        # Establish a connection to the HANA database using dbapi
        connection = dbapi.connect(
            address=hana_host_name,
            port=hana_port_num,
            user=db_username,
            password=db_password
        )

        # Define the query with placeholders for table and schema name
        query = f"""
        SELECT
            '' || STRING_AGG(B.COLUMN_NAME, ', ' ORDER BY B.POSITION) || '' AS PRIMARYKEY_COLUMNS,
            '' || STRING_AGG(A.COLUMN_NAME, ', ' ORDER BY A.POSITION) || '' AS TABLE_COLUMN_NAMES
        FROM TABLE_COLUMNS A
        LEFT JOIN SYS.CONSTRAINTS B
        ON A.TABLE_NAME = B.TABLE_NAME
        AND A.SCHEMA_NAME = B.SCHEMA_NAME
        AND A.COLUMN_NAME = B.COLUMN_NAME
        AND B.IS_PRIMARY_KEY = 'TRUE'
        WHERE A.TABLE_NAME = '{source_table_name}'
        AND A.SCHEMA_NAME = '{source_schema_name}'
        """

        # Execute the query
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()

        if result:
            hana_columns_df = pd.DataFrame(result, columns=['PRIMARYKEY_COLUMNS', 'TABLE_COLUMN_NAMES'])
            primary_key_columns = hana_columns_df['PRIMARYKEY_COLUMNS'].iloc[0]
            table_columns = hana_columns_df['TABLE_COLUMN_NAMES'].iloc[0]
            cursor.close()
            connection.close()
            return primary_key_columns, table_columns
        else:
            cursor.close()
            connection.close()
            return None, None

    except Exception as e:
        logging.error(f"Error retrieving columns from HANA: {e}")
        return None, None


# ------------------------------------------------------------
# Function 2: Generate Delete Query
# ------------------------------------------------------------
def generate_delete_query_based_primary(schema_name, target_table_name, delete_table_name,
                                        primary_keys, delete_incremental_field_max_value,
                                        athena_schema_name_archive):
    """
    Generate a delete SQL query for HANA based on primary keys and max incremental field.
    """
    primary_key_conditions = ",".join(primary_keys)
    primary_key_select = ",".join(primary_keys)

    query = f"""
    UPDATE {schema_name}.{target_table_name}
    SET delete_flag = 'Y', update_ts = current_timestamp
    WHERE ({primary_key_conditions}) IN (
        SELECT {primary_key_select}
        FROM {athena_schema_name_archive}.{delete_table_name}
        WHERE row_id > {delete_incremental_field_max_value}
    )
    """

    return query


# ------------------------------------------------------------
# Function 3: Execute HANA Query using hdbcli
# ------------------------------------------------------------
def execute_hana_query_hdbcli(hana_query, db_url, db_username, db_password, jdbc_driver_name):
    """
    Execute a HANA SQL query and return results as DataFrame.
    """
    try:
        logging.info("HANA Query Starts " + str(datetime.now()))
        logging.info("HANA SQL: " + hana_query)

        hana_host_name = db_url.split("//")[1].split(":")[0]
        hana_port_num = int(db_url.split(":")[2].split("/")[0])

        conn = dbapi.connect(
            address=hana_host_name,
            port=hana_port_num,
            user=db_username,
            password=db_password
        )

        cursor = conn.cursor()
        cursor.execute(hana_query)
        rows = cursor.fetchall()

        column_names = ['HANA_RECON_INCR', 'HANA_RECON_FULL', 'ATHENA_HASH_QUERY']
        df = pd.DataFrame(rows, columns=column_names)

        hana_recon_query = str(df.loc[0, 'HANA_RECON_INCR'])
        athena_recon_query = str(df.loc[0, 'ATHENA_HASH_QUERY'])

        cursor.close()
        conn.close()
        return hana_recon_query, athena_recon_query

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None


# ------------------------------------------------------------
# Function 4: Execute Archive Table Delete HANA Query
# ------------------------------------------------------------
def execute_archive_table_delete_hana_query(hana_query, db_url, db_username, db_password, jdbc_driver_name):
    """
    Execute HANA query for deleting records from archive table.
    """
    try:
        logging.info("HANA Query Starts " + str(datetime.now()))
        logging.info("HANA SQL: " + hana_query)

        hana_host_name = db_url.split("//")[1].split(":")[0]
        hana_port_num = int(db_url.split(":")[2].split("/")[0])

        conn = dbapi.connect(
            address=hana_host_name,
            port=hana_port_num,
            user=db_username,
            password=db_password
        )

        cursor = conn.cursor()
        cursor.execute(hana_query)
        conn.commit()
        cursor.close()
        conn.close()

        logging.info("Archive delete query executed successfully.")
        return True

    except Exception as e:
        logging.error(f"Error executing archive delete query: {e}")
        return False


# ------------------------------------------------------------
# Function 5: Execute Stored Procedure in HANA
# ------------------------------------------------------------
def execute_hana_stored_procedure(hana_query, db_url, db_username, db_password):
    """
    Execute a stored procedure or query in SAP HANA using dbapi.
    """
    try:
        logging.info("Starting HANA Query: '%s'", hana_query)
        hana_host_name = db_url.split("//")[1].split(":")[0]
        hana_port_num = int(db_url.split(":")[2].split("/")[0])

        logging.info("Creating HANA hdbcli Connection at %s", str(datetime.now()))
        conn = dbapi.connect(
            address=hana_host_name,
            port=hana_port_num,
            user=db_username,
            password=db_password
        )

        cursor = conn.cursor()
        sql_exec_status = cursor.execute(hana_query)
        logging.info("Stored procedure executed, status: %s", sql_exec_status)

        return sql_exec_status

    except Exception as e:
        raise Exception(f"HANA query failed with error: {e}")


# ------------------------------------------------------------
# Function 6: HANA → Snowflake Insert
# ------------------------------------------------------------
def hana_to_snowflake_insert(
    hana_select_statement,
    db_url,
    db_username,
    db_password,
    jdbc_driver_name,
    predicates,
    row,
    sfOptions
):
    """
    Extract data from SAP HANA using JDBC and insert it into Snowflake.
    """
    try:
        args_array = ['JOB_NAME']
        args = getResolvedOptions(sys.argv, args_array)
        job_name = args['JOB_NAME']
        job_run_id = args['JOB_RUN_ID']
        glue_client = boto3.client('glue')

        spark = SparkSession.builder.getOrCreate()

        # JDBC connection properties
        properties = {
            "user": db_username,
            "password": db_password,
            "driver": jdbc_driver_name
        }

        logging.info(f"Start processing HANA Query: {hana_select_statement}")

        # Read from HANA using Spark JDBC
        df = spark.read.jdbc(
            url=db_url,
            table=hana_select_statement,
            predicates=predicates,
            properties=properties
        )

        no_of_records_processed = df.count()
        logging.info(f"HANA Query completed: {no_of_records_processed} records")

        if no_of_records_processed == 0:
            logging.info("No records to process.")
            hana_start_time = datetime.now()
            hana_end_time = datetime.now()
            load_start_time = '1900-01-01 00:00:00'
            load_end_time = '1900-01-01 00:00:00'
            delete_start_time = '1900-01-01 00:00:00'
            delete_end_time = '1900-01-01 00:00:00'
            recon_start_time = '1900-01-01 00:00:00'
            recon_end_time = '1900-01-01 00:00:00'
            snowflake_start_time = '1900-01-01 00:00:00'
            snowflake_end_time = '1900-01-01 00:00:00'
            delta_record_count = 0
            delta_record_samples = ''
            return no_of_records_processed, hana_start_time, hana_end_time, load_start_time, load_end_time, delete_start_time, delete_end_time, recon_start_time, recon_end_time, snowflake_start_time, snowflake_end_time, delta_record_count, delta_record_samples

        # Clean column names
        df = replace_special_characters_with_underscore_in_spark_dataframe_column_names(df)

        # Add metadata columns
        current_ts = datetime.now()

        def add_column_if_not_exists(df, col_name, col_value):
            if col_name.lower() not in [c.lower() for c in df.columns]:
                df = df.withColumn(col_name, col_value)
                logging.info(f"Column added: {col_name}")
            return df

        if row['LOAD_TYPE'] == 'RECON-FULL':
            df = add_column_if_not_exists(df, "insert_ts", lit(current_ts).cast("timestamp"))
        else:
            df = add_column_if_not_exists(df, "delete_flag", lit("N").cast("string"))
            df = add_column_if_not_exists(df, "insert_ts", lit(current_ts).cast("timestamp"))
            df = add_column_if_not_exists(df, "update_ts", lit(current_ts).cast("timestamp"))

        # Generate Snowflake DDLs
        drop_ddl, create_ddl, iceberg_ddl, snowflake_table_stg, iceberg_table_name, snowflake_column_names_df_pandas = generate_snowflake_tblnames_ddls(df, row, sfOptions, operation_type='generate_ddl')

        # Full-load SQL script
        full_load_script = f"""
            INSERT OVERWRITE INTO {sfOptions['sfSchema']}.{iceberg_table_name}
            SELECT * FROM {sfOptions['sf_temp_Schema']}.{snowflake_table_stg}
        """

        # Convert timestamps to string
        for field in df.schema.fields:
            if isinstance(field.dataType, TimestampType):
                df = df.withColumn(field.name, col(field.name).cast("string"))

        # Repartition based on Glue worker type
        glue_worker_type = glue_client.get_job_run(JobName=args['JOB_NAME'], RunId=args['JOB_RUN_ID'])['JobRun']['WorkerType']
        number_of_workers = glue_client.get_job_run(JobName=args['JOB_NAME'], RunId=args['JOB_RUN_ID'])['JobRun']['NumberOfWorkers']
        num_partitions = int(number_of_workers * 1.5) if glue_worker_type[2] == 'G' else int(number_of_workers)

        df = df.repartition(num_partitions)

        # Convert to DynamicFrame
        hana_start_time = datetime.now()
        dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
        hana_end_time = datetime.now()

        if row['LOAD_TYPE'] == "RECON-FULL":
            reset_delete_flag_query = f"""
            UPDATE {sfOptions['sfSchema']}.{iceberg_table_name}
            SET delete_flag = 'N', update_ts = current_timestamp
            WHERE delete_flag = 'Y'
            """

            # Define the query to compare the recon table with the source table in case of FULL load
            update_delete_flag_query = f"""
            UPDATE {sfOptions['sfSchema']}.{iceberg_table_name}
            SET delete_flag = 'Y', update_ts = current_timestamp
            WHERE row_id NOT IN (
                SELECT recon.row_id
                FROM {sfOptions['sf_temp_Schema']}.{snowflake_table_stg} recon
            )
            """

            # snowflake_postactions = f"BEGIN;{reset_delete_flag_query};{update_delete_flag_query};COMMIT;"
            snowflake_postactions = f"BEGIN;{reset_delete_flag_query};{update_delete_flag_query};COMMIT;"
            snowflake_preactions = f"{drop_ddl};{create_ddl}"
        else:
            snowflake_postactions = f"BEGIN;{full_load_script};COMMIT;"
            snowflake_preactions = f"{drop_ddl};{create_ddl};{iceberg_ddl}"

        # Snowflake connection options
        sf_options = {
            "autopushdown": "on",
            "sfURL": sfOptions['sfURL'],
            "sfUser": sfOptions['sfUser'],
            "pem_private_key": sfOptions['pem_private_key'],
            "sfDatabase": sfOptions['sfDatabase'],
            "sfSchema": sfOptions['sf_temp_Schema'],
            "sf_temp_Schema": sfOptions['sf_temp_Schema'],
            "dbtable": snowflake_table_stg,
            "sfAccount": sfOptions['sfAccount'],
            "sfWarehouse": sfOptions['sfWarehouse'],
            "preactions": snowflake_preactions,
            "postactions": snowflake_postactions
        }

        logging.info("snowflake_preactions: \n" + snowflake_preactions)
        logging.info("snowflake_postactions: \n" + snowflake_postactions)

        logging.info("Writing data to Snowflake...")
        snowflake_start_time = datetime.now()
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="snowflake",
            connection_options=sf_options,
            transformation_ctx="snowflake_node"
        )
        snowflake_end_time = datetime.now()

        if row['RECON_ENABLE_FLAG'] == 'true':
            comparison_filter = """
            CASE
                WHEN b.row_id IS NULL THEN 'Missing row id: '
                WHEN COALESCE(b.delete_flag, 'N') = 'Y' THEN 'Deleted in AWS, active in HANA:'
                WHEN b.row_id IS NOT NULL AND a.hash_value <> b.hash_value THEN 'Mismatch in field values: '
                ELSE 'No Delta'
            END AS delta_record_samples
            """
            recon_columns = 'row_id,hash_value,insert_ts'
        else:
            comparison_filter = """
            CASE
                WHEN b.row_id IS NULL THEN 'Missing row id: '
                WHEN COALESCE(b.delete_flag, 'N') = 'Y' THEN 'Deleted in AWS, active in HANA:'
                ELSE 'No Delta'
            END AS delta_record_samples
            """

        recon_query = f"""
        WITH aggregated_data AS (
            SELECT
                a.row_id,
                {comparison_filter}
            FROM {sfOptions['sf_temp_Schema']}.{snowflake_table_stg} a
            LEFT JOIN {sfOptions['sfSchema']}.{iceberg_table_name} b
                ON a.row_id = b.row_id
            WHERE a.row_id <= (
                SELECT INCREMENTAL_FIELD_MAX_VALUE
                FROM {sfOptions['sf_temp_Schema']}.AWS_ETL_FRAMEWORK_LOAD_CTRL
                WHERE TARGET1_OBJECT_NAME = '{iceberg_table_name}'
            )
        ),
        ranked AS (
            SELECT
                row_id,
                delta_record_samples,
                ROW_NUMBER() OVER (PARTITION BY delta_record_samples ORDER BY row_id) AS rn,
                COUNT(*) OVER (PARTITION BY delta_record_samples) AS total_cnt
            FROM aggregated_data
        )
        SELECT
            MAX(total_cnt) AS delta_record_count,
            delta_record_samples || ':' || 
                LISTAGG(CAST(row_id AS STRING), ',') 
                WITHIN GROUP (ORDER BY row_id) AS delta_record_samples
        FROM ranked
        WHERE delta_record_samples NOT LIKE '%No Delta%'
          AND rn <= 10
        GROUP BY delta_record_samples
        ORDER BY delta_record_count DESC;
        """

        if row['LOAD_TYPE'] == "RECON-FULL":
            recon_start_time = datetime.now()
            sf_options = {
                "autopushdown": "on",
                "sfURL": sfOptions['sfURL'],
                "sfUser": sfOptions['sfUser'],
                "pem_private_key": sfOptions['pem_private_key'],
                "sfDatabase": sfOptions['sfDatabase'],
                "sfSchema": sfOptions['sf_temp_Schema'],
                "sf_temp_Schema": sfOptions['sf_temp_Schema'],
                "sfWarehouse": sfOptions['sfWarehouse']
            }

            df_recon = run_snowflake_query_to_df_spark(sf_options, recon_query)
    
            if df_recon is not None or not df_recon.empty:
                logging.info("recon processed, delta record count : " + str(df_recon.loc[0, 'DELTA_RECORD_COUNT']))
                delta_record_count = str(df_recon.loc[0, 'DELTA_RECORD_COUNT'])
                delta_record_samples = df_recon.loc[0, 'DELTA_RECORD_SAMPLES']
                recon_end_time = datetime.now()
            else:
                logging.info("recon processed, delta record count : 0")
                delta_record_count = 0
                delta_record_samples = "No Delta"
                recon_end_time = datetime.now()
        else:
            delta_record_count = 0
            delta_record_samples = "No Delta"
            recon_start_time = row['RECON_START_TIME']
            recon_end_time = row['RECON_END_TIME']

        logging.info("Snowflake load completed.")
        return no_of_records_processed, snowflake_start_time, snowflake_end_time, hana_start_time, hana_end_time, recon_start_time, recon_end_time, delta_record_count, delta_record_samples

    except Exception as e:
        logging.error("Glue Job Failed", exc_info=True)
        raise
        

def hana_to_snowflake_merge(
    row,
    hana_select_query,
    snowflake_columns_list,
    db_url,
    db_username,
    db_password,
    jdbc_driver_name,
    predicates,
    primary_key_str,
    sfOptions
):
    try:
        # Creating array of Glue Job input arguments
        args_array = ['JOB_NAME']
        args = getResolvedOptions(sys.argv, args_array)
        job_name = args['JOB_NAME']
        job_run_id = args['JOB_RUN_ID']
        glue_client = boto3.client('glue')
        from pyspark.sql.functions import lit, col
        from datetime import datetime
        from awsglue.dynamicframe import DynamicFrame
        import logging

        logging.info(f"Starting hana_to_snowflake_merge()")

        # Prepare JDBC properties
        properties = {
            "user": db_username,
            "password": db_password,
            "driver": jdbc_driver_name
        }

        # Read data from HANA using predicates
        df = spark.read.jdbc(
            url=db_url,
            table=hana_select_query,
            predicates=predicates,
            properties=properties
        )

        no_of_records_processed = df.count()
        logging.info(f"Records read from HANA: {no_of_records_processed}")

        if no_of_records_processed == 0:
            logging.info("No records to process.")
            hana_start_time=datetime.now()
            hana_end_time=datetime.now()
            hana_end_time=datetime.now()
            snowflake_start_time=datetime.now()
            snowflake_end_time=datetime.now()
            return no_of_records_processed,snowflake_start_time,snowflake_end_time,hana_start_time,hana_end_time

        # Clean column names
        df = replace_special_characters_with_underscore_in_spark_dataframe_column_names(df)

        # Add metadata columns if missing
        current_ts = datetime.now()

        def add_column_if_not_exists(df, col_name, col_value):
            if col_name.lower() not in [c.lower() for c in df.columns]:
                df = df.withColumn(col_name, col_value)
                logging.info(f"Added column: {col_name}")
            return df

        df = add_column_if_not_exists(df, "delete_flag", lit("N").cast("string"))
        df = add_column_if_not_exists(df, "insert_ts", lit(current_ts).cast("timestamp"))
        df = add_column_if_not_exists(df, "update_ts", lit(current_ts).cast("timestamp"))

        # Generate DDLs and merge query
        drop_ddl, create_ddl, iceberg_ddl, snowflake_table_stg, iceberg_table_name,snowflake_column_names_df,ddls_pandas=generate_snowflake_tblnames_ddls(df,row,sfOptions,operation_type="generate_ddl")

        if row['LOAD_TYPE'] == 'FULL-REFRESH':
            merge_script,insert_script = prepare_snowflake_merge_statements(row,snowflake_columns_list,primary_key_str,iceberg_table_name,snowflake_table_stg)
            load_script=merge_script
            logging.info("Go for insert overwrite: " + insert_script)
        else:
            merge_script,insert_script = prepare_snowflake_merge_statements(row,snowflake_columns_list,primary_key_str,iceberg_table_name,snowflake_table_stg)
            load_script=merge_script
            logging.info("Go for merge: " + load_script)

        # Convert timestamp columns to string
        for field in df.schema.fields:
            if isinstance(field.dataType, TimestampNTZType):
                df = df.withColumn(field.name, col(field.name).cast("string"))

        # Repartition based on Glue worker type
        glue_worker_type = glue_client.get_job_run(JobName=args['JOB_NAME'], RunId=args['JOB_RUN_ID'])['JobRun']['WorkerType']
        number_of_workers = glue_client.get_job_run(JobName=args['JOB_NAME'], RunId=args['JOB_RUN_ID'])['JobRun']['NumberOfWorkers']
        num_partitions = int(glue_worker_type[2]) * 4 * (int(number_of_workers) - 1)

        df = df.repartition(num_partitions)

        # Convert to Glue DynamicFrame
        hana_start_time=datetime.now()
        dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
        hana_end_time=datetime.now()
        snowflake_preactions=f"drop ddl;{create_ddl};{iceberg_ddl};"
        snowflake_postactions=f"BEGIN;{load_script};COMMIT;"

        sf_options = {
            "autopushdown": "on",
            "sfURL": sfOptions['sfURL'],
            "sfRole": sfOptions['sfRole'],
            "pem_private_key": sfOptions['pem_private_key'],
            "sfDatabase": sfOptions['sfDatabase'],
            "sfSchema": sfOptions['sfTemp_Schema'],
            "sfWarehouse": sfOptions['sfWarehouse'],
            "sfTable": snowflake_table_stg,
            "preactions": snowflake_preactions,
            "postactions": snowflake_postactions,
        }
        logging.info("snowflake_preactions: \n" + snowflake_preactions)
        logging.info("snowflake_postactions: \n" + snowflake_postactions)

        snowflake_start_time = datetime.now()

        # Write to Snowflake
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="snowflake",
            connection_options=sf_options,
            transformation_ctx="snowflake_node"
        )

        snowflake_end_time = datetime.now()

        logging.info("Data successfully written to Snowflake.")
        return no_of_records_processed, snowflake_start_time, snowflake_end_time, hana_start_time, hana_end_time

    except Exception as e:
        logging.error("Glue Job Failed", exc_info=True)
        raise Exception(f"hana_to_snowflake_merge failed: {e}")