import sys
import boto3
import json
import time
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark import StorageLevel
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from hdbcli import dbapi
import logging
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import pandas as pd
# importing custom script
import aws_functions_spark_v3_snowflake as aws_functions
import re
import snowflake.connector

# Setting Log Level to INFO
logging.getLogger().setLevel(logging.INFO)
logging.info("Glue Job Start Time: " + str(datetime.now()))
logging.info("Define Clients: " + str(datetime.now()))

sns_client = boto3.client('sns')
glue_client = boto3.client("glue")
client = boto3.client("secretsmanager")
logging.info("Get Job parameters")

args = getResolvedOptions(
    sys.argv,
    [
        'batch_group',
        'partition_size',
        'JOB_NAME',
        'sns_topic_arn',
        'account_env_number',
        'main_job_run_id',
        'secret_name_hana',
        'secret_name_snowflake',
        'secret_name_snowflake_keypair'
    ]
)

sns_topic_arn = args['sns_topic_arn']
account_env_number = args['account_env_number']
main_job_run_id = args['main_job_run_id']
job_name = args['JOB_NAME']
batch_group = args['batch_group']
job_run_id = aws_functions.get_running_job_ids_by_batch_group(job_name, batch_group)
secret_name_hana = args['secret_name_hana']
secret_name_snowflake = args['secret_name_snowflake']
secret_name_snowflake_keypair = args['secret_name_snowflake_keypair']

get_secret_value_response = client.get_secret_value(SecretId=secret_name_hana)
secret = json.loads(get_secret_value_response['SecretString'])
db_username = secret.get('db_username')
db_password = secret.get('db_password')
db_url = secret.get('db_url')
jdbc_driver_name = secret.get('jdbc_driver_name')
error_details = ""

# Getting Snowflake DB credentials from Secrets Manager
get_secret_value_response_snowflake = client.get_secret_value(SecretId=secret_name_snowflake)
secret_snowflake = get_secret_value_response_snowflake['SecretString']
secret_snowflake = json.loads(secret_snowflake)

# Retrieving Snowflake Connection Details
snowflake_db_username = secret_snowflake.get("sfuser")
snowflake_url = secret_snowflake.get('sfURL')
snowflake_private_key_passphrase = secret_snowflake.get('sf_private_key_passphrase')
snowflake_warehouse = secret_snowflake.get('sf_Warehouse')
snowflake_extvol = secret_snowflake.get('sf_EXTERNAL_VOLUME')
snowflake_catalog = secret_snowflake.get('sf_CATALOG')
snowflake_dbname = secret_snowflake.get('sf_DBNAME')
snowflake_schema = secret_snowflake.get('sf_SCHEMA')
snowflake_temp_schema = secret_snowflake.get('sf_Temp_Schema')
snowflake_account = secret_snowflake.get('sfAccount')

# Getting Snowflake certificate from Secrets Manager
get_secret_value_response_snowflake_keypair = client.get_secret_value(SecretId=secret_name_snowflake_keypair)
snowflake_certificate_string = get_secret_value_response_snowflake_keypair['SecretString']

# Generating snowflake private key
snowflake_ppk, snowflake_ppk_non_spark = aws_functions.generate_snowflake_ppk(
    snowflake_certificate_string, snowflake_private_key_passphrase
)

sfOptions = {
    "autopushdown": "on",
    "sfURL": snowflake_url,
    "sfUser": snowflake_db_username,
    "pem_private_key": snowflake_ppk,
    "sfDatabase": snowflake_dbname,
    "sfSchema": snowflake_schema,
    "sf_temp_Schema": snowflake_temp_schema,
    "sWarehouse": snowflake_warehouse,
    "sfAccount": snowflake_account,
}


try:
    # Main script Logic
    batch_group = args['batch_group']
    partition_size = args['partition_size']
    job_name, worker_count = aws_functions.get_glue_job_name_and_worker_count()

    snowflake_query_to_get_tables_for_replication = f"""
    SELECT SOURCE1_SCHEMA_NAME, SOURCE1_OBJECT_NAME,
           TARGET1_SCHEMA_NAME, TARGET1_OBJECT_NAME, LOAD_TYPE,
           INCREMENTAL_FIELD, IFNULL(INCREMENTAL_FIELD_MAX_VALUE, 0) AS INCREMENTAL_FIELD_MAX_VALUE,
           IFNULL(DELETE_INCREMENTAL_FIELD_MAX_VALUE, 0) AS DELETE_INCREMENTAL_FIELD_MAX_VALUE, AGILENT_LEAD, BATCH_GROUP,
           TARGET1_DELETE_TABLE_NAME,
           TARGET1_DELETE_TABLE_SCHEMA_NAME, SOURCE1_DELETE_TABLE_SCHEMA_NAME, SOURCE1_DELETE_TABLE_NAME,
           DELETE_ENABLE_FLAG, RECON_ENABLE_FLAG, LOAD_ENABLE_FLAG,
           DELETE_PROCESS_STATUS, RECON_PROCESS_STATUS, LOAD_STATUS, IFNULL(DATA_RETENTION_DAYS_ARCHIVE, 14) AS DATA_RETENTION_DAYS_ARCHIVE,
           NO_OF_WORKERS as WORKER_COUNT, WAREHOUSE 
    FROM INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_CTRL
    WHERE LOAD_ENABLE_FLAG = TRUE
      AND LOAD_STATUS = 'ready_to_replicate'
      AND BATCH_GROUP = {batch_group}
    ORDER BY SOURCE1_OBJECT_NAME;
    """

    # Function call to get list of tables in scope for replication
    snowflake_load_control_df = aws_functions.run_snowflake_query_to_df_spark(sfOptions, snowflake_query_to_get_tables_for_replication)

    # Check if snowflake_load_control_df is empty and exit the job
    if snowflake_load_control_df is None or snowflake_load_control_df.empty:
        logging.info(f"snowflake_load_control_df is empty. No more tables to replicate at {datetime.now()}")
        try:
            sys.exit(0)
        except SystemExit:
            logging.info("Exiting script after job commit.")

    # Loop through the list of tables in scope for replication
    for index, row in snowflake_load_control_df.iterrows():
        primary_key_str = ""
        try:
            logging.info("Initialize all the status flags")
            row['error_details'] = ''
            partition_count = -1

            if row['LOAD_STATUS'] == 'ready_to_replicate':
                row['LOAD_STATUS'] = 'in_progress'
                load_status = 'in_progress'
            else:
                load_status = row['LOAD_STATUS']

            if row['DELETE_PROCESS_STATUS'] == 'ready_to_replicate' and row['LOAD_TYPE'] in ('RECON-FULL', 'FULL-REFRESH'):
                row['DELETE_PROCESS_STATUS'] = 'not_applicable'
                delete_process_status = 'not_applicable'
            elif row['DELETE_PROCESS_STATUS'] == 'ready_to_replicate' and row['LOAD_TYPE'] not in ('RECON-FULL', 'FULL-REFRESH'):
                row['DELETE_PROCESS_STATUS'] = 'in_progress'
                delete_process_status = 'in_progress'
            else:
                delete_process_status = row['DELETE_PROCESS_STATUS']

            if row['RECON_PROCESS_STATUS'] == 'ready_to_replicate':
                row['RECON_PROCESS_STATUS'] = 'in_progress'
                recon_process_status = 'in_progress'
            else:
                recon_process_status = row['RECON_PROCESS_STATUS']

            load_start_time = '1900-01-01 00:00:00'
            load_end_time = '1900-01-01 00:00:00'
            delete_start_time = '1900-01-01 00:00:00'
            delete_end_time = '1900-01-01 00:00:00'
            recon_start_time = '1900-01-01 00:00:00'
            recon_end_time = '1900-01-01 00:00:00'
            snowflake_start_time = '1900-01-01 00:00:00'
            snowflake_end_time = '1900-01-01 00:00:00'
            hana_start_time = '1900-01-01 00:00:00'
            hana_end_time = '1900-01-01 00:00:00'
            delta_record_count = 0
            delta_record_samples = ''
            no_of_records_processed = 0
            predicates_list = []
            predicates_list_archive = ''
            row['predicates_list_archive'] = ''
            data_retention_days_archive = int(row['DATA_RETENTION_DAYS_ARCHIVE'])
            logging.info(f"flags set are load_enable_flag: {row['LOAD_ENABLE_FLAG']}, delete_enable_flag: {row['DELETE_ENABLE_FLAG']}, recon_enable_flag: {row['RECON_ENABLE_FLAG']}")

            row['snowflake_extvol'] = snowflake_extvol
            row['job_run_id'] = job_run_id
            row['main_job_run_id'] = main_job_run_id
            row['snowflake_ppk'] = snowflake_ppk
            row['snowflake_ppk_non_spark'] = snowflake_ppk_non_spark
            row['RECON_START_TIME'] = recon_start_time
            row['RECON_END_TIME'] = recon_end_time

            sfOptions['sfSchema'] = row['TARGET_SCHEMA_NAME']
            row['DELETE_INCREMENTAL_FIELD_MAX_VALUE'] = 0
            row['TARGET_SCHEMA_NAME'] = row['TARGET_SCHEMA_NAME'].upper() if row['TARGET_SCHEMA_NAME'] else row['TARGET1_SCHEMA_NAME']
            row['TARGET1_DELETE_TABLE_SCHEMA_NAME'] = row['TARGET1_DELETE_TABLE_SCHEMA_NAME'].upper() if row['TARGET1_DELETE_TABLE_SCHEMA_NAME'] else row['TARGET1_DELETE_TABLE_SCHEMA_NAME']
            sfOptions['sfWarehouse'] = row['WAREHOUSE']

            if row['LOAD_TYPE'] in ('FULL', 'INCR', 'FULL-REFRESH'):
                error_details = ""
                if row['INCREMENTAL_FIELD'] == 'row_id':
                    if row['TARGET_OBJECT_NAME'] is None or str(row['TARGET1_OBJECT_NAME']).strip() in ['', 'None']:
                        logging.info("Generate snowflake table names and column names using the logic defined in function generate_snowflake_tbinames_ddls")
                        drop_ddl, create_ddl, iceberg_ddl, snowflake_table_stg, iceberg_table_name, snowflake_column_names_f_pandas = aws_functions.generate_snowflake_tblnames_ddls([], row, sfOptions, "Get Columns")
                        row['TARGET1_OBJECT_NAME'] = iceberg_table_name
                        logging.info(f"generated TARGET1_OBJECT_NAME is {row['TARGET1_OBJECT_NAME']}")
                    else:
                        logging.info("Check if the table exists or not by retrieving snowflake_column_names_f_pandas, target table name will be picked up from control table as is")
                        drop_ddl, create_ddl, iceberg_ddl, snowflake_table_stg, iceberg_table_name, snowflake_column_names_df_pandas = aws_functions.generate_snowflake_tblnames_ddls([], row, sfOptions, "Get Columns")
                        logging.info(f"predefined TARGET_OBJECT_NAME is {row['TARGET1_OBJECT_NAME']}")
                    logging.info("Make an entry into Log table" + str(datetime.now()))
                    row["no_of_records_processed"] = 0
                    aws_functions.update_load_ctrl_log_entry(sfOptions=sfOptions, row_array=row, IN_TBL_NAME="CTRL/LOG", IN_LOAD_STAGE="INITIAL")

                    if row['LOAD_TYPE'] == 'FULL-REFRESH':
                        logging.info("Entered loop for FULL-REFRESH load with new table creation " + str(datetime.now()))
                        load_start_time = datetime.now()
                        hana_query = f'''CALL "APP_EBI"."sp_generate_dynamic_partition_queries_replication"( 'FULL', '{row['SOURCE1_OBJECT_NAME']}', '{row['SOURCE1_SCHEMA_NAME']}', {partition_size}, 200000000, 0)'''
                        logging.info("HANA Procedure call formed" + hana_query)
                        predicates_list = aws_functions.execute_hana_query(hana_query, db_url, db_username, db_password, jdbc_driver_name, row['SOURCE1_OBJECT_NAME'], row['SOURCE1_SCHEMA_NAME'], [], 'FULL')
                        logging.info("HANA Procedure executed and predicates retrieved " + hana_query)

                        primary_key_list, snowflake_columns_list, hana_select_statement, snowflake_recon_query = aws_functions.get_matching_table_columns_hana_snowflake(row, db_url, db_username, db_password, jdbc_driver_name, sfOptions, snowflake_column_names_df_pandas)

                        no_of_records_processed, snowflake_start_time, snowflake_end_time, hana_start_time, hana_end_time = aws_functions.hana_to_snowflake_merge(row, hana_select_statement, snowflake_columns_list, db_url, db_username, db_password, jdbc_driver_name, predicates_list, None, sfOptions)

                        logging.info("set load_status to completed since full refresh completed successfully")
                        load_status = 'completed'
                        load_end_time = datetime.now()
                        predicates_str = ', '.join(predicates_list)
                        primary_key_str = ""

                        if 'ROW_ID >=' in predicates_str:
                            m = re.search(r'\d+$', predicates_str)
                            if m:
                                row['INCREMENTAL_FIELD_MAX_VALUE'] = m.group()
                                logging.info(f"INCREMENTAL_FIELD_MAX_VALUE is : {row['INCREMENTAL_FIELD_MAX_VALUE']}")
                            partition_count = predicates_str.count(', ') + 1
                            logging.info(f"partition_count is : {partition_count}")
                        else:
                            print(f"No 'ROW_ID >=' found in the string, retain old value of INCREMENTAL_FIELD_MAX_VALUE: {row['INCREMENTAL_FIELD_MAX_VALUE']}")
                            partition_count = -1

                        logging.info("set load_status to completed since incremental completed successfully")

                if ((snowflake_column_names_df_pandas.empty or (len(snowflake_column_names_df_pandas) == 0 and snowflake_column_names_df_pandas['COLUMN_NAME'].isnull().all())) and row['LOAD_TYPE'] != 'FULL-REFRESH'):
                    logging.info("Entered loop for FULL load with new table creation" + str(datetime.now()))
                    load_start_time = datetime.now()
                    hana_query = f'''CALL "APP_EBI"."sp_generate_dynamic_partition_queries_replication"('{row['LOAD_TYPE']}', '{row['SOURCE1_OBJECT_NAME']}', '{row['SOURCE1_SCHEMA_NAME']}', {partition_size}, 200000000, 0)'''
                    logging.info("HANA Procedure call formed " + hana_query)
                    predicates_list = aws_functions.execute_hana_query(hana_query, db_url, db_username, db_password, jdbc_driver_name, row['SOURCE1_OBJECT_NAME'], row['SOURCE1_SCHEMA_NAME'], [], row['LOAD_TYPE'])
                    logging.info("HANA Procedure executed and predicates retrieved " + hana_query)

                    primary_key_list, snowflake_columns_list, hana_select_statement, snowflake_recon_query = aws_functions.get_matching_table_columns_hana_snowflake(row, db_url, db_username, db_password, jdbc_driver_name, sfOptions, snowflake_column_names_df_pandas)

                    if primary_key_list is None:
                        logging.info("primary key does not exist at HANA so retrieving from athena table " + row['SOURCE1_OBJECT_NAME'])
                        primary_key_list = aws_functions.fetch_snowflake_ctrl_primary_keys(row['SOURCE1_SCHEMA_NAME'], row['SOURCE1_OBJECT_NAME'], account_env_number, sfOptions)

                    logging.info("primary key retrieval attempted")
                    if primary_key_list is not None:
                        primary_key_str = ','.join(primary_key_list)
                        logging.info("INSERT data into snowflake table" + str(datetime.now()))
                        no_of_records_processed, snowflake_start_time, snowflake_end_time, hana_start_time, hana_end_time, recon_start_time, recon_end_time, delta_record_count, delta_record_samples = aws_functions.hana_to_snowflake_insert(hana_select_statement, db_url, db_username, db_password, jdbc_driver_name, predicates_list, row, sfOptions)
                        logging.info("set load_status to completed since load completed successfully")
                        aws_functions.add_pk_iceberg_table(snowflake_dbname, snowflake_schema, row['TARGET1_OBJECT_NAME'], primary_key_list, row, sfOptions)
                        load_status = 'completed'
                        load_end_time = datetime.now()
                        row['load_status'] = 'completed'
                        row['load_end_time'] = load_end_time
                        row['DELETE_PROCESS_STATUS'] = 'Skipped'
                        delete_process_status = 'Skipped'
                    else:
                        primary_key_str = ""
                        logging.info("primary key does not exist please update primary in athena table for table- " + row['SOURCE1_OBJECT_NAME'])
                        raise Exception("primary key does not exist please update primary keys in athena table for table- " + row['SOURCE1_OBJECT_NAME'])

                    predicates_str = ', '.join(predicates_list)
                    if 'ROW_ID >=' in predicates_str:
                        m = re.search(r'\d+$', predicates_str)
                        if m:
                            row['INCREMENTAL_FIELD_MAX_VALUE'] = m.group()
                            logging.info(f"INCREMENTAL_FIELD_MAX_VALUE is : {row['INCREMENTAL_FIELD_MAX_VALUE']}")
                        partition_count = predicates_str.count(', ') + 1
                        logging.info(f"partition_count is : {partition_count}")
                    else:
                        print(f"No 'ROW_ID >=' found in the string, retain old value of INCREMENTAL_FIELD_MAX_VALUE: {row['INCREMENTAL_FIELD_MAX_VALUE']}")
                        partition_count = -1
                    logging.info("set load_status to completed since incremental completed successfully")

                elif row['LOAD_TYPE'] in ('FULL', 'INCR'):
                    logging.info("Entered loop for load with existing table| load " + str(datetime.now()))
                    load_start_time = datetime.now()
                    primary_key_list, snowflake_columns_list, hana_select_statement, snowflake_recon_query = aws_functions.get_matching_table_columns_hana_snowflake(row, db_url, db_username, db_password, jdbc_driver_name, sfOptions, snowflake_column_names_df_pandas)

                    if primary_key_list is None:
                        logging.info("primary key does not exist at HANA so retrieving from athena table " + row['SOURCE1_OBJECT_NAME'])
                        primary_key_list = aws_functions.fetch_snowflake_ctrl_primary_keys(row['SOURCE1_SCHEMA_NAME'], row['SOURCE1_OBJECT_NAME'], account_env_number, sfOptions)
                        logging.info("primary key retrieval attempted")

                    if primary_key_list is not None:
                        primary_key_str = ','.join(primary_key_list)
                        logging.info("Call HANA procedure to get predicates")
                        hana_query = f'''CALL "APP_EBI"."sp_generate_dynamic_partition_queries_replication"('{row['LOAD_TYPE']}', '{row['SOURCE1_OBJECT_NAME']}', '{row['SOURCE1_SCHEMA_NAME']}', {partition_size}, 208888080, {row['INCREMENTAL_FIELD_MAX_VALUE']})'''
                        logging.info("HANA Procedure call formed " + hana_query)
                        predicates_list = aws_functions.execute_hana_query(hana_query, db_url, db_username, db_password, jdbc_driver_name, row['SOURCE1_OBJECT_NAME'], row['SOURCE1_SCHEMA_NAME'], [], row['LOAD_TYPE'])
                        logging.info("HANA Procedure executed and predicates retrieved" + hana_query + str(predicates_list))
                        logging.info("hana_select_statement " + str(hana_select_statement))

                        no_of_records_processed, snowflake_start_time, snowflake_end_time, hana_start_time, hana_end_time = aws_functions.hana_to_snowflake_merge(row, hana_select_statement, snowflake_columns_list, db_url, db_username, db_password, jdbc_driver_name, predicates_list, primary_key_str, sfOptions)

                        load_status = 'completed'
                        load_end_time = datetime.now()
                        row['load_status'] = 'completed'
                        row['load_end_time'] = load_end_time

                        logging.info("Logic to extract incremental field max value from predicates")
                        predicates_str = ', '.join(predicates_list)
                        if 'ROW_ID >=' in predicates_str:
                            m = re.search(r'\d+$', predicates_str)
                            if m:
                                row['INCREMENTAL_FIELD_MAX_VALUE'] = m.group()
                                logging.info(f"INCREMENTAL_FIELD_MAX_VALUE is : {row['INCREMENTAL_FIELD_MAX_VALUE']}")
                            partition_count = predicates_str.count(', ') + 1
                            logging.info(f"partition_count is : {partition_count}")
                        else:
                            print(f"No 'ROW_ID >=' found in the string, retain old value of INCREMENTAL_FIELD_MAX_VALUE: {row['INCREMENTAL_FIELD_MAX_VALUE']}")
                            partition_count = -1
                        logging.info("set load_status to completed since incremental completed successfully")

                    logging.info(f"delete_enable_flag is : {row['DELETE_ENABLE_FLAG']}")
                    row['DELETE_PROCESS_STATUS'] = 'Skipped'
                    delete_process_status = 'Skipped'
                    if row['DELETE_ENABLE_FLAG'] == True:
                        delete_process_status = 'in_progress'
                        row['DELETE_PROCESS_STATUS'] = delete_process_status
                        logging.info("start processing deletes")
                        delete_start_time = datetime.now()
                        if row['TARGET1_DELETE_TABLE_NAME'] is None or str(row['TARGET1_DELETE_TABLE_NAME']).strip() in ['', 'None']:
                            logging.info("target1_delete_table_name doesnt exist")
                            source_table_name_rep = aws_functions.replace_special_characters_with_underscore(row['SOURCE1_OBJECT_NAME'])
                            hana_archive_table_name = "T_" + row['TARGET1_OBJECT_NAME'].split("_")[0] + "_" + source_table_name_rep + "_ARCHIVE"
                            hana_archive_table_name = hana_archive_table_name.upper()
                            snowflake_table_name_archive = hana_archive_table_name.removeprefix("T_")
                            if snowflake_table_name_archive.endswith("_ARCHIVE"):
                                snowflake_table_name_archive = snowflake_table_name_archive.replace('_ARCHIVE', '_DELETED')
                            snowflake_schema_name_archive = "INTEGRATION"
                            row['SOURCE1_DELETE_TABLE_SCHEMA_NAME'] = "DATA_LOGGING_HANA"
                            row['SOURCE1_DELETE_TABLE_NAME'] = hana_archive_table_name
                            row['TARGET1_DELETE_TABLE_NAME'] = snowflake_table_name_archive
                            row['TARGET1_DELETE_TABLE_SCHEMA_NAME'] = snowflake_schema_name_archive
                            # Print for debugging
                            print("HANA Archive Table Name:", hana_archive_table_name)
                            print("Snowflake Table Name Archive:", snowflake_table_name_archive)
                        else:
                            logging.info("target1_delete_table_name exists")
                            snowflake_table_name_archive = row['TARGET1_DELETE_TABLE_NAME']
                            if row['SOURCE1_DELETE_TABLE_NAME'] is None or str(row['SOURCE1_DELETE_TABLE_NAME']).strip() in ['', 'None']:
                                logging.info("SOURCE1_DELETE_TABLE_NAME does not exist. regenerate it")
                                source_table_name_rep = aws_functions.replace_special_characters_with_underscore(row['SOURCE1_OBJECT_NAME'])
                                hana_archive_table_name = "T_" + row['TARGET1_OBJECT_NAME'].split("_")[0] + "_" + source_table_name_rep + "_ARCHIVE"
                                hana_archive_table_name = hana_archive_table_name.upper()
                                row['SOURCE1_DELETE_TABLE_SCHEMA_NAME'] = "DATA_LOGGING_HANA"
                                row['SOURCE1_DELETE_TABLE_NAME'] = hana_archive_table_name
                                logging.info(f"generated SOURCE1_DELETE_TABLE_NAME is {row['SOURCE1_DELETE_TABLE_NAME']}")
                            snowflake_schema_name_archive = row['TARGET1_DELETE_TABLE_SCHEMA_NAME']
                        logging.info(f"target1_delete_table_name: {row['TARGET1_DELETE_TABLE_NAME']}")
                        logging.info(f"target1_delete_table_schema_name: {row['TARGET1_DELETE_TABLE_SCHEMA_NAME']}")
                        logging.info(f"source1_delete_table_schema_name: {row['SOURCE1_DELETE_TABLE_SCHEMA_NAME']}")
                        logging.info(f"source1_delete_table_name: {row['SOURCE1_DELETE_TABLE_NAME']}")
                        # Initialization and HANA delete call
                        hana_archive_delet_query = f'''CALL "APP_EBI"."sp_dynamic_archive_table_delete"(
                            '{row['SOURCE1_SCHEMA_NAME']}',
                            '{row['SOURCE1_OBJECT_NAME']}',
                            '{row['SOURCE1_DELETE_TABLE_SCHEMA_NAME']}',
                            '{row['SOURCE1_DELETE_TABLE_NAME']}',
                            {data_retention_days_archive}
                        )'''
                        logging.info("Flush out records that are recreated at source " + str(hana_archive_delet_query))
                        sql_exec_status = aws_functions.execute_archive_table_delete_hana_query(hana_archive_delet_query, db_url, db_username, db_password, jdbc_driver_name)
                        hana_query_archive = f'''CALL "APP_EBI"."sp_generate_dynamic_partition_queries_replication"(
                            '{row['LOAD_TYPE']}',
                            '{row['SOURCE1_DELETE_TABLE_NAME']}',
                            '{row['SOURCE1_DELETE_TABLE_SCHEMA_NAME']}',
                            {partition_size},
                            200000000,
                            {row['DELETE_INCREMENTAL_FIELD_MAX_VALUE']}
                        )'''
                        logging.info("HANA Procedure call formed-hana_query_archive " + hana_query_archive)
                        predicates_list_archive = aws_functions.execute_hana_query(hana_query_archive, db_url, db_username, db_password, jdbc_driver_name, row['SOURCE1_DELETE_TABLE_NAME'], row['SOURCE1_DELETE_TABLE_SCHEMA_NAME'], [], row['LOAD_TYPE'])
                        logging.info("Assign sfSchema with target1_delete_table_schema_name")
                        sfOptions['sfSchema'] = row['TARGET1_DELETE_TABLE_SCHEMA_NAME']
                        drop_ddl, create_ddl, iceberg_ddl, snowflake_table_stg, iceberg_table_name, snowflake_column_names_f_pandas = aws_functions.generate_snowflake_delete_tblnames_ddls([], row, sfOptions, account_env_number, "Get Columns")
                        primary_key_list_archive, snowflake_columns_list, hana_select_statement_archive, snowflake_recon_query = aws_functions.get_matching_delete_table_columns_hana_snowflake(row, db_url, db_username, db_password, jdbc_driver_name, sfOptions, snowflake_column_names_df_pandas)
                        if primary_key_list_archive is None:
                            logging.info("primary key does not exist at HANA so retrieving from athena table " + row['SOURCE1_OBJECT_NAME'])
                            primary_key_list_archive = aws_functions.fetch_athena_ctrl_primary_keys(row['SOURCE1_SCHEMA_NAME'], row['SOURCE1_OBJECT_NAME'], account_env_number)
                            logging.info("primary key retrieval attempted")
                        no_of_records_processed_archive = aws_functions.hana_to_snowflake_hard_delete_delete_table(hana_select_statement_archive, snowflake_columns_list, db_url, db_username, db_password, jdbc_driver_name, predicates_list_archive, row, sfOptions, primary_key_str, account_env_number)
                        predicates_str_archive = ', '.join(predicates_list_archive) if predicates_list_archive else ''
                        if 'ROW_ID >=' in predicates_str_archive:
                            m = re.search(r'\d+$', predicates_str_archive)
                            row['DELETE_INCREMENTAL_FIELD_MAX_VALUE'] = m.group()
                            logging.info(f"DELETE_INCREMENTAL_FIELD_MAX_VALUE is : {row['DELETE_INCREMENTAL_FIELD_MAX_VALUE']}")
                        else:
                            print(f"No 'ROW_ID >=' found in the string, retain old value of DELETE_INCREMENTAL_FIELD_MAX_VALUE: {row['DELETE_INCREMENTAL_FIELD_MAX_VALUE']}")
                        delete_process_status = 'completed'
                        delete_end_time = datetime.now()
            elif row['LOAD_TYPE'] == 'RECON-FULL':
                logging.info("Entered loop for FULL RECON" + str(datetime.now()))
                row['target1_recon_table_schema_name'] = row['TARGET1_DELETE_TABLE_SCHEMA_NAME']
                logging.info("Make an entry into Log table" + str(datetime.now()))
                aws_functions.update_load_ctrl_log_entry(sfOptions=sfOptions, row_array=row, IN_TBL_NAME="CTRL/LOG", IN_LOAD_STAGE="INITIAL")
                primary_key_str = ""
                primary_key_list = aws_functions.fetch_snowflake_ctr1_primary_keys(row['SOURCE1_SCHEMA_NAME'], row['SOURCE1_OBJECT_NAME'], account_env_number, sfOptions)
                if primary_key_list is not None:
                    primary_key_str = ','.join(primary_key_list)
                if row['TARGET1_OBJECT_NAME'] is None or row['TARGET1_OBJECT_NAME'].strip() in ['', 'None']:
                    logging.info("Generate snowflake table names and column names using the logic defined in function generate_snowflake_tblnames_ddls")
                    drop_ddl, create_ddl, iceberg_ddl, snowflake_table_stg, iceberg_table_name, snowflake_column_names_df_pandas = aws_functions.generate_snowflake_tblnames_ddls([], row, sfOptions, "Get Columns")
                    row['TARGET1_OBJECT_NAME'] = iceberg_table_name
                else:
                    logging.info("Take the table name from athena itself since its not blank")
                    drop_ddl, create_ddl, iceberg_ddl, snowflake_table_stg, iceberg_table_name, snowflake_column_names_df_pandas = aws_functions.generate_snowflake_tblnames_ddls([], row, sfOptions, "Get Columns")
                hana_query = f'''CALL "APP_EBI"."SP_generate_dynamic_partition_queries_replication"('{row['LOAD_TYPE']}', '{row['SOURCE1_OBJECT_NAME']}', '{row['SOURCE1_SCHEMA_NAME']}', {partition_size}, 200000000, {row['INCREMENTAL_FIELD_MAX_VALUE']})'''
                logging.info("HANA Procedure call formed " + hana_query)
                predicates_list = aws_functions.execute_hana_query(hana_query, db_url, db_username, db_password, jdbc_driver_name, row['SOURCE1_OBJECT_NAME'], row['SOURCE1_SCHEMA_NAME'], [], row['LOAD_TYPE'])
                logging.info("HANA Procedure executed and predicates retrieved " + hana_query)
                
                hana_select_statement_recon = f'(select "$rowid$" as row_id from "{row["SOURCE1_SCHEMA_NAME"]}"."{row["SOURCE1_OBJECT_NAME"]}") subquery'
                no_of_records_processed, snowflake_start_time, snowflake_end_time, hana_start_time, hana_end_time, recon_start_time, recon_end_time, delta_record_count, delta_record_samples = aws_functions.hana_to_snowflake_insert(hana_select_statement_recon, db_url, db_username, db_password, jdbc_driver_name, predicates_list, row, sfOptions)
                load_status = 'completed'
                load_end_time = datetime.now()
            row['no_of_records_processed'] = no_of_records_processed
            row['partition_count'] = partition_count
            row['predicates'] = predicates_list if predicates_list else None
            print("primary_key_str ", primary_key_str)
            row['LOAD_END_TIME'] = load_end_time
            row['SOURCE1_PRIMARY_KEY'] = primary_key_str
            row['DELETE_START_TIME'] = delete_start_time
            row['DELETE_END_TIME'] = delete_end_time
            row['RECON_START_TIME'] = recon_start_time
            row['RECON_END_TIME'] = recon_end_time
            row['SNOWFLAKE_START_TIME'] = snowflake_start_time
            row['SNOWFLAKE_END_TIME'] = snowflake_end_time
            row['HANA_START_TIME'] = hana_start_time
            row['HANA_END_TIME'] = hana_end_time
            row['LOAD_STATUS'] = load_status
            row['DELETE_PROCESS_STATUS'] = delete_process_status
            row['RECON_PROCESS_STATUS'] = recon_process_status
            row['DELTA_RECORD_COUNT'] = delta_record_count
            row['DELTA_RECORD_SAMPLES'] = delta_record_samples
            row["predicates_list_archive"] = predicates_list_archive if not predicates_list_archive else predicates_list_archive[0]
            aws_functions.update_load_ctrl_log_entry(sfOptions=sfOptions, row_array=row, IN_TBL_NAME="CTRL/LOG", IN_LOAD_STAGE="FINAL")

        except Exception as error_message:
            logging.info(f"An error occurred: {error_message}")
            if error_message is not None:
                logging.info(f"starting replace function")
                error_details = str(error_message)[0:500]
                error_details = error_details.replace("'", "''")
                logging.info(f"completed replace function")
            else:
                error_details = "No error occurred"
            error_details = error_details.replace("\n", " ").replace("'", "''")
            print(f"error_details: {error_details}")
            row['error_details'] = error_details
            no_of_records_processed = 0
            worker_count=0
            row[partition_count]=partition_count
            row["predicates"]=""
            row['LOAD_STATUS'] = load_status
            row['DELETE_PROCESS_STATUS'] = delete_process_status
            row['RECON_PROCESS_STATUS'] = recon_process_status
            row["DELTA_RECORD_COUNT"] = delta_record_count
            row["DELTA_RECORD_SAMPLES"] = delta_record_samples
            row["SOURCE1_PRIMARY_KEY"] = primary_key_str
            if row["LOAD_STATUS"] == 'in_progress':
                row['LOAD_STATUS'] = 'failed'
                predicates_list=''
            else:
                predicates_list=predicates_list
            if row["DELETE_PROCESS_STATUS"] == 'in_progress':
                row['DELETE_PROCESS_STATUS'] = 'failed'
                row["predicates_list_archive"] = ''
            elif row["DELETE_PROCESS_STATUS"] == 'completed':
                row["predicates_list_archive"] = predicates_list_archive
            if row["RECON_PROCESS_STATUS"] == 'in_progress':
                row['RECON_PROCESS_STATUS'] = 'failed'
            row["LOAD_END_TIME"] = load_end_time
            row["DELETE_START_TIME"] = delete_start_time
            row["DELETE_END_TIME"] = delete_end_time
            row["RECON_START_TIME"] = recon_start_time
            row["RECON_END_TIME"] = recon_end_time
            row["SNOWFLAKE_START_TIME"] = snowflake_start_time
            row["SNOWFLAKE_END_TIME"] = snowflake_end_time
            row["HANA_START_TIME"] = hana_start_time
            row["HANA_END_TIME"] = hana_end_time
            aws_functions.update_load_ctrl_log_entry(sfOptions=sfOptions, row_array=row, IN_TBL_NAME="CTRL/LOG", IN_LOAD_STAGE="FINAL")
            email_subject = f"{account_env_number} - Failed - Glue: {args["JOB_NAME"]} - {row['SOURCE1_SCHEMA_NAME']}.{row['SOURCE1_OBJECT_NAME']}"
            email_body = f"Glue Job Status: Failed\nGlue Job Name: {args['JOB_NAME']}\nError Details: {error_details}\nJob Run ID: {job_run_id}\nMain Job Run ID: {main_job_run_id}\nPlease check the logs for more details."

except Exception as e:
    logging.exception("Fatal error in main script: %s", str(e))
    error_details = str(e)
    no_of_records_processed = 0
    worker_count=0
    email_subject = f"{account_env_number} - Fatal Error - Glue: {args['JOB_NAME']}"
    email_body = f"Glue Job Status: Failed with Fatal Error\nGlue Job Name: {args['JOB_NAME']}\nError Details: {error_details}\nJob Run ID: {job_run_id}\nMain Job Run ID: {main_job_run_id}\nPlease check the logs for more details."
    aws_functions.send_email(sns_topic_arn, email_subject, email_body)
