import sys
import re
import json
import boto3
import time
import logging
from datetime import datetime
from awsglue.utils import getResolvedOptions
from hdbcli import dbapi
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import aws_functions_python_v3_snowflake as aws_functions

# --- Python logging configuration ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter('%(levelname)s:%(message)s\n', datefmt="%Y-%m-%dT%I:%M:%S"))
if not logger.handlers:
    logger.addHandler(handler)
else:
    # Avoid duplicate handlers on reruns
    logger.handlers = [handler]

try:
    logger.info("Glue Job Start Time: " + str(datetime.now()))
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'sns_topic_arn', 'account_env_number'])
    sns_topic_arn = args['sns_topic_arn']
    account_env_number = args['account_env_number']
    job_name = args['JOB_NAME']

    glue_client = boto3.client('glue')
    job_run_id = glue_client.get_job_runs(JobName=job_name)['JobRuns'][0]['Id']
    client = boto3.client('secretsmanager')

    # ==== Secret names (edit if needed) ====
    secret_name_hana = "eas-it-key-sap-hana-credential-hd3"
    # secret_name_hana = "eas-it-key-sap-hana-credential-ht1"
    logger.info(f"HANA Secret name is {secret_name_hana}")

    # secret_name_snowflake = "eas-it-snowflake-npr00001-testing"
    secret_name_snowflake = "eas-it-snowflake-prd00001"
    # secret_name_snowflake_keypair = "eas-it-snowflake-keypair-prd00000"
    secret_name_snowflake_keypair = "eas-it-snowflake-keypair"
    logger.info(f"Snowflake Secret name is {secret_name_snowflake} & {secret_name_snowflake_keypair}")

    # === Get Secrets ===
    logger.info("Start retrieving all secret values")
    get_secret_value_response_hana = client.get_secret_value(SecretId=secret_name_hana)
    secret_hana = json.loads(get_secret_value_response_hana['SecretString'])
    db_username = secret_hana.get('db_username')
    db_password = secret_hana.get('db_password')
    db_url = secret_hana.get('db_url')
    jdbc_driver_name = secret_hana.get('jdbc_driver_name')

    get_secret_value_response_snowflake = client.get_secret_value(SecretId=secret_name_snowflake)
    secret_snowflake = json.loads(get_secret_value_response_snowflake['SecretString'])
    snowflake_db_username = secret_snowflake.get('sfUser')
    snowflake_url = secret_snowflake.get('sfURL')
    snowflake_private_key_passphrase = secret_snowflake.get('sf_private_key_passphrase')
    snowflake_warehouse = secret_snowflake.get('sfWarehouse')
    snowflake_extvol = secret_snowflake.get('sf_EXTERNAL_VOLUME')
    snowflake_catalog = secret_snowflake.get('sf_CATALOG')
    snowflake_dbname = secret_snowflake.get('sf_DBNAME')
    snowflake_schema = secret_snowflake.get('sf_SCHEMA')
    snowflake_account = secret_snowflake.get('sfAccount')
    snowflake_temp_schema = secret_snowflake.get('sfTemp_Schema')
    logger.info("Completed retrieving all secret values")

    logger.info("Getting Snowflake certificate from Secrets Manager")
    get_secret_value_response_snowflake_key = client.get_secret_value(SecretId=secret_name_snowflake_keypair)
    snowflake_certificate_string = get_secret_value_response_snowflake_key['SecretString']
    snowflake_ppk = aws_functions.generate_snowflake_ppk(
        snowflake_certificate_string, snowflake_private_key_passphrase
    )

    sfOptions = {
        "autopushdown": "on",
        "sfURL": snowflake_url,
        "sfUser": snowflake_db_username,
        "pem_private_key": snowflake_ppk,
        "sfDatabase": snowflake_dbname,
        "sfSchema": snowflake_schema,
        "sfAccount": snowflake_account,
        "sfTemp_Schema": snowflake_temp_schema,
        "sfWarehouse": snowflake_warehouse
    }
    # === Update control for schedule and run ===
    update_schedule_query = f"CALL INTEGRATION.SP_UPDATE_READY_TO_RUN()"
    logging.info("Executing Snowflake procedure SP_UPDATE_READY_TO_RUN")
    aws_functions.run_snowflake_query_to_df(sfOptions, update_schedule_query)

    # === Config from Snowflake ===
    df_config = aws_functions.fetch_snowflake_config(sfOptions)
    recon_full_day_of_week = df_config.iloc[0]['RECON_FULL_DAY_OF_WEEK']  # Expecting 1..7 as int or str
    try:
        recon_full_day_of_week = int(recon_full_day_of_week)
    except Exception:
        raise ValueError(f"RECON_FULL_DAY_OF_WEEK must be integer 1..7, got: {recon_full_day_of_week}")
    logger.info(f"recon_full_day_of_week = {recon_full_day_of_week}")

    # === Determine current ISO day-of-week in business zone (America/Los Angeles) ===
    snowflake_query_to_get_day_of_week = """
        SELECT DAYOFWEEKISO(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) AS DAY_OF_WEEK;
    """
    df_day_of_week = aws_functions.run_snowflake_query_to_df(sfOptions, snowflake_query_to_get_day_of_week)
    day_of_week = int(df_day_of_week.iloc[0]['DAY_OF_WEEK'])
    logger.info(f"day_of_week (ISO Mon=1..Sun=7): {day_of_week}, recon_full_day_of_week: {recon_full_day_of_week}")

    # === Has RECON-FULL already run today (business timezone)? ===
    already_ran_today_sql = """
        SELECT IFF(COUNT(*) > 0, TRUE, FALSE) AS ALREADY_RAN
        FROM INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_LOG
        WHERE LOAD_TYPE = 'RECON-FULL'
        AND CAST(CONVERT_TIMEZONE('UTC','America/Los_Angeles', LOAD_START_TIME) AS DATE)
             = CAST(CONVERT_TIMEZONE('UTC','America/Los_Angeles', CURRENT_TIMESTAMP()) AS DATE)
    """
    df_already = aws_functions.run_snowflake_query_to_df(sfOptions, already_ran_today_sql)
    #snowflake booleans often come back as python bools; still coerce defensively
    already_ran_today = bool(df_already.iloc[0]['ALREADY_RAN'])
    is_recon_day = (day_of_week == recon_full_day_of_week)

    # === One RECON-FULL-per-day gating ===
    if is_recon_day and not already_ran_today:
        execute_recon_full = "Y"
        enable_recon_full_load_sql = (
            "UPDATE INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_CTRL "
            "SET LOAD_TYPE = 'RECON-FULL', READY_TO_RUN = 'TRUE' "
            "WHERE LOAD_TYPE = 'INCR' AND LOAD_ENABLE_FLAG = TRUE AND SOURCE1_OBJECT_NAME != 'BSEG' "
            "AND NOT EXISTS ("
                " SELECT 1 FROM INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_LOG B "
                " WHERE B.LOAD_TYPE = 'RECON-FULL' "
                "  AND CAST(CONVERT_TIMEZONE('UTC','America/Los_Angeles', B.LOAD_START_TIME) AS DATE) = "
                "    CAST(CONVERT_TIMEZONE('UTC','America/Los_Angeles', CURRENT_TIMESTAMP()) AS DATE)"
            ");"
        )
        aws_functions.run_snowflake_query_to_df(sfOptions, enable_recon_full_load_sql)
        logger.info("First run today on recon day — enabling RECON-FULL.")
    else:
        execute_recon_full = "N"
        if not is_recon_day:
            # Non-recon days: ensure lingering RECON-FULL rows are reset back to INCR
            logger.info("Non-recon day — ensured LOAD_TYPE reset back to INCR.")
        else:
            # Recon day but already ran today — incremental this time
            logger.info("Recon day but RECON-FULL already executed today — running incremental this time.")
        disable_recon_full_load_sql = (
            "UPDATE INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_CTRL "
            "SET LOAD_TYPE = 'INCR' "
            "WHERE LOAD_TYPE = 'RECON-FULL' AND LOAD_ENABLE_FLAG = TRUE AND READY_TO_RUN = 'TRUE';"
        )
        aws_functions.run_snowflake_query_to_df(sfOptions, disable_recon_full_load_sql)
    # === Build input for APP_EBI_ETL.get_table_partitioning_details ===
    ctrl_query_to_get_table_list = f"""
    SELECT LISTAGG(SOURCE1_SCHEMA_NAME || '.' || SOURCE1_OBJECT_NAME || '.' || LOAD_TYPE, ',')
            WITHIN GROUP (ORDER BY SOURCE1_SCHEMA_NAME) AS V_INPUT
    FROM INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_CTRL
    WHERE LOAD_ENABLE_FLAG = TRUE AND READY_TO_RUN = 'TRUE' AND LOWER(COALESCE(DELETE_PROCESS_STATUS, 'na')) <> 'in_progress'
    AND LOWER(COALESCE(LOAD_STATUS, 'na')) <> 'in_progress'
          AND (
              (LOAD_TYPE IN ('FULL','INCR','FULL-REFRESH') AND '{execute_recon_full}' = 'N')
              OR (LOAD_TYPE IN ('RECON-FULL') AND '{execute_recon_full}' = 'Y')
          );
    """
    df = aws_functions.run_snowflake_query_to_df(sfOptions, ctrl_query_to_get_table_list)
    v_input = df.iloc[0]['V_INPUT'] if not df.empty else None
    if not v_input:
        logger.info("No tables in scope based on current LOAD_TYPE and flags. Exiting gracefully.")
        sys.exit(0)

    logger.info(f"ctrl table result is {v_input}")

    # === Call HANA TVF for partitioning/batch planning ===
    hana_query = f"""
        SELECT LOAD_TYPE, SCHEMA_NAME, TABLE_NAME, SIZE_GB, NO_OF_WORKERS, BATCH_GROUP
        FROM "APP_EBI"."tf_get_table_partitioning_details"(
            V_INPUT => '{v_input}',
            partition_size => {df_config.loc[0,'PARTITION_SIZE']},
            max_tables_per_batch => {df_config.loc[0,'MAX_TABLES_PER_BATCH']},
            max_workers => {df_config.loc[0,'MAX_WORKERS']},
            min_workers => {df_config.loc[0,'MIN_WORKERS']},
            cores_per_worker => {df_config.loc[0,'CORES_PER_WORKER']},
            worker_ranges => {df_config.loc[0,'WORKER_RANGES']}
        );
    """
    hana_df = aws_functions.execute_hana_query(hana_query, db_url, db_username, db_password, jdbc_driver_name)
    print(" execute_hana_query_python ")
    print(hana_df.head())
    hana_df.columns = ['LOAD_TYPE', 'SCHEMA_NAME', 'TABLE_NAME', 'SIZE_GB', 'NO_OF_WORKERS', 'BATCH_GROUP']

    # === Load plan rows into Snowflake EXT table ===
    logger.info("Starting load hana_df into Snowflake table AWS_ETL_FRAMEWORK_LOAD_CTRL_EXT.")
    success, nrows = aws_functions.load_hana_config_to_snowflake(hana_df, sfOptions)
    logger.info(f"Loading hana_df into snowflake table AWS_ETL_FRAMEWORK_LOAD_CTRL_EXT completed. No of rows: {nrows}")

    # === Merge Plan Back to CTRL (status + batch group/no_of_workers) ===
    update_query = """
        MERGE INTO INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_CTRL AS ctrl
        USING INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_CTRL_EXT AS hana_results
        ON ctrl.source1_object_name = hana_results.TABLE_NAME
           AND ctrl.source1_schema_name = hana_results.SCHEMA_NAME
           AND ctrl.load_type = hana_results.LOAD_TYPE
           AND ctrl.load_enable_flag = TRUE
           AND ctrl.READY_TO_RUN = 'TRUE'
        WHEN MATCHED THEN
            UPDATE SET
                batch_group = CAST(hana_results.BATCH_GROUP AS INT),
                no_of_workers = CAST(hana_results.NO_OF_WORKERS AS INT),
                load_status = 'ready_to_replicate',
                delete_process_status = CASE
                    WHEN delete_enable_flag = TRUE AND ctrl.load_type NOT IN ('RECON-FULL','FULL','FULL-REFRESH')
                    THEN 'ready_to_replicate' ELSE 'skipped' END,
                recon_process_status = CASE
                    WHEN (recon_enable_flag = TRUE AND ctrl.load_type <> 'RECON-FULL')
                    THEN 'ready_to_replicate' ELSE 'skipped' END,
                comments = CONCAT('Status updated to ready_to_replicate on ',
                    CAST(EXTRACT(YEAR FROM current_timestamp) AS VARCHAR),'-',
                    LPAD(CAST(EXTRACT(MONTH FROM current_timestamp) AS VARCHAR),2,'0'),'-',
                    LPAD(CAST(EXTRACT(DAY FROM current_timestamp) AS VARCHAR),2,'0'),' ',
                    LPAD(CAST(EXTRACT(HOUR FROM current_timestamp) AS VARCHAR),2,'0'),':',
                    LPAD(CAST(EXTRACT(MINUTE FROM current_timestamp) AS VARCHAR),2,'0'),':',
                    LPAD(CAST(EXTRACT(SECOND FROM current_timestamp) AS VARCHAR),2,'0')
                );
    """
    affected_rows = aws_functions.run_snowflake_query_to_df(sfOptions, update_query)
    logger.info(f"Snowflake MERGE completed. Rows affected: {affected_rows}")
    # == Get distinct batch groups and worker needs for scope ==
    snowflake_query_to_get_batch_details = f"""
    SELECT LISTAGG(CAST(BATCH_GROUP AS VARCHAR) || '~' || CAST(NO_OF_WORKERS AS VARCHAR), ',')
           WITHIN GROUP (ORDER BY BATCH_GROUP) AS BATCH_DETAILS
    FROM (
        SELECT DISTINCT BATCH_GROUP, NO_OF_WORKERS
        FROM INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_CTRL
        WHERE LOAD_ENABLE_FLAG = TRUE AND READY_TO_RUN ='TRUE'
          AND (
               (LOAD_TYPE IN ('FULL', 'INCR', 'FULL-REFRESH') AND '{execute_recon_full}' = 'N')
               OR (LOAD_TYPE IN ('RECON-FULL') AND '{execute_recon_full}' = 'Y')
          )
    );
    """
    batch_details_df = aws_functions.run_snowflake_query_to_df(sfOptions, snowflake_query_to_get_batch_details)
    batch_details = batch_details_df.iloc[0]['BATCH_DETAILS']
    logger.info(f"snowflake result is {batch_details}")

    # == Initialize lists for batch group and number of workers ==
    if execute_recon_full == True:
        max_concurrent_jobs = int(df_config.loc[0, 'MAX_CONCURRENT_JOBS_FULL'])
    else:
        max_concurrent_jobs = int(df_config.loc[0, 'MAX_CONCURRENT_JOBS'])
    partition_size = str(df_config.loc[0, 'PARTITION_SIZE'])
    worker_type = str(df_config.loc[0, 'WORKER_TYPE'])

    # == Get batch group and number of workers ==
    batch_groups, no_of_workers = aws_functions.extract_batch_details(batch_details)
    print(f"Batch Groups: {batch_groups}")
    print(f"Number of workers: {no_of_workers}")
    print(f"Max concurrent jobs: {max_concurrent_jobs}")

    job_name = "glue-eas-etl-glue-read-from-hana-replicate-to-snowflake"
    print("max_batch_groups before:", max(batch_groups))
    max_batch_groups = max(batch_groups)
    if max_batch_groups > 50:
        last_5_max_groups = sorted(batch_groups, reverse=True)[:5]
        select_query_max_group = f"""
            SELECT TARGET1_OBJECT_NAME, BATCH_GROUP
            FROM INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_CTRL
            WHERE LOAD_TYPE NOT IN ('FULL','FULL-REFRESH')
              AND LOAD_ENABLE_FLAG = TRUE AND READY_TO_RUN = 'TRUE'
              AND BATCH_GROUP IN ({','.join(map(str, last_5_max_groups))})
            ORDER BY BATCH_GROUP
        """
        batch_details_df = aws_functions.run_snowflake_query_to_df(sfOptions, select_query_max_group)
        print(batch_details_df)
        print(batch_details_df.head(10))
        target_objects = batch_details_df['TARGET1_OBJECT_NAME']
        print("target_objects:", target_objects)
        start_batch_group = 1
        updated_entries = []
        # == Loop to generate and execute update queries ==
        for index, target_name in enumerate(target_objects, start=start_batch_group):
            update_query = f"""
                UPDATE INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_CTRL
                SET BATCH_GROUP = {index}
                WHERE TARGET1_OBJECT_NAME = '{target_name}'
            """
            updated_entries.append((target_name, index))
            aws_functions.run_snowflake_query_to_df(sfOptions, update_query)
        print(f"Updated max_batch_groups (after removing max): ", max(batch_groups))
        print("BATCH_GROUP assignment completed. Below are the updated TARGET1_OBJECT_NAMEs and their assigned batch groups:")
        print(updated_entries)

    # == Run AWS Glue jobs ==
    completed_jobs = aws_functions.run_aws_glue_jobs_parallel_controlled_no_of_execution(
        job_name, batch_groups, max_concurrent_jobs, worker_type, batch_details,
        partition_size, account_env_number, job_run_id, sns_topic_arn, execute_recon_full,
        secret_name_hana, secret_name_snowflake, secret_name_snowflake_keypair, sfOptions
    )
    logger.info(f"Glue Job Execution Summary: {completed_jobs}")

    # == Cleanup old logs ==
    delete_query_log = f"DELETE FROM INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_LOG WHERE load_start_time < DATEADD(DAY, -30, CURRENT_TIMESTAMP());"
    result_df = aws_functions.run_snowflake_query_to_df(sfOptions, delete_query_log)

# == Exception handling ==
except Exception as e:
    logger.error(f"An error occurred: {str(e)}")
    error_details = str(e).replace("\n", " ").replace("'", "")
    email_subject = f"{account_env_number} - Failed - Glue: {args['JOB_NAME']}"
    email_body = (
        f"Glue Job Status: Failed\n"
        f"Glue Job Name: {args['JOB_NAME']}\n"
        f"Glue Job Run Id: {job_run_id}\n"
        f"Glue Job Unknown Exception: {error_details}"
    )
    aws_functions.send_email(sns_topic_arn, email_subject, email_body)
    sys.exit(1)  # Exit with non-zero status to indicate failure