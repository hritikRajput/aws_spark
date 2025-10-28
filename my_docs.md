1. ENTRYPOINT — Controller Glue job (the script you showed earlier — “Job 2”)

This is the script that starts when you run the controller Glue job. I’ll follow it in execution order and explain every executed line/statement.

Note: I’ll denote short code fragments in monospace and call out the helper functions (which you pasted) when the controller calls them.

A — Initialization & Logging

logger = logging.getLogger()

Create (or get) the root logger for the script.

logger.setLevel(logging.INFO)

Set logging verbosity to INFO. All subsequent logger.info() calls will emit.

Handler setup (StreamHandler, Formatter)

Ensures logs go to STDOUT in a consistent format. Prevents duplicate handlers on reruns.

logger.info("Glue Job Start Time: " + str(datetime.now()))

Log job start timestamp. Useful for diagnostics and control table entries.

B — Resolve input arguments

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'sns_topic_arn', 'account_env_number'])

This reads Glue runtime arguments passed when the Glue job was launched. At runtime these are set by AWS Glue or by the calling process (like Job 2 launching Job 1). Key values that appear later:

JOB_NAME — the Glue job name.

sns_topic_arn — SNS topic for notifications.

account_env_number — environment identifier used in notification subjects/body.

Assign sns_topic_arn, account_env_number, job_name from args.

These are used repeatedly: SNS topic for alerts, account env number for email subject, and job_name for Glue API calls.

glue_client = boto3.client('glue')

Create a Boto3 Glue client used to get job run metadata (and in other code to start jobs). This runs in the controller; it will also be used later by helper functions to start/monitor Glue jobs.

job_run_id = glue_client.get_job_runs(JobName=job_name)['JobRuns'][0]['Id']

Important: The controller immediately retrieves the latest job run id for itself (the controller job run id). This job_run_id is later passed to downstream jobs as --main_job_run_id so worker jobs can correlate to the controller run. This is how you tie multiple runs together for logging.

client = boto3.client('secretsmanager')

Create Secrets Manager client to fetch credentials for HANA and Snowflake.

C — Define secret names and fetch secrets

secret_name_hana = "eas-it-key-sap-hana-credential-hd3" (and similar assignments)

The script defines which Secrets Manager entries to use for HANA and Snowflake credentials — these names are environment-specific (dev / prod variants commented out).

get_secret_value_response_hana = client.get_secret_value(SecretId=secret_name_hana)

Retrieve HANA credentials JSON string from Secrets Manager.

secret_hana = json.loads(get_secret_value_response_hana['SecretString'])

Parse the secret JSON into a Python dict.

Extract db_username, db_password, db_url, jdbc_driver_name from secret_hana.

These are HANA connection values used later when the script calls HANA.

Repeat for Snowflake: fetch secret_name_snowflake and secret_name_snowflake_keypair (private key/certificate) from Secrets Manager, parse them into secret_snowflake and snowflake_certificate_string.

Extract Snowflake connection details from secret_snowflake:

snowflake_db_username, snowflake_url, snowflake_private_key_passphrase, snowflake_warehouse, snowflake_extvol, snowflake_catalog, snowflake_dbname, snowflake_schema, snowflake_temp_schema, snowflake_account

Those are used to build sfOptions (Snowflake connection options) and to generate the private key.

D — Generate Snowflake private key object (call to helper)

snowflake_ppk = aws_functions.generate_snowflake_ppk(snowflake_certificate_string, snowflake_private_key_passphrase)

What runs: generate_snowflake_ppk in aws_functions_python_v3_snowflake.py.

Line-by-line inside generate_snowflake_ppk:

pkb1 = bytes(certificate_string, 'utf-8') — convert secret to bytes.

p_key = serialization.load_pem_private_key(pkb1, password=bytes(private_key_passphrase, 'utf-8'), backend=default_backend()) — decrypt and load RSA private key.

If for_snowflake true (default), the function returns the p_key object (an RSA private key object) — this is what Snowflake Python connector expects for key-pair auth.

Result: snowflake_ppk holds the private key object used for the Snowflake connector.

E — Build sfOptions (Snowflake connection options)

sfOptions = {...} dictionary population:

Sets "autopushdown": "on", "sfURL": snowflake_url, "sfUser": snowflake_db_username, "pem_private_key": snowflake_ppk, "sfDatabase": snowflake_dbname, "sfSchema": snowflake_schema, "sfTemp_Schema": snowflake_temp_schema, "sfWarehouse": snowflake_warehouse, "sfAccount": snowflake_account

Why: this sfOptions object is the standard config passed to many helper functions, enabling them to authenticate & run SQL on Snowflake using key-pair auth.

F — Run initial Snowflake stored proc to update schedule

update_schedule_query = f"CALL INTEGRATION.SP_UPDATE_READY_TO_RUN()"

This prepares a small stored-proc call in Snowflake to refresh any "ready to run" flags or perform housekeeping. The stored procedure lives inside Snowflake.

aws_functions.run_snowflake_query_to_df(sfOptions, update_schedule_query)

What runs: run_snowflake_query_to_df from aws_functions_python_v3_snowflake.py.

Line-by-line inside run_snowflake_query_to_df:

conn = snowflake.connector.connect(user=sfOptions['sfUser'], authenticator='SNOWFLAKE_JWT', private_key=sfOptions['pem_private_key'], account=..., warehouse=..., database=..., schema=...) — opens Snowflake connection using JWT (private key).

cursor = conn.cursor()

cursor.execute(query) executes the CALL ... statement.

Because CALL is not a SELECT, the function will return number of affected rows (or 0) and then close cursor & connection.

Effect: SP_UPDATE_READY_TO_RUN runs in Snowflake, updating control flags as intended.

G — Fetch framework config from Snowflake

df_config = aws_functions.fetch_snowflake_config(sfOptions)

What runs: fetch_snowflake_config in aws_functions_python_v3_snowflake.py.

Inside fetch_snowflake_config:

It sets snowflake_query_config = "SELECT \* FROM INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_CONFIG;".

Calls run_snowflake_query_to_df(sfOptions, snowflake_query_config) and returns the resulting Pandas DataFrame df_config.

Result: df_config is a Pandas DataFrame holding values such as PARTITION_SIZE, MAX_CONCURRENT_JOBS, WORKER_TYPE, RECON_FULL_DAY_OF_WEEK, etc. The controller uses these to decide how many parallel jobs to kick off and partition sizes.

recon_full_day_of_week = df_config.iloc[0]['RECON_FULL_DAY_OF_WEEK']

Read recon-full scheduling setting from config row 0 (the framework design uses a single row for configuration).

recon_full_day_of_week = int(recon_full_day_of_week) with try/except

Ensures the config value is integer 1..7, otherwise an error is raised. This is a guard so the schedule interpretation is reliable.

H — Determine current "business timezone" day-of-week (Snowflake-side)

snowflake_query_to_get_day_of_week = """ SELECT DAYOFWEEKISO(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) AS DAY_OF_WEEK; """

Build SQL to get the current ISO day-of-week in the business timezone (America/Los_Angeles). Snowflake is used for timezone correctness rather than local Python.

df_day_of_week = aws_functions.run_snowflake_query_to_df(sfOptions, snowflake_query_to_get_day_of_week)

Execute the SQL in Snowflake and fetch result as DataFrame.

day_of_week = int(df_day_of_week.iloc[0]['DAY_OF_WEEK'])

Now the controller knows whether it’s the recon-full scheduled day in the business timezone.

I — Check whether RECON-FULL already ran today

Build already_ran_today_sql:

SQL checks INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_LOG for any RECON-FULL run today in the business timezone. It returns boolean TRUE/FALSE.

df_already = aws_functions.run_snowflake_query_to_df(sfOptions, already_ran_today_sql)

Run the SQL and fetch result into DataFrame.

already_ran_today = bool(df_already.iloc[0]['ALREADY_RAN'])

Interpret the result as Python bool.

is_recon_day = (day_of_week == recon_full_day_of_week)

Determine whether today is designated recon-full day.

J — Toggle whether to execute RECON-FULL or INCR

if is_recon_day and not already_ran_today:

If today's the recon day and recon hasn't run yet: set execute_recon_full = "Y" and enable RECON-FULL loads by updating INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_CTRL rows from INCR → RECON-FULL (for eligible rows).

enable_recon_full_load_sql = "UPDATE INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_CTRL SET LOAD_TYPE = 'RECON-FULL', READY_TO_RUN = 'TRUE' WHERE LOAD_TYPE = 'INCR' AND LOAD_ENABLE_FLAG = TRUE AND SOURCE1_OBJECT_NAME != 'BSEG' AND NOT EXISTS (... check today has RECON-FULL in log ...)"; aws_functions.run_snowflake_query_to_df(sfOptions, enable_recon_full_load_sql)

This updates control table rows to mark them as RECON-FULL, so those tables will be run in RECON-FULL mode this run.

Else branch: execute_recon_full = "N" and the script resets any leftover RECON-FULL rows back to INCR with disable_recon_full_load_sql. This prevents stale flags from causing unwanted full recon loads.

Effect: The controller now determines whether this scheduling run will trigger RECON-FULL loads for eligible tables or just INCR loads.

K — Build list of tables in scope (single string V_INPUT) for HANA partitioning TVF

ctrl_query_to_get_table_list = f""" SELECT LISTAGG(SOURCE1_SCHEMA_NAME || '.' || SOURCE1_OBJECT_NAME || '.' || LOAD_TYPE, ',') WITHIN GROUP (ORDER BY SOURCE1_SCHEMA_NAME) AS V_INPUT FROM INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_CTRL WHERE LOAD_ENABLE_FLAG = TRUE AND READY_TO_RUN = 'TRUE' AND LOWER(COALESCE(DELETE_PROCESS_STATUS, 'na')) <> 'in_progress' AND LOWER(COALESCE(LOAD_STATUS, 'na')) <> 'in_progress' AND ((LOAD_TYPE IN ('FULL','INCR','FULL-REFRESH') AND '{execute_recon_full}' = 'N') OR (LOAD_TYPE IN ('RECON-FULL') AND '{execute_recon_full}' = 'Y')); """

This SQL selects rows from the control table that are enabled & ready to run and not in progress, and that match the choice of whether to do RECON-FULL or INCR. It concatenates schema.table.load_type entries into one big CSV string V_INPUT. This V_INPUT is the input to the HANA TVF tf_get_table_partitioning_details.

df = aws_functions.run_snowflake_query_to_df(sfOptions, ctrl_query_to_get_table_list)

Execute and fetch. df will have one row with column V_INPUT. If no tables are in scope, v_input will be None/empty.

v_input = df.iloc[0]['V_INPUT'] if not df.empty else None

Extract the V_INPUT string. If empty, the controller logs and exits gracefully — no work to do.

if not v_input: logger.info("No tables in scope based on current LOAD_TYPE and flags. Exiting gracefully."); sys.exit(0)

Stop right here if nothing to do — good early exit.

L — Call HANA TVF to get partitioning & worker plan

Compose hana_query = f""" SELECT LOAD_TYPE, SCHEMA_NAME, TABLE_NAME, SIZE_GB, NO_OF_WORKERS, BATCH_GROUP FROM "APP_EBI"."tf_get_table_partitioning_details"( V_INPUT => '{v_input}', partition_size => {df_config.loc[0,'PARTITION_SIZE']}, max_tables_per_batch => {df_config.loc[0,'MAX_TABLES_PER_BATCH']}, max_workers => {df_config.loc[0,'MAX_WORKERS']}, min_workers => {df_config.loc[0,'MIN_WORKERS']}, cores_per_worker => {df_config.loc[0,'CORES_PER_WORKER']}, worker_ranges => {df_config.loc[0,'WORKER_RANGES']} ); """

This builds a HANA Table-Valued Function (TVF) call that returns per-table partitioning suggestions, number of workers needed, batch groups and sizes. The TVF is executed in HANA; it determines how to chunk HANA data efficiently for parallel extraction.

hana_df = aws_functions.execute_hana_query(hana_query, db_url, db_username, db_password, jdbc_driver_name)

What runs: execute_hana_query from aws_functions_python_v3_snowflake.py.

Inside that function (line-by-line):

Logs start and SQL text.

Parses db_url to get HANA host and port.

Connects to HANA via hdbcli.dbapi.connect(address=hana_host_name, port=hana_port_num, user=db_username, password=db_password).

Executes the hana_query via cursor.execute(hana_query).

rows = cursor.fetchall() — fetch results.

Converts rows to a pandas DataFrame: df = pd.DataFrame(rows) and returns it.

Result: hana_df contains rows with (LOAD_TYPE, SCHEMA_NAME, TABLE_NAME, SIZE_GB, NO_OF_WORKERS, BATCH_GROUP) — the planned batch execution details.

hana_df.columns = ['LOAD_TYPE', 'SCHEMA_NAME', 'TABLE_NAME', 'SIZE_GB', 'NO_OF_WORKERS', 'BATCH_GROUP']

Label the columns so the later function load_hana_config_to_snowflake can load them into Snowflake and the MERGE can use these fields.

M — Load hana_df into Snowflake EXT table

success, nrows = aws_functions.load_hana_config_to_snowflake(hana_df, sfOptions)

What runs: load_hana_config_to_snowflake in aws_functions_python_v3_snowflake.py. Step-by-step:

It opens a Snowflake connection using snowflake.connector.connect(user=sf_options['sfUser'], authenticator='SNOWFLAKE_JWT', private_key=sf_options['pem_private_key'], account=..., warehouse=..., database=..., schema="INTEGRATION", table = "AWS_ETL_FRAMEWORK_LOAD_CTRL_EXT") — note: slight bug in original code (an extra table = ... in connect call) but intent is to connect and then truncate and write.

Executes TRUNCATE TABLE <db>.INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_CTRL_EXT to clear out old results.

Uses write_pandas(conn, df, table_name="AWS_ETL_FRAMEWORK_LOAD_CTRL_EXT", schema="INTEGRATION", database=sf_options['sfDatabase']) to upload the pandas DataFrame into Snowflake.

Returns (success, nrows) where success is boolean and nrows number of rows uploaded.

Effect: Snowflake now has an AWS_ETL_FRAMEWORK_LOAD_CTRL_EXT table populated with the HANA-planned partitions.

N — Merge plan back into main CTRL table

The controller runs this SQL MERGE (string named update_query) to update INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_CTRL using the \_EXT results. The MERGE sets batch_group, no_of_workers, load_status = 'ready_to_replicate', and configures delete_process_status and recon_process_status appropriately.

affected_rows = aws_functions.run_snowflake_query_to_df(sfOptions, update_query)

Executes the MERGE (a DML). Result is number of rows affected. After this, the main control table has been updated to mark rows as ready_to_replicate.

O — Compute batch details (listagg)

Controller builds snowflake_query_to_get_batch_details = f""" SELECT LISTAGG(CAST(BATCH_GROUP AS VARCHAR) || '~' || CAST(NO_OF_WORKERS AS VARCHAR), ',') WITHIN GROUP (ORDER BY BATCH_GROUP) AS BATCH_DETAILS FROM ( SELECT DISTINCT BATCH_GROUP, NO_OF_WORKERS FROM INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_CTRL WHERE LOAD_ENABLE_FLAG = TRUE AND READY_TO_RUN ='TRUE' AND ((LOAD_TYPE IN ('FULL', 'INCR', 'FULL-REFRESH') AND '{execute_recon_full}' = 'N') OR (LOAD_TYPE IN ('RECON-FULL') AND '{execute_recon_full}' = 'Y')) ); """

This builds BATCH_DETAILS as a CSV like "1~10,2~5,3~8" that encodes distinct batch groups and their worker needs.

batch_details_df = aws_functions.run_snowflake_query_to_df(sfOptions, snowflake_query_to_get_batch_details)

Run this SQL and fetch result.

batch_details = batch_details_df.iloc[0]['BATCH_DETAILS']

Extract CSV string.

Later: batch_groups, no_of_workers = aws_functions.extract_batch_details(batch_details)

Uses the helper (split on comma and ~) to return batch_groups list and no_of_workers list.

P — Determine concurrency & job params

Controller calculates max_concurrent_jobs based on config row (full vs non-full recon). Also reads partition_size and worker_type from df_config.

job_name = "glue-eas-etl-glue-read-from-hana-replicate-to-snowflake"

This is the worker Glue job that will be launched per-batch. It’s the Spark job (Job 1) whose code you provided earlier.

Additional logic: If max_batch_groups > 50, the controller adjusts BATCH_GROUP assignments to avoid too many groups (it may reassign some batch group numbers in CTRL table). This involves selecting target object names for the last 5 max groups and updating BATCH_GROUP values in the control table. This is an administrative step to keep batch group numbers manageable.

Q — Launch worker Glue jobs (parallel controlled)

completed_jobs = aws_functions.run_aws_glue_jobs_parallel_controlled_no_of_execution(job_name, batch_groups, max_concurrent_jobs, worker_type, batch_details, partition_size, account_env_number, job_run_id, sns_topic_arn, execute_recon_full, secret_name_hana, secret_name_snowflake, secret_name_snowflake_keypair, sfOptions)

This calls the big helper that starts and monitors Glue worker jobs in parallel — I explained its internal flow earlier; key aspects in chronological order:

For each batch_group in batch_groups, it:

If concurrency limit hit, poll active jobs and wait while respecting max_concurrent_jobs.

When capacity available, call start_job(batch_group):

get_workers_for_batch() → looks up the worker count for that batch group (parsing batch_details string).

update_worker_type_no_of_worker_glue_job(job_name, worker_type, no_of_workers) → updates the Glue job definition to use the right worker type & number (so the job will run with the required compute).

glue_client.start_job_run(JobName=job_name, Arguments={...}) → actually starts the worker Glue Spark job, passing runtime arguments: --batch_group, --partition_size, --main_job_run_id (controller job_run_id), --secret_name_hana, --secret_name_snowflake, --secret_name_snowflake_keypair.

start_job returns job_id (Glue JobRunId) which is appended to active_jobs.

The loop continues until all batch_groups have been started and then waits until active_jobs is empty.

Every ~20 minutes it refreshes df_config and may adjust max_concurrent_jobs mid-run; it also calls check_long_running_table_job to send alerts if a table has been running too long.

Result: Worker Glue jobs have been launched concurrently per configured batch groups and worker counts.

R — Final cleanup & delete old logs

After all jobs finish, controller runs delete_query_log = f"DELETE FROM INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_LOG WHERE load_start_time < DATEADD(DAY, -30, CURRENT_TIMESTAMP());" and executes it via aws_functions.run_snowflake_query_to_df(sfOptions, delete_query_log) to purge old logs.

Controller finishes. If an exception occurred anywhere in the try-block, the except block at the bottom of the controller:

Logs error.

Builds email_subject and email_body.

Sends email via aws_functions.send_email(sns_topic_arn, email_subject, email_body).

sys.exit(1) to mark controller run as failed.

2. WORKER Glue job (Job 1) — what runs when controller starts a worker job

When the controller calls glue_client.start_job_run(JobName=job_name, Arguments={...}) for the worker job, AWS Glue will spin up the worker job (a Spark-enabled Glue job) whose script is the first file you pasted. I’ll walk through that worker job in execution order, showing how it uses both the Spark helpers file and the python helpers where relevant.

Worker job startup & argument parsing

At process start, worker job code executes imports (boto3, json, time, Spark/Glue contexts, habli.dbapi, logging, aws_functions_spark_v3_snowflake aliased as aws_functions, snowflake.connector etc.). Import time just loads modules; main logic starts below.

Logging setup:

logging.getLogger().setLevel(logging.INFO)

Log job start: logging.info("Glue Job Start Time: " + str(datetime.now()))

logging.info("Define Clients: " + str(datetime.now()))

Boto3 clients created:

sns_client = boto3.client('sns')

glue_client = boto3.client("glue")

client = boto3.client("secretsmanager")

These are used by the worker to send alerts, talk to Glue API if needed, and read secrets.

Read runtime args:

args = getResolvedOptions(sys.argv, [
'batch_group','partition_size','JOB_NAME','sns_topic_arn','account_env_number',
'main_job_run_id','secret_name_hana','secret_name_snowflake','secret_name_snowflake_keypair'
])

This reads arguments passed by the controller at start. Important values:

batch_group — the batch group this worker will process (controls which tables it picks up from the control table).

partition_size — passed from controller, influences HANA partitioning.

main_job_run_id — controller job run id for correlation.

secret*name*\* — names of Secrets Manager entries so the worker can fetch HANA & Snowflake credentials locally.

job_run_id = aws_functions.get_running_job_ids_by_batch_group(job_name, batch_group)

This calls a helper that inspects running Glue job runs and finds the current job run id for this job filtered by --batch_group. It’s used to uniquely identify this worker run in logs and control table updates. (Implementation in aws_functions_python_v3_snowflake.py iterates glue_client.get_job_runs(JobName=job_name) and matches Arguments['--batch_group']).

Secret fetch — HANA:

get_secret_value_response = client.get_secret_value(SecretId=secret_name_hana)

secret = json.loads(get_secret_value_response['SecretString'])

Extract db_username, db_password, db_url, jdbc_driver_name for HANA (same as controller did, but worker fetches again).

Secret fetch — Snowflake:

Fetch secret_name_snowflake to get Snowflake credentials and settings.

Fetch secret_name_snowflake_keypair to get the certificate string.

Generate Snowflake private key(s) (Spark and non-spark variants):

snowflake_ppk, snowflake_ppk_non_spark = aws_functions.generate_snowflake_ppk(snowflake_certificate_string, snowflake_private_key_passphrase)

Important: You told me the Spark helpers produce two variants — snowflake_ppk for the Spark connector (string form) and snowflake_ppk_non_spark for the Python connector. In the Spark helper file you provided earlier, generate_snowflake_ppk had a branch to return base64 string (for Spark) vs RSA object (for non-spark). The worker keeps both forms in row for functions that need either.

Compose sfOptions used by Spark Snowflake connector:

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

These options are what spark.read.format("snowflake").options(\*\*sfOptions) expects — in the Spark helpers it uses sfOptions to access things like sfSchema, sf_temp_Schema, etc.

Worker main try/except — process control loop

Enter try: block — this wraps the per-table loop. If a fatal exception bubbles here, the outer except at bottom will capture it and send an SNS email.

Re-assign local variables batch_group, partition_size = args['partition_size'], and get job_name, worker_count = aws_functions.get_glue_job_name_and_worker_count() (this helper likely reads env/glue definitions to determine worker count, not provided here but it's used).

Build snowflake_query_to_get_tables_for_replication = f""" SELECT ... FROM INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_CTRL WHERE LOAD_ENABLE_FLAG = TRUE AND LOAD_STATUS = 'ready_to_replicate' AND BATCH_GROUP = {batch_group} ORDER BY SOURCE1_OBJECT_NAME; """

This query asks Snowflake for all control-table rows for this BATCH_GROUP that are enabled and ready to replicate. This is the list of tables the worker will process.

snowflake_load_control_df = aws_functions.run_snowflake_query_to_df_spark(sfOptions, snowflake_query_to_get_tables_for_replication)

What runs: run_snowflake_query_to_df_spark from aws_functions_spark_v3_snowflake.py.

Inside run_snowflake_query_to_df_spark:

df = spark.read.format("snowflake").options(\*\*sfOptions).option("query", query).load() — uses Spark Snowflake connector to run the query and produce a Spark DataFrame; then .toPandas() returns a pandas DataFrame. (So result is pandas).

Result: snowflake_load_control_df is a pandas DataFrame with rows describing each table to replicate in this batch group. If empty, there’s nothing for this worker to do.

if snowflake_load_control_df is None or snowflake_load_control_df.empty: logging.info("... no more tables ..."); try: sys.exit(0) except SystemExit: logging.info("Exiting script after job commit.")

If empty, the worker exits cleanly.

Worker loop over tables (core replication loop)

for index, row in snowflake_load_control_df.iterrows():

The worker iterates rows of control table (each row = a table to replicate). For each row it sets up many status fields and timings and then depending on LOAD_TYPE executes different flows (FULL, INCR, FULL-REFRESH, RECON-FULL).

I’ll step through the standard path for each major LOAD_TYPE, but first the common initialization steps executed at top of loop:

Per-row initialization (common)

primary_key_str = "" — init.

row['error_details'] = '' — prepare error field.

partition_count = -1 — initialize.

Determine load_status: if row['LOAD_STATUS'] == 'ready_to_replicate' set to 'in_progress' else leave as-is. This is the worker updating its in-memory row before logging status to Snowflake.

delete_process_status, recon_process_status — set based on current control values and LOAD_TYPE. For example, DELETE_PROCESS_STATUS gets set to 'in_progress' for incremental loads (but not_applicable for certain full scenarios). This logic decides whether delete processing will happen.

Initialize all timestamps to '1900-01-01 00:00:00' (default placeholders) and counters to 0. row['snowflake_extvol'] = snowflake_extvol, row['job_run_id'] = job_run_id, row['main_job_run_id'] = main_job_run_id, row['snowflake_ppk'] = snowflake_ppk, row['snowflake_ppk_non_spark'] = snowflake_ppk_non_spark. These fields are packaged into row and later passed into update_load_ctrl_log_entry to log progress.

sfOptions['sfSchema'] = row['TARGET_SCHEMA_NAME'] and sfOptions['sfWarehouse'] = row['WAREHOUSE']

For this table, the worker updates the sfOptions to use the correct target schema and warehouse as specified in control row. This influences downstream Snowflake DDLs & writes.

Worker: Branch by LOAD_TYPE — high-level paths

The worker tests row['LOAD_TYPE'] and goes into one of these flows:

FULL-REFRESH — do full refresh creation (new table creation & data load).

FULL or INCR — existing table load (merge or insert).

RECON-FULL — special recon flow to compute mismatches and reconcile tables.

Delete processing block if row['DELETE_ENABLE_FLAG'] true — separate logic for deletes.

I’ll follow a typical incremental or full path where INCREMENTAL_FIELD == 'row_id' and show exact helper call sequences since those are the ones that call the Spark helper functions you provided.

Example: Typical INCR / FULL path (most common operations)
A — Decide whether to generate table DDLs / names

If row['INCREMENTAL_FIELD'] == 'row_id' and row['TARGET1_OBJECT_NAME'] is missing, the worker generates Snowflake table names and DDLs by calling:

drop_ddl, create_ddl, iceberg_ddl, snowflake_table_stg, iceberg_table_name, snowflake_column_names_f_pandas = aws_functions.generate_snowflake_tblnames_ddls([], row, sfOptions, "Get Columns")

What runs: generate_snowflake_tblnames_ddls inside aws_functions_spark_v3_snowflake.py (you pasted this function). Key line-by-line inside it in runtime order:

type_mapping mapping defined (Spark types -> Snowflake types).

If operation_type != "generate_ddl": df = spark.createDataFrame([], StructType([])) — but here operation_type is "Get Columns", so logic goes into the if row["LOAD_TYPE"] in ["FULL-REFRESH", "INCR", "FULL"]: branch.

Determine iceberg_table_name from SOURCE1_SCHEMA_NAME and SOURCE1_OBJECT_NAME (with replace_special_characters_with_underscore(...)), or take TARGET1_OBJECT_NAME if provided.

Determine snowflake_table_stg = iceberg_table_name + "\_STG".

If row["LOAD_TYPE"] != "RECON-FULL": it queries Snowflake INFORMATION_SCHEMA via Spark connector to get existing column list:

snowflake_column_names_df = spark.read.format("snowflake").options(\*\*sfOptions).option("query", f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{sfOptions['sfSchema']}' AND TABLE_NAME = '{iceberg_table_name.upper()}'").load()
snowflake_column_names_df_pandas = snowflake_column_names_df.toPandas()

This checks whether target Iceberg table exists & grabs current column names. Useful to decide whether to create or not.

If the incoming df (the provided Spark DF) has rows, it builds drop_ddl, create_ddl for staging table, and iceberg_ddl for Iceberg final table using map_spark_to_snowflake_type for each field in df.schema.fields. (This uses the Spark schema.)

Return (drop_ddl, create_ddl, iceberg_ddl, snowflake_table_stg, iceberg_table_name, snowflake_column_names_df_pandas).

Effect: Worker now has DDL SQL strings to create staging and final Iceberg tables, and the names to use.

B — Log initial status

aws_functions.update_load_ctrl_log_entry(sfOptions=sfOptions, row_array=row, IN_TBL_NAME="CTRL/LOG", IN_LOAD_STAGE="INITIAL")

What runs: update_load_ctrl_log_entry in aws_functions_spark_v3_snowflake.py. Step-by-step (runtime):

Accepts row_array (pandas Series or dict) and sfOptions.

Temporarily sets sfOptions['pem_private_key'] = row_array['snowflake_ppk_non_spark'] (so the non-spark private key is used for the non-spark connection inside run_snowflake_query_to_df).

Constructs a long CALL INTEGRATION.SP_ETL_FRAMEWORK_TBL_LOADS(... long parameter list ...) statement embedding fields from row_array (job_run_id, LOAD_TYPE, SOURCE1_SCHEMA_NAME, etc.). This stored procedure writes an entry to a control/log table — used for audit and failure recovery.

Calls run_snowflake_query_to_df(sfOptions, update_load_ctrl) which connects to Snowflake with pem_private_key and executes the CALL.

Restores sfOptions['pem_private_key'] = row_array['snowflake_ppk'].

Effect: An "INITIAL" log entry is recorded for the table load in Snowflake with initial statuses and timestamps.

C — Partitioning & HANA predicate generation (for FULL-REFRESH or FULL)

If row['LOAD_TYPE'] == 'FULL-REFRESH':

Build HANA procedure call:

hana_query = f'''CALL "APP_EBI"."sp_generate_dynamic_partition_queries_replication"( 'FULL', '{row['SOURCE1_OBJECT_NAME']}', '{row['SOURCE1_SCHEMA_NAME']}', {partition_size}, 200000000, 0)'''

predicates_list = aws_functions.execute_hana_query(hana_query, db_url, db_username, db_password, jdbc_driver_name, row['SOURCE1_OBJECT_NAME'], row['SOURCE1_SCHEMA_NAME'], [], 'FULL')

What runs: execute_hana_query in aws_functions_spark_v3_snowflake.py or (if worker invoked Python version) aws_functions.execute_hana_query from Python helper — either way the TVF/PROC on HANA returns a list of predicate strings like ["ROW_ID >= 1000 AND ROW_ID <= 1999", ...] representing partitions. In the Spark variant you supplied, if predicates argument is empty it connects to HANA using dbapi.connect, executes the stored proc and then runs a predicate retrieval query against EBI.T_HANA_REPLICATION_FRAMEWORK using Spark JDBC to get a PREDICATES column which is split into a list. In simpler Python version it executes the TVF and returns a pandas DataFrame.

Effect: predicates_list contains the HANA partition queries that the worker will iterate over to extract data in parallel/partitioned loads.

primary_key_list, snowflake_columns_list, hana_select_statement, snowflake_recon_query = aws_functions.get_matching_table_columns_hana_snowflake(row, db_url, db_username, db_password, jdbc_driver_name, sfOptions, snowflake_column_names_df_pandas)

This helper finds matching columns between HANA and Snowflake and prepares the HANA SELECT statement to extract the data. It also builds snowflake_recon_query used later for recon. (You didn’t paste this function's code; we assume it uses JDBC metadata queries and returns the required lists/SQL.)

D — Extract from HANA and load into Snowflake: call Spark helper

Now depending on the scenario the worker calls one of the spark helper functions:

For full/insert/initial loads: no_of_records_processed, snowflake_start_time, snowflake_end_time, hana_start_time, hana_end_time, recon_start_time, recon_end_time, delta_record_count, delta_record_samples = aws_functions.hana_to_snowflake_insert(hana_select_statement, db_url, db_username, db_password, jdbc_driver_name, predicates_list, row, sfOptions)

For merge/INCR flows: no_of_records_processed, snowflake_start_time, snowflake_end_time, hana_start_time, hana_end_time = aws_functions.hana_to_snowflake_merge(row, hana_select_statement, snowflake_columns_list, db_url, db_username, db_password, jdbc_driver_name, predicates_list, primary_key_str, sfOptions)

I’ll step into each Spark helper now (you already pasted them); I’ll unfold the exact runtime line-by-line actions they perform.

3. SPARK HELPER: hana_to_snowflake_merge(...) — exact runtime steps (line-by-line logic)

You provided this function. Here’s the chronological execution and explanation of each key block.

Function signature:

def hana_to_snowflake_merge(row, hana_select_query, snowflake_columns_list, db_url, db_username, db_password, jdbc_driver_name, predicates, primary_key_str, sfOptions):

row is the control row dict/Series.

hana_select_query is a HANA SELECT (or subquery) string.

predicates is a list of predicate strings for JDBC partitioning.

sfOptions is the Spark Snowflake connector options dict.

args_array = ['JOB_NAME'] → args = getResolvedOptions(sys.argv, args_array) → job_name = args['JOB_NAME'] → job_run_id = args['JOB_RUN_ID']

Reads Glue runtime arguments inside the Spark helper. The helper expects JOB_RUN_ID to be passed in — this links logs to job_run_id. (If not present, these lines will error; but typical Glue job runs supply JOB_RUN_ID.)

glue_client = boto3.client('glue') — used to read worker config (WorkerType, NumberOfWorkers). The helper uses these to determine a proper number of partitions.

logging.info("Starting hana_to_snowflake_merge()") — log start.

Prepare JDBC properties used by Spark to read from HANA:

properties = {"user": db_username, "password": db_password, "driver": jdbc_driver_name}

Read data from HANA using Spark JDBC:

df = spark.read.jdbc(url=db_url, table=hana_select_query, predicates=predicates, properties=properties)

Spark makes JDBC connections to HANA and executes the hana_select_query with the predicates argument supplied by the HANA partitioning TVF. This produces a Spark DataFrame df containing a partition’s rows.

no_of_records_processed = df.count()

Materialize count to know rows processed. Important for logging and control table updates.

If no*of_records_processed == 0: set hana_start_time, hana_end_time, snowflake*\* timestamps to now and return zeros — nothing to load.

df = replace_special_characters_with_underscore_in_spark_dataframe_column_names(df)

Normalize column names to safe identifiers (remove special characters). This keeps Snowflake DDL friendly.

Add metadata columns if missing:

add_column_if_not_exists(df, "delete_flag", lit("N").cast("string"))

add_column_if_not_exists(df, "insert_ts", lit(current_ts).cast("timestamp"))

add_column_if_not_exists(df, "update_ts", lit(current_ts).cast("timestamp"))

These ensure every row has these meta columns that Snowflake Iceberg tables expect.

Generate DDLs and table names:

drop_ddl, create_ddl, iceberg_ddl, snowflake_table_stg, iceberg_table_name, snowflake_column_names_df, ddls_pandas = generate_snowflake_tblnames_ddls(df,row,sfOptions,operation_type="generate_ddl")

Calls the generate_snowflake_tblnames_ddls function to produce SQL DDLs for temp staging and final Iceberg tables and the staging table name. We discussed that function earlier: it uses the Spark DF schema to create create_ddl for sfOptions['sfTemp_Schema'].<snowflake_table_stg> and iceberg_ddl for the final Iceberg table.

Prepare merge/insert SQL:

merge_script, insert_script = prepare_snowflake_merge_statements(row, snowflake_columns_list, primary_key_str, iceberg_table_name, snowflake_table_stg)
load_script = merge_script # or insert_script for FULL-REFRESH

prepare_snowflake_merge_statements (not pasted) builds the final MERGE SQL referencing the staging table and the Iceberg final table. The load_script will be run as Snowflake postactions.

Convert timestamp columns to string before pushing to Snowflake:

for field in df.schema.fields:
if isinstance(field.dataType, TimestampNTZType):
df = df.withColumn(field.name, col(field.name).cast("string"))

Spark's TimestampNTZType might not map cleanly — so convert to string for safe transfer. This avoids type-mismatch issues during the Snowflake write.

Determine number of partitions based on Glue worker metadata:

glue_worker_type = glue_client.get_job_run(JobName=args['JOB_NAME'], RunId=args['JOB_RUN_ID'])['JobRun']['WorkerType']
number_of_workers = glue_client.get_job_run(... )['JobRun']['NumberOfWorkers']
num_partitions = int(glue_worker_type[2]) _ 4 _ (int(number_of_workers) - 1)
df = df.repartition(num_partitions)

Uses Glue job run metadata to estimate how many Spark partitions to use for parallel write. This is Spark-specific tuning to match Glue worker capacity.

Convert df to a Glue DynamicFrame:

dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
hana_start_time = datetime.now()
hana_end_time = datetime.now()

Convert Spark DF to Glue DynamicFrame (Glue API convenience object) for the sink write operation.

Build Snowflake pre/post actions:

snowflake_preactions = f"drop ddl;{create_ddl};{iceberg_ddl};"
snowflake_postactions = f"BEGIN;{load_script};COMMIT;"

preactions create staging & final tables, postactions perform the MERGE inside a transaction. These strings are passed to the Spark Snowflake connector so Snowflake executes them in the same session before/after the data write.

Prepare sf_options for the Spark Snowflake connector (note keys: sfURL, sfRole, pem_private_key, sfDatabase, sfSchema, sfTable, preactions, postactions).

glueContext.write_dynamic_frame.from_options(frame=dynamic_frame, connection_type="snowflake", connection_options=sf_options, transformation_ctx="snowflake_node")

This instructs Glue to use the Spark Snowflake connector to write the staging table. Under the hood:

Spark writes the dynamic frame to Snowflake staging table (via the connector) possibly using PUT and COPY INTO semantics.

Snowflake preactions are executed before load, and postactions (the MERGE) are executed after load.

This is the core data transfer HANA → Snowflake for merge flows.

Set snowflake_end_time = datetime.now() and return no_of_records_processed, snowflake_start_time, snowflake_end_time, hana_start_time, hana_end_time

Returns timing and counts to the worker to record in control/log table.

except block catches errors, logs, and re-raises as "hana_to_snowflake_merge failed: {e}". The worker catches exceptions around whole per-row work and handles updates & notifications.

4. SPARK HELPER: hana_to_snowflake_insert(...) — exact runtime steps

This is similar to merge but differs in postactions and recon logic. Key steps line-by-line:

Read Glue args (JOB_NAME, JOB_RUN_ID) and glue_client to inspect worker metadata.

Open Spark session if not present: spark = SparkSession.builder.getOrCreate().

JDBC read from HANA:

df = spark.read.jdbc(url=db_url, table=hana_select_statement, predicates=predicates, properties=properties)

hana_select_statement is a SELECT or subquery; predicates partitions the query.

no_of_records_processed = df.count() — materialize count.

If count == 0 return zeros (with timestamps).

df = replace_special_characters_with_underscore_in_spark_dataframe_column_names(df) — normalize columns.

Add delete_flag, insert_ts, update_ts if missing similarly to merge.

Generate DDLs:

drop_ddl, create_ddl, iceberg_ddl, snowflake_table_stg, iceberg_table_name, snowflake_column_names_df_pandas = generate_snowflake_tblnames_ddls(df, row, sfOptions, operation_type='generate_ddl')

Build staging & final DDLs and get iceberg_table_name.

Build full_load_script (INSERT OVERWRITE INTO final FROM staging) for full loads.

Convert timestamp columns to string if needed.

Repartition df according to glue worker metadata (logic differs slightly here).

dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame") and hana_start_time = datetime.now().

Based on row['LOAD_TYPE'] == "RECON-FULL" construct snowflake_postactions:

For RECON-FULL build reset_delete_flag_query and update_delete_flag_query which run updates against the final Iceberg table to mark delete_flag = 'Y' for rows not present in staging. This is the "recon" comparison implemented via SQL in Snowflake.

For non RECON-FULL sets snowflake_postactions = f"BEGIN;{full_load_script};COMMIT;" and snowflake_preactions = f"{drop_ddl};{create_ddl};{iceberg_ddl}".

Set sf_options for connector and then call:

glueContext.write_dynamic_frame.from_options(frame=dynamic_frame, connection_type="snowflake", connection_options=sf_options, transformation_ctx="snowflake_node")

Same as merge: writes staging data and executes pre/post actions in Snowflake.

Recon analysis: If row['RECON_ENABLE_FLAG']=='true', build the recon SQL (recon_query) that does a LEFT JOIN of staging vs final and returns aggregated delta_record_count and delta_record_samples. Then call run_snowflake_query_to_df_spark(sf_options, recon_query) to fetch the recon stats.

Return no_of_records_processed, snowflake_start_time, snowflake_end_time, hana_start_time, hana_end_time, recon_start_time, recon_end_time, delta_record_count, delta_record_samples.

5. update_load_ctrl_log_entry(...) exact runtime lines (used by Worker to log)

You saw the function earlier. Runtime flow when worker calls it:

Function receives sfOptions, row_array, IN_TBL_NAME, IN_LOAD_STAGE.

If row_array has .to_dict() coerce it to dict.

Validate row_array.get('job_run_id') is not None — we must have job_run_id to log.

Temporarily set sfOptions['pem_private_key'] = row_array['snowflake_ppk_non_spark'] — ensure non-spark private key object is used for run_snowflake_query_to_df.

Build the CALL INTEGRATION.SP_ETL_FRAMEWORK_TBL_LOADS(... long parameter list ...) string with parameters pulled from row_array (LOAD_TYPE, SOURCE1_SCHEMA_NAME, TARGET1_OBJECT_NAME, INCREMENTAL_FIELD_MAX_VALUE, PARTITIONED_INCREMENTAL_FIELD_MAX_VALUE, BATCH_GROUP, WORKER_COUNT, LOAD_STATUS, DELETE_PROCESS_STATUS, RECON_PROCESS_STATUS, error_details, no_of_records_processed, LOAD_END_TIME ... etc.)

run_snowflake_query_to_df(sfOptions, update_load_ctrl) — executes stored procedure in Snowflake to insert/update a log row.

Restore sfOptions['pem_private_key'] = row_array['snowflake_ppk'].

On exception, logs and re-raises.

Effect: every time worker wants to update log/control table (INITIAL, FINAL), it calls this. That is how the control table reflects the run progress.

6. run_snowflake_query_to_df_spark(...) — how worker reads small results from snowflake inside spark flows

When spark code needs query results back (e.g., recon_query result) it calls:

df = spark.read.format("snowflake").options(\*\*sfOptions).option("query", query).load()
df = df.toPandas()
return df

This runs the query via Spark Snowflake connector and returns a pandas DataFrame with results. Good for small recon result sets; not recommended for very large selects.

7. HANA predicate derivation inside spark helper: execute_hana_query(...) (spark variant)

When Spark helper is called with predicates empty, this function:

Parses db_url for host/port.

Connects to HANA via dbapi.connect(address=hana_host_name, port=int(hana_port_num), user=db_username, password=db_password).

Executes the provided stored procedure (e.g., sp_generate_dynamic_partition_queries_replication) via cursor.execute(hana_query) — this stored proc populates HANA table EBI.T_HANA_REPLICATION_FRAMEWORK with PREDICATES.

Then the function uses a Spark JDBC read to select from EBI.T_HANA_REPLICATION_FRAMEWORK:

predicates_df = spark.read.jdbc(url=db_url, table=predicates_filters, properties=properties)

predicates_filters is a subquery that SELECT STRING_AGG(PREDICATES, '||') AS PREDICATES ... which produces aggregated predicate values.

It then splits the PREDICATES string into a list and flatten them into flat_predicates_list and returns that list.

Effect: produce a list of SQL predicates such as ["ROW_ID >= 1 AND ROW_ID <= 1000", "ROW_ID >= 1001 AND ROW_ID <= 2000", ...] which the Spark JDBC read uses to partition the HANA read.

8. Delete processing path in worker

After load processing, if row['DELETE_ENABLE_FLAG'] == True, worker enters delete processing flow:

Sets delete_process_status = 'in_progress' and logs start.

If row['TARGET1_DELETE_TABLE_NAME'] not provided, the worker fabricates names for source & target delete/archive tables (hana_archive_table_name, snowflake_table_name_archive) and sets row['SOURCE1_DELETE_TABLE_SCHEMA_NAME'] etc.

Calls HANA stored proc sp_dynamic_archive_table_delete to purge old archived rows from delete/archive table, via aws_functions.execute_archive_table_delete_hana_query(...). (Not pasted, but similar pattern: use HANA dbapi to execute.)

Get predicates_list_archive by calling the HANA partition TVF for the archive delete table, similar to load partitioning.

sfOptions['sfSchema'] = row['TARGET1_DELETE_TABLE_SCHEMA_NAME'] — adjust schema context.

Call generate_snowflake_delete_tblnames_ddls(...) to get DDLs for delete table staging & target.

primary_key_list_archive, snowflake_columns_list, hana_select_statement_archive, snowflake_recon_query = aws_functions.get_matching_delete_table_columns_hana_snowflake(...) — determine primary key & columns for delete table.

If primary_key_list_archive None, fallback to Athena ctrl table retrieval (via fetch_athena_ctrl_primary_keys, not pasted).

no_of_records_processed_archive = aws_functions.hana_to_snowflake_hard_delete_delete_table(hana_select_statement_archive, snowflake_columns_list, db_url, ... ) — call a specialized helper to perform deletes in Snowflake (implementation not pasted). Likely it writes staging rows and issues DELETE statements in Snowflake to remove rows.

Parse predicates for DELETE_INCREMENTAL_FIELD_MAX_VALUE to update control table with the max row_id processed.

Set delete_process_status = 'completed', delete_end_time = datetime.now().

9. RECON-FULL path (worker)

If row['LOAD_TYPE'] == 'RECON-FULL':

The worker generates/uses TARGET1_OBJECT_NAME or creates it via generate_snowflake_tblnames_ddls.

It calls HANA TVF to get partitions:
predicates_list = aws_functions.execute_hana_query(hana_query, ...)

Prepares hana_select_statement_recon = f'(select "$rowid$" as row_id from "{row["SOURCE1_SCHEMA_NAME"]}"."{row["SOURCE1_OBJECT_NAME"]}") subquery' — this selects row ids from source table.

Calls hana_to_snowflake_insert(hana_select_statement_recon, ...) — this will insert recon data into staging and then do the RECON logic described earlier (reset flags, update delete flags, compute recon stats).

Sets load_status = 'completed' and load_end_time = datetime.now().

recon summary (delta count & samples) is computed inside hana_to_snowflake_insert using SQL recon_query run in Snowflake and returned to worker.

10. Per-row finalization & log update

After successful processing of a row:

Worker sets a bunch of fields on row: no_of_records_processed, partition_count, predicates, LOAD_END_TIME, SOURCE1_PRIMARY_KEY, DELETE_START_TIME, DELETE_END_TIME, RECON_START_TIME, RECON_END_TIME, SNOWFLAKE_START_TIME, SNOWFLAKE_END_TIME, HANA_START_TIME, HANA_END_TIME, LOAD_STATUS, DELETE_PROCESS_STATUS, RECON_PROCESS_STATUS, DELTA_RECORD_COUNT, DELTA_RECORD_SAMPLES.

Then calls aws_functions.update_load_ctrl_log_entry(sfOptions=sfOptions, row_array=row, IN_TBL_NAME="CTRL/LOG", IN_LOAD_STAGE="FINAL")

This writes a FINAL log entry to the Snowflake stored proc SP_ETL_FRAMEWORK_TBL_LOADS containing final statuses, counts, timestamps, and deltas. In your control table this is how LOAD_STATUS gets set to 'completed' or 'failed'.

11. Per-row exception handling inside worker

If an exception occurs inside the per-row try block:

except Exception as error_message: runs.

error_details = str(error_message)[0:500] and sanitized (replace("\n"," ").replace("'", "''")).

Update row fields to reflect failure:

row['error_details'] = error_details

no_of_records_processed = 0

row['LOAD_STATUS'] = 'failed' if it was 'in_progress'

row['DELETE_PROCESS_STATUS'] or row['RECON_PROCESS_STATUS'] become 'failed' if they were 'in_progress'.

aws_functions.update_load_ctrl_log_entry(sfOptions=sfOptions, row_array=row, IN_TBL_NAME="CTRL/LOG", IN_LOAD_STAGE="FINAL") — write final failed entry to log stored procedure so control table reflects failure.

Compose email_subject and email_body for SNS: includes account, job name, table & error details, and then (outside per-row loop) the code may aws_functions.send_email(sns_topic_arn, email_subject, email_body) to notify.

12. Worker job top-level exception handling

If the main try: that wraps the worker code raises (e.g., inability to fetch secrets, unexpected fatal error), the outer except Exception as e: block in the worker:

Logs the error.

Sets error_details = str(e), no_of_records_processed=0, worker_count=0.

Compose email_subject and email_body and call aws_functions.send_email(sns_topic_arn, email_subject, email_body).

The worker then exits — Glue marks job as failed.

13. Notification helper: send_email(...)

This helper simply calls:

sns_client = boto3.client('sns')
sns_client.publish(TopicArn=sns_topic_arn, Subject=email_subject, Message=email_body)

SNS will deliver email to all topic subscribers (your team); this is how both controller and worker notify of errors.

14. Auxiliary helpers referenced in control flow (quick explanations)

extract_batch_details(batch_details) — parses "1~10,2~5" into [1,2] and [10,5]. Used by controller to derive batch_groups and worker counts.

get_workers_for_batch(batch_group, batch_details) — returns number of workers for a given batch by mapping parsed lists.

update_worker_type_no_of_worker_glue_job(job_name, worker_type, number_of_workers) — calls glue.get_job(JobName=job_name) to get current job definition, builds job_update_params with new WorkerType and NumberOfWorkers, and calls glue.update_job(JobName=..., JobUpdate=...) to update Glue job definition before starting the run. This is how the controller configures the next worker run to use the proper Glue compute size.

trigger_glue_jobs(job_name) — trivial wrapper doing glue_client.start_job_run(JobName=job_name) and returning the job run id.

check_jobs_status(job_name, job_ids) — polls Glue for job run status and raises exception if any job failed; otherwise confirms success.

get_running_job_ids_by_batch_group(job_name, batch_group) — used in the worker to detect the job_run_id for the running worker invocation. It iterates glue_client.get_job_runs(JobName=job_name) and finds Arguments['--batch_group'] == batch_group and returns the matching job run id.

check_long_running_table_job(account_env_number, sns_topic_arn, job_name, sfOptions, duration_threshold) — constructs SQL to fetch any rows in AWS_ETL_FRAMEWORK_LOAD_CTRL where LOAD_STATUS='in progress' and DATEDIFF('MINUTE', LOAD_START_TIME, COALESCE(LOAD_END_TIME, CURRENT_TIMESTAMP)) >= {threshold}. If any rows found, builds an email_body and calls send_email(...).

15. Exact sequence of state changes to Snowflake control tables (chronological)

For each table processed by a worker, the controller + worker produce the following series of updates via calls to SP_ETL_FRAMEWORK_TBL_LOADS (the stored proc invoked by update_load_ctrl_log_entry):

Controller initially marks rows READY_TO_RUN = 'TRUE' and may update LOAD_TYPE to RECON-FULL (if the recon day logic sets it). Controller also sets load_status = 'ready_to_replicate' via the initial MERGE.

Worker for each table issues update_load_ctrl_log_entry(..., IN_LOAD_STAGE="INITIAL") — this writes an "INITIAL" log row marking LOAD_STATUS='in_progress' and populates LOAD_START_TIME.

Worker performs hana_to_snowflake_merge/insert(...) which:

Writes staging data into sfOptions['sf_temp_Schema'].<snowflake_table_stg>.

Executes preactions (create table) and postactions (MERGE or INSERT OVERWRITE) in Snowflake inside that connector call.

If recon is enabled, it runs the recon SQL and reads small results back to the worker.

Worker sets LOAD_STATUS='completed' and LOAD_END_TIME and calls update_load_ctrl_log_entry(..., IN_LOAD_STAGE="FINAL") to record final counts & timestamps.

If delete processing runs, worker calls stored proc logs for active delete steps and writes DELETE_PROCESS_STATUS updates likewise.

On error at any point, worker marks LOAD_STATUS='failed', sets error_details, and calls update_load_ctrl_log_entry(..., IN_LOAD_STAGE="FINAL").

Controller periodically deletes old log rows older than 30 days via DELETE query run at end.

16. Notifications: when and how

Controller-level fatal errors (e.g., secret fetch failed, Snowflake auth error): caught by controller except at bottom — aws_functions.send_email(sns_topic_arn, email_subject, email_body) with job-run id and error details.

Per-table worker errors: worker catches exceptions for each table and calls update_load_ctrl_log_entry(... "FINAL") to record failure, and composes an email_subject and email_body for this table then aws_functions.send_email(sns_topic_arn, email_subject, email_body) to notify team.

Long-running alert: controller periodically calls check_long_running_table_job(...) which will send an SNS email if any table duration exceeds threshold.

17. End-to-end example timeline (concise)

Controller job starts — fetch secrets, generate private key, compute whether recon-full should run, build V_INPUT, call HANA TVF for partitioning, load \_EXT table in Snowflake and MERGE to mark rows ready_to_replicate.

Controller computes batch_groups and starts worker Glue jobs in parallel (calls update_worker_type_no_of_worker_glue_job then start_job for each batch group). It monitors concurrent jobs and refreshes config periodically.

Worker job (per-batch) starts — reads secrets, builds sfOptions, queries control table for rows with BATCH_GROUP = X and LOAD_STATUS='ready_to_replicate'.

Worker iterates rows — for each table:

Sets LOAD_STATUS='in_progress', writes an INITIAL log row in Snowflake.

Calls HANA stored procs / TVFs to get predicates for partitioned extraction (if needed).

Reads HANA data via Spark JDBC partitioned by predicates.

Cleans DataFrame, adds metadata columns, maps datatypes.

Calls generate_snowflake_tblnames_ddls to compute staging/final table DDLs and names.

Writes staging data to Snowflake using Glue Spark Snowflake connector which executes pre/post actions including MERGE/INSERT commands inside Snowflake.

For recon runs, executes recon SQL to compute delta counts and samples (reads back via Spark Snowflake connector).

Calls update_load_ctrl_log_entry(... "FINAL") to write final statuses & counts.

If errors occur, update final log entry as failed and send SNS email.

Worker finishes when the worker’s list of tables for that batch group is exhausted and exits. Glue marks the job SUCCEEDED or FAILED based on final status.

Controller sees job completions via Glue API and when all are done executes cleanup (e.g., delete old logs) and exits.

18. Important implementation details, pitfalls & side-effects (practical)

Where are secrets used? Both controller and worker read secrets from AWS Secrets Manager at runtime. The generated private key is used differently: Spark connector needs a PEM string variant; python connector needs an RSA object. Your generate_snowflake_ppk handles both.

Two Snowflake connectors used:

Spark Snowflake connector inside Glue Spark job (via glueContext.write_dynamic_frame.from_options and spark.read.format("snowflake")) — optimized for distributed writes/reads.

Python Snowflake connector in run_snowflake_query_to_df and update_load_ctrl_log_entry — used by small operations and control table updates. That's why both pem_private_key and pem_private_key_non_spark are needed.

Glue worker sizing affects repartitioning — helper reads Glue job run metadata (get_job_run) to determine num_partitions. If worker sizing is miscalibrated, you may under/over partition and either underutilize or saturate cluster.

HANA stored procedures drive partitioning — the TVFs/SPs in HANA are the source-of-truth for partition predicates. If these produce skewed or incorrect predicates you may get imbalanced loads.

Control table stored procedures (SP_ETL_FRAMEWORK_TBL_LOADS) are critical for idempotency and audit. They must accept the same parameter order as update_load_ctrl_log_entry constructs.

Race conditions: controller and worker both query & update the same control tables. This system relies on the controller setting READY_TO_RUN and the worker updating statuses. Make sure MERGE and CALL stored procedures use appropriate transaction logic / locks to avoid races.

Error handling: exceptions are caught at per-row level and at top-level. Worker marks LOAD_STATUS as failed and controller can be configured to retry or escalate.

19. Short checklist to migrate this workload into Snowflake-native components (practical next steps)

(You asked earlier about migrating to Snowflake — here’s the immediate plan based on exact execution flow.)

Keep HANA extraction external — Snowflake stored procs cannot connect to HANA. You need an external runtime to pull HANA data (ECS/Fargate, Airflow worker, or a Snowpark external function). So worker job logic (JDBC to HANA) will remain external.

Move orchestration into Snowflake (recommended):

Replace the controller job by a Snowflake Stored Procedure (Python) that:

Runs the SP_UPDATE_READY_TO_RUN logic (already in Snowflake),

Calls the HANA TVF? (No: HANA TVF runs in HANA — so you must call it from external service. Options: keep controller external OR build a thin external service that calls HANA and writes \_EXT, then call a Snowflake stored proc to do the MERGE).

Alternatively keep the controller in AWS but replace Glue Spark workers with an external Python job using snowflake-connector-python + write_pandas or staged files for large data.

Replace Spark write with Snowpark or staged COPY:

For very large tables, use PUT to internal stage + COPY INTO or Snowflake Snowpark for MERGE operations inside Snowflake for scale and performance instead of Spark connector.

Re-implement generate_snowflake_tblnames_ddls in Python/Snowpark to build proper DDL based on pandas or Snowpark DataFrame schema.

For reconciliation logic keep recon SQL inside Snowflake — that’s the ideal spot.

20. Final short example: exact sequence for a single table replicate run (one micro-run)

To make this concrete, here’s a condensed exact event timeline for processing one table SCHEMA.TABLE on batch group 2:

Controller determines SCHEMA.TABLE.INCR is READY_TO_RUN and assigns BATCH_GROUP=2.

Controller calls start_job(batch_group=2) which sets Glue job worker count to N and calls start_job_run for worker job glue-eas-etl-glue-read-from-hana-replicate-to-snowflake with --batch_group=2 ....

Glue worker starts, reads secrets and builds sfOptions and private keys, queries AWS_ETL_FRAMEWORK_LOAD_CTRL for BATCH_GROUP=2 and finds row describing SCHEMA.TABLE.

Worker sets LOAD_STATUS='in_progress' and calls update_load_ctrl_log_entry(... "INITIAL") which executes CALL INTEGRATION.SP_ETL_FRAMEWORK_TBL_LOADS(... initial params ...) in Snowflake.

Worker constructs HANA CALL sp_generate_dynamic_partition_queries_replication(...) and predicates_list = execute_hana_query(...) which returns e.g. ["ROW_ID >= 1 AND ROW_ID <= 1000", "ROW_ID >= 1001 AND ROW_ID <= 2000"].

Worker builds hana_select_statement and calls hana_to_snowflake_merge(...):

Spark reads HANA via JDBC with these predicates into Spark DataFrame partitions.

Spark sanitizes columns & adds insert_ts/update_ts.

generate_snowflake_tblnames_ddls returns staging & final DDLs and table names.

Spark repartitions DF according to Glue worker size; converts timestamps to string; turns into DynamicFrame.

Glue Spark Snowflake connector writes staging table and then Snowflake MERGE runs as the postactions.

hana_to_snowflake_merge returns counts & timestamps.

Worker runs recon SQL if needed using run_snowflake_query_to_df_spark to fetch delta summary.

Worker sets LOAD_STATUS='completed', populates final timestamps and calls update_load_ctrl_log_entry(... "FINAL").

Worker moves to next table or exits if none remain. Controller polls Glue status and when all batch groups are complete runs cleanup.
