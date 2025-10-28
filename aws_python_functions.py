import json
import time
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from boto3.dynamodb.conditions import Attr, Key
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
import logging
import re
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import pandas as pd
from datetime import datetime
import hdbcli.dbapi


# Set log level to INFO
logging.getLogger().setLevel(logging.INFO)


# Function to generate Snowflake private key
def generate_snowflake_ppk(certificate_string, private_key_passphrase, for_snowflake=True):
    try:
        pkb1 = bytes(certificate_string, 'utf-8')
        p_key = serialization.load_pem_private_key(
            pkb1,
            password=bytes(private_key_passphrase, 'utf-8'),
            backend=default_backend()
        )

        if for_snowflake:
            # Return RSAPrivateKey object for Python connector
            return p_key
        else:
            # Return base64 string for Spark
            pkb = p_key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption()
            )
            pkb = pkb.decode("utf-8")
            pkb = re.sub("-*(BEGIN|END) PRIVATE KEY-*\n", "", pkb).replace("\n", "")
            return pkb

    except Exception as e:
        logging.error("Exception in generate_snowflake_ppk: %s", e)
        raise Exception(f"Exception in generate_snowflake_ppk: {e}")


# Helper functions to infer data types and convert values
def infer_data_type(value):
    try:
        int(value)
        return int
    except ValueError:
        try:
            float(value)
            return float
        except ValueError:
            return str


def convert_value(value, data_type):
    if data_type == int:
        return int(value)
    elif data_type == float:
        return float(value)
    else:
        return value


# Define function to execute query in HANA
def execute_hana_query(hana_query, db_url, db_username, db_password, jdbc_driver_name):
    try:
        logging.info("HANA Query Starts " + str(datetime.now()))
        logging.info("HANA SQL: " + hana_query)
        logging.info("Starting HANA Query")

        logging.info("Data extraction query starts " + str(datetime.now()))
        hana_host_name = db_url.split("//")[1].split(":")[0]
        hana_port_num = int(db_url.split(":")[3].split("/")[0])

        conn = hdbcli.dbapi.connect(
            address=hana_host_name,
            port=hana_port_num,
            user=db_username,
            password=db_password
        )

        cursor = conn.cursor()
        cursor.execute(hana_query)
        rows = cursor.fetchall()

        logging.info("Data extraction query completed " + str(datetime.now()))
        df = pd.DataFrame(rows)
        return df

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None



# Function to extract batch details
def extract_batch_details(batch_details):
    # Split the batch_details string by comma to get individual groups
    groups = batch_details.split(',')

    # Initialize lists to store batch groups and number of workers
    batch_group = []
    no_of_workers = []

    for group in groups:
        # Split each group by '~' to get the batch and worker count
        batch, worker_count = group.split('~')
        batch_group.append(int(batch))
        no_of_workers.append(int(worker_count))

    return batch_group, no_of_workers


# Function to get workers for a specific batch group
def get_workers_for_batch(batch_group, batch_details):
    """
    Retrieves the workers for a specified batch group from batch details.

    Parameters:
        batch_group (str): The batch group to retrieve workers for.
        batch_details (list): The batch details containing batch groups and workers.

    Returns:
        list or None: A list of workers for the specified batch group, or None if not found.
    """
    try:
        batch_groups, workers = extract_batch_details(batch_details)
        print("Batch Groups:", batch_groups)
        print("Workers:", workers)

        batch_workers_dict = dict(zip(batch_groups, workers))
        print("Batch Workers Dictionary:", batch_workers_dict)

        result = batch_workers_dict.get(batch_group, None)
        print("Result for batch group", batch_group, ":", result)

        return result

    except Exception as e:
        logging.error(f"Error extracting batch details: {str(e)}")
        return None


# Function to update worker type and number of workers for AWS Glue job
def update_worker_type_no_of_worker_glue_job(job_name, worker_type, number_of_workers):
    """
    Updates the worker type and number of workers for an AWS Glue job.

    Parameters:
        job_name (str): The name of the Glue job to update.
        worker_type (str): The new type of Glue worker to use (e.g., 'G.2X').
        number_of_workers (int): The new number of workers to assign to the Glue job.

    Returns:
        dict: A response from the AWS Glue service that includes the status of the update operation.

    Raises:
        Exception: If the job retrieval fails or if the AWS Glue client encounters an error.
    """

    # Create a Glue client
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
            'WorkerType': worker_type,
            'NumberOfWorkers': number_of_workers
        }

        #Include optional settings if present crefully omitting maxcapacity
        optional_fields = ['MaxRetries', 'Timeout', 'Connections', 'SecurityConfiguration', 'GlueVersion']
        for field in optional_fields:
            if field in job_definition:
                job_update_params[field] = job_definition[field]

        # Update the Glue job with the new parameters
        update_response = glue.update_job(JobName=job_name, JobUpdate=job_update_params)
        logging.info(f"Updated Glue job '{job_name}' with WorkerType='{worker_type}', NumberOfWorkers={number_of_workers}")

        return update_response

    except ClientError as e:
        raise Exception(f"Failed to update Glue job '{job_name}': {str(e)}")
    except BotoCoreError as e:
        raise Exception(f"An error occurred with AWS SDK '{job_name}': {str(e)}")


def trigger_glue_jobs(job_name):
    glue_client = boto3.client('glue')
    job_ids = []

    response = glue_client.start_job_run(
        JobName=job_name
    )

    job_ids.append(response['JobRunId'])
    print(f"Triggered Glue job {response['JobRunId']}")

    return job_ids


def check_jobs_status(job_name, job_ids):
    time.sleep(20)  # Initial wait time before first check
    glue_client = boto3.client('glue')

    for job_id in job_ids:
        status = glue_client.get_job_run(JobName=job_name, RunId=job_id)['JobRun']['JobRunState']

        while status not in ("SUCCEEDED", "FAILED", "STOPPED"):
            print(f"Job {job_id} is {status}. Checking again in 10 seconds.")
            time.sleep(10)  # Wait before checking again
            status = glue_client.get_job_run(JobName=job_name, RunId=job_id)['JobRun']['JobRunState']

        if status != "SUCCEEDED":
            raise Exception(f"Job {job_id} has completed with status {status}.")
        else:
            print(f"Job {job_id} has completed with status {status}.")


def fetch_snowflake_config(sfOptions):
    snowflake_query_config = "SELECT * FROM INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_CONFIG;"
    df_config = run_snowflake_query_to_df(sfOptions, snowflake_query_config)
    print("Config table results")
    print(df_config.head())
    return df_config


def run_aws_glue_jobs_parallel_controlled_no_of_execution(
        job_name, batch_groups, max_concurrent_jobs, worker_type, batch_details,
        partition_size, account_env_number, job_run_id, sns_topic_arn, execute_recon_full,
        secret_name_hana, secret_name_snowflake, sfOptions):
    """
    Generic function to run AWS Glue jobs in parallel with controlled concurrency.
    Author: Vinay Kumar
    :param glue_client: Boto3 Glue client
    :param job_name: Name of the AWS Glue job
    :param batch_groups: List of batch groups to process
    :param max_concurrent_jobs: Max Glue jobs to run at the same time
    :return: List of completed jobs with status
    """
    active_jobs = []
    completed_jobs = []

    def start_job(batch_group):
        """Starts a Glue job for a given batch group and returns the JobRunId."""
        try:
            logging.info(f"Going to start glue job for batch group {batch_group}")
            glue_client = boto3.client('glue')
            no_of_workers = get_workers_for_batch(batch_group, batch_details)
            logging.info(
                f"For batch group: {batch_group}, partition size: {partition_size}, "
                f"number of workers to use is {no_of_workers}"
            )
            update_worker_type_no_of_worker_glue_job(job_name, worker_type, no_of_workers)
            response = glue_client.start_job_run(JobName=job_name
                , Arguments={
                    '--batch_group': str(batch_group),
                    '--partition_size': str(partition_size),
                    '--main_job_run_id': str(job_run_id),
                    '--secret_name_hana': str(secret_name_hana),
                    '--secret_name_snowflake': str(secret_name_snowflake),
                    '--secret_name_snowflake_keypair': str(secret_name_snowflake)
                }
                                                 )
            job_id = response['JobRunId']
            logging.info(f"Started Glue job {job_id} for batch group {batch_group}")
            return job_id
        except Exception as e:
            logging.error(f"Error starting job for batch group {batch_group}: {str(e)}")
            return None
        
    def check_job_status(job_id, batch_group, sfOptions):
        """Checks the status of a running Glue job and updates completed_jobs."""
        try:
            glue_client = boto3.client('glue')
            status = glue_client.get_job_run(JobName=job_name, RunId=job_id)['JobRun']['JobRunState']

            if status in ('SUCCEEDED', 'FAILED', 'STOPPED'):
                logging.info(f"Job {job_id} completed with status {status}.")
                completed_jobs.append((batch_group, job_id, status))
                return status
            else:
                logging.info(f"Job {job_id} is still running.")
                return None
        except Exception as e:
            logging.error(f"Failed to fetch status for job {job_id}: {e}")
            return None


    # -----------------------------
    # Main loop to handle job execution
    # -----------------------------
    logging.info("Before starting batch group loop.")
    start_run_time = time.time()

    for batch_group in batch_groups:
        logging.info("After starting batch group loop.")

        # Check if concurrent job limit reached
        while len(active_jobs) >= max_concurrent_jobs:
            logging.info(f"Total active jobs: {len(active_jobs)}")
            # Check job statuses and update list
            active_jobs[:] = [
                (jid, bg)
                for jid, bg in active_jobs
                if check_job_status(jid, bg, sfOptions) is None
            ]
            time.sleep(20)  # Avoid excessive API calls

            current_time = time.time()
            if (current_time - start_run_time) / 60 >= 20:
                df_config = fetch_snowflake_config(sfOptions)
                logging.info(df_config.head())
                max_concurrent_jobs = int(df_config.loc[0, 'MAX_CONCURRENT_JOBS'])
                max_concurrent_jobs_full = int(df_config.loc[0, 'MAX_CONCURRENT_JOBS_FULL'])

                if execute_recon_full:
                    max_concurrent_jobs = int(df_config.loc[0, 'MAX_CONCURRENT_JOBS_FULL'])
                else:
                    max_concurrent_jobs = int(df_config.loc[0, 'MAX_CONCURRENT_JOBS'])
                logging.info(f"max_concurrent_jobs = {max_concurrent_jobs}")
                log_running_alert_threshold_minutes = int(
                    df_config.loc[0, 'LOG_RUNNING_ALERT_THRESHOLD_MINUTES']
                )
                logging.info(f"log_running_alert_threshold_minutes = {str(log_running_alert_threshold_minutes)}")
                check_long_running_table_job(
                    account_env_number, sns_topic_arn, job_name,
                    sfOptions, log_running_alert_threshold_minutes
                )
                start_run_time = current_time

        # Start new job if capacity allows
        job_id = start_job(batch_group)
        logging.info(f"Job id = {job_id}")
        if job_id:
            active_jobs.append((job_id, batch_group))

 

    # Wait for remaining jobs to finish
    while active_jobs:
        active_jobs[:] = [(jid, bg) for jid, bg in active_jobs if check_job_status(jid, bg, sfOptions) is None]
        time.sleep(20)
        current_time = time.time()
        if (current_time - start_run_time) / 60 >= 20:
            df_config = fetch_snowflake_config(sfOptions)
            logging.info(df_config.head())
            max_concurrent_jobs = int(df_config.loc[0, 'MAX_CONCURRENT_JOBS'])
            logging.info("max_concurrent_jobs = " + str(max_concurrent_jobs))
            log_running_alert_threshold_minutes = int(df_config.loc[0, 'LOG_RUNNING_ALERT_THRESHOLD_MINUTES'])
            logging.info("log_running_alert_threshold_minutes = " + str(log_running_alert_threshold_minutes))
            check_long_running_table_job(account_env_number, sns_topic_arn, job_name, sfOptions, log_running_alert_threshold_minutes)
            start_run_time = current_time

    logging.info("All Glue jobs completed.")
    return completed_jobs


# --------------------------------------------------
# Function to send email
# --------------------------------------------------
def send_email(sns_topic_arn, email_subject, email_body):
    """Send email notification using AWS SNS."""
    logging.info("aws_functions.send_email starts")
    try:
        logging.info("sns topic arn: " + sns_topic_arn)
        logging.info("email subject: " + email_subject)
        logging.info("email body: " + email_body)

        sns_client = boto3.client('sns')
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject=email_subject,
            Message=email_body
        )
        logging.info("The email sent successfully")
    except Exception as e:
        logging.error(f"Exception in aws_functions.send_email. Exception: {e}")


# --------------------------------------------------
# Function to execute query in Snowflake
# --------------------------------------------------
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

        cursor = conn.cursor()
        logging.info(f"Executing query: {query}")
        cursor.execute(query)

        if query.strip().lower().startswith("select"):
            df = cursor.fetch_pandas_all()
            print("Print top 2 rows")
            print(df.head(2))
            return df
        else:
            affected_rows = cursor.rowcount
            logging.info(f"Query executed successfully. Rows affected: {affected_rows}")
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


#----------------------------------
# Function to load HANA config to Snowflake
# --------------------------------------------------
def load_hana_config_to_snowflake(df, sf_options):
    """
    Truncates a Snowflake table and loads a DataFrame using private key authentication.

    Parameters:
        df (pd.DataFrame): The DataFrame to load.
        sf_options (dict): Dictionary with Snowflake connection options. Required keys:
            - sfUser
            - sfAccount
            - sfWarehouse
            - sfDatabase
            - sfSchema
            - pem_private_key
            - table
            - role (optional)
    """
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=sf_options['sfUser'],
            authenticator='SNOWFLAKE_JWT',
            private_key=sf_options['pem_private_key'],
            account=sf_options['sfAccount'],
            warehouse=sf_options['sfWarehouse'],
            database=sf_options['sfDatabase'],
            schema="INTEGRATION"
            table = "AWS_ETL_FRAMEWORK_LOAD_CTRL_EXT"
        )

        try:
            # Truncate the table
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {sf_options['sfDatabase']}.INTEGRATION.AWS_ETL_FRAMEWORK_LOAD_CTRL_EXT")

            # Load DataFrame
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name="AWS_ETL_FRAMEWORK_LOAD_CTRL_EXT",
                schema="INTEGRATION",
                database=sf_options['sfDatabase']
            )

            if not success:
                raise RuntimeError("DataFrame upload to Snowflake failed.")

            print(f"Upload successful: {success}, Rows inserted: {nrows}")
            return success, nrows

        finally:
            conn.close()

    except Exception as e:
        raise RuntimeError(f"Failed to load data into Snowflake: {str(e)}") from e


# --------------------------------------------------
# Function to get running AWS Glue job IDs by batch group
# --------------------------------------------------
def get_running_job_ids_by_batch_group(job_name, batch_group):
    """
    Retrieves running job run IDs for a specified AWS Glue job filtered by batch group.

    Parameters:
        job_name (str): The name of the Glue job.
        batch_group (str): The batch group to filter the job runs.

    Returns:
        list: A list of dictionaries containing JobRunId and start time for each matching job run.
    """
    # Initialize the Glue client
    glue_client = boto3.client('glue')

    # Get all job runs for the specified job
    try:
        response = glue_client.get_job_runs(JobName=job_name)
    except Exception as e:
        print(f"Error retrieving job runs: {e}")
        return None

    # Filter and process job runs
    for job_run in response['JobRuns']:
        if job_run['JobRunState'] == 'RUNNING':
            # Access the batch_group argument from Glue job parameters
            current_batch_group = job_run['Arguments'].get('--batch_group', None)
            if current_batch_group == batch_group:
                running_jobs=job_run['Id']
                break

    return running_jobs
# --------------------------------------------------
# Function to check for long-running table jobs from HANA to AWS Silver
# --------------------------------------------------
def check_long_running_table_job(account_env_number, sns_topic_arn, job_name, sfOptions, duration_threshold=30):
    """
    Checks for long-running table loads from HANA to AWS Silver and sends an alert
    if any table exceeds the specified threshold duration.

    Parameters:
        account_env_number (str): AWS account environment number.
        sns_topic_arn (str): SNS topic ARN for sending alert emails.
        job_name (str): Name of the job.
        sfOptions (dict): Snowflake connection configuration options.
        duration_threshold (int, optional): Threshold duration in minutes above which an alert will be sent.
                                            Default is 30 minutes.
    """

    # Log the start of the function
    logging.info(f"Checking for long-running operations with a threshold of {duration_threshold} minutes.")

    # Define the Snowflake query with parameterized duration threshold
    checking_long_running_tables_query = f"""
        SELECT SOURCE1_SCHEMA_NAME,
               SOURCE1_OBJECT_NAME,
               DATEDIFF('MINUTE', (LOAD_START_TIME), (COALESCE(LOAD_END_TIME, CURRENT_TIMESTAMP))) AS DURATION_MINUTES
        FROM "INTEGRATION"."AWS_ETL_FRAMEWORK_LOAD_CTRL"
        WHERE LOAD_TYPE='INCR'
          AND LOAD_STATUS='in progress'
          AND LOAD_ENABLE_FLAG=TRUE
          AND DATEDIFF('MINUTE', (LOAD_START_TIME), (COALESCE(LOAD_END_TIME, CURRENT_TIMESTAMP))) >= {duration_threshold}
    """

    # Log execution of the query
    logging.info("Executing Snowflake query to check for long-running operations.")
    logging.info(checking_long_running_tables_query)

    # Execute the query and store results in a dataframe
    df_time_alert = run_snowflake_query_to_df(sfOptions, checking_long_running_tables_query)

    # Check if there are any long-running operations
    if df_time_alert.shape[0] > 0:
        # Log the detection of long-running operations
        logging.info("Long-running operations detected. Preparing alert message.")

        # Format alert messages for each long-running operation
        alert_messages = "\n".join(
            [f"Schema: {row['SOURCE1_SCHEMA_NAME']}, Table: {row['SOURCE1_OBJECT_NAME']}, Duration (min): {row['DURATION_MINUTES']}"
             for _, row in df_time_alert.iterrows()]
        )

        email_subject = f"[{account_env_number}] Alert: Long Running Tables Detected - {job_name}"
        email_body = (
            f"The following tables are running longer than expected "
            f"(Threshold: {duration_threshold} minutes):\n\n{alert_messages}\n\n"
            f"Please check the progress of the table data load and ensure the job is not stuck."
        )

        # Send the alert email
        send_email(sns_topic_arn, email_subject, email_body)
        logging.info("Alert email sent for long-running operations.")
    else:
        # Log when no long-running operations are found
        logging.info(f"No long-running tables found exceeding the threshold of {duration_threshold} minutes.")