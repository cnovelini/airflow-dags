from sqlalchemy.sql import Executable
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowFailException
from coh.tables.cuprod.struct import CUPROD_SCHEMA
from datetime import timedelta
from airflow.models import Variable
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import text
from as400.as400 import AS400Connector
import io
import boto3
from datetime import datetime
import os

#Global variables
s3_client = boto3.client(
    "s3",
    aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY")
)

# Connect into staging
print("Connecting to STAGE...")
try:
    stage_developer_hostname = Variable.get("stage_developer_hostname")
    stage_developer_username = Variable.get("stage_developer_username")
    stage_developer_password = Variable.get("stage_developer_password")
    stage_engine = create_engine(f"postgresql+psycopg2://{stage_developer_username}:{stage_developer_password}@{stage_developer_hostname}:5432/stage_developer", pool_size=5, pool_recycle=3600)
except Exception as e:
    print(e)
    raise AirflowException("Failed to connect to database engine")

# TODO - Add timezone to timestamp (missing add pytz to image that execute DAG)
timestamp_str = str(datetime.now()).split(" ")
timestamp = f"{timestamp_str[0]}_{timestamp_str[1]}"

# Defining main DAG's config
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 10),
    'email': ['novelini@c9apps.com.br'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Dag declaration
dag = DAG(
    'CUPROD_ETL',
    default_args=default_args,
    description='Fetch data from CUPROD - AS400 and dumps into S3 -> STAGE',
    schedule_interval=None,
)


def test_env_var():
    testing_var = 'FAIL'
    try:
        testing_var = os.getenv('testing-var')
    except Exception as e:
        print(e)
        print("Testing var not found")
    
    print(testing_var)

def df_cleanup(df):
    """Clean up input dataframe

    Args:
        df (data fram): Pandas Data Frame

    Returns:
        df (data fram): Pandas Data Frame
    """
    # Remove extra space char from all dataframe fields
    try:
        df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    except Exception as e:
        print(e)
        raise AirflowException("Failed to transform data")
    return df


def create_sql():
    """CREATE or DROP and CREATE CUPROD schema into Staging

    Raises:
        AirflowException: _description_
        AirflowException: _description_
    """

    #Create STAGE connection
    try:
        conn = stage_engine.connect()
    except Exception as e:
        print(e)
        raise AirflowException("Failed to connect to STAGE")

    # Drop CUPROD STAGE if it exists
    print('Dropping CUPROD - STAGE...')
    try:
        conn.execute(text("DROP TABLE IF EXISTS cuprod_stage_dev"))
    except Exception as e:
        print(e)
        conn.close()
        raise AirflowException("Failed to drop CUPROD STAGE")

    # Execute SQL Command
    print("Creating CUPROD - STAGE schema...")
    try:
        query = text(CUPROD_SCHEMA)
        result = conn.execute(query)
    except Exception as e:
        print(e)
        conn.close()
        raise AirflowException("Failed to create schema for CUPROD")

    # Close Connection
    conn.close()


def as400_to_S3():
    """Query CUPROD on AS400 and save's it into S3

    Raises:
        AirflowException: _description_
    """

    print("Connecting to AS400")
    try:
        as400 = AS400Connector()
        query = "SELECT * FROM SCFBR.CUPROD LIMIT 100"
        user_identification = "Airflow - CUPROD ETL"
        print("Executing AS400 Query...")
        executed_with_success, response = as400.query_as_df(query, user_identification)
    except Exception as e:
        print(e)
        raise AirflowException("Failed to fetch data from AS400")

    if not executed_with_success:
        raise AirflowException("Failed to query AS400")

    print(f"Number of entries: {len(response)}")

    # Connect to S3 Bucket
    AWS_S3_BUCKET = "coh-dump"
    print("Saving into S3...")
    try:
        with io.StringIO() as csv_buffer:
            response.to_csv(csv_buffer, index=False)

            result = s3_client.put_object(
                Bucket=AWS_S3_BUCKET, Key=f"files/CUPROD_DUMP.csv", Body=csv_buffer.getvalue()
            )
    except Exception as e:
        print(e)
        raise AirflowException("Failed to insert into S3")


def S3_cleanup():
    """Cleanup S3 Data
    """

    # S3 Bucket Name
    AWS_S3_BUCKET = "coh-dump"

    #Retreive file from S3
    print('Retrieving Dump File...')
    try:
        s3_dump_file = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=f"files/CUPROD_DUMP.csv")
    except Exception as e:
        print(e)
        raise AirflowException("Failed retrieve S3 file") 

    # Transform CSV into Pandas Dataframe
    df = pd.read_csv(s3_dump_file.get("Body"))

    # Cleanup data
    print('Applying cleanup...')
    df = df_cleanup(df)

    # Save into S3
    print('Saving into S3')
    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)

        result = s3_client.put_object(
            Bucket=AWS_S3_BUCKET, Key=f"files/CUPROD_CLEAN.csv", Body=csv_buffer.getvalue()
        )


def s3_to_stage():
    """ETL from S3 to Stage

    Raises:
        AirflowException: _description_
        AirflowException: _description_
    """

    #Create STAGE connection
    try:
        conn = stage_engine.connect()
    except Exception as e:
        print(e)
        raise AirflowException("Failed to connect to STAGE")
    
    # S3 Bucket Name
    AWS_S3_BUCKET = "coh-dump"

    #Retreive file from S3
    print('Retrieving Dump File...')
    s3_dump_file = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=f"files/CUPROD_CLEAN.csv")

    # Transform CSV into Pandas Dataframe
    df = pd.read_csv(s3_dump_file.get("Body"))

    # Writing file into STAGE
    print('Writing to stage...')
    try:
        df.to_sql('cuprod_stage_dev',stage_engine, if_exists = 'replace')
    except Exception as e:
        print(e)
        conn.close()
        raise AirflowException("Failed to insert data into STAGE")
    
    conn.close()


# Operators declaration

test_env_var
test_env_var = PythonOperator(
    task_id='test_env_var',
    python_callable=test_env_var,
    dag=dag,
)

s3_dump = PythonOperator(
    task_id='as400_to_S3',
    python_callable=as400_to_S3,
    dag=dag,
)

s3_clean = PythonOperator(
    task_id='S3_cleanup',
    python_callable=S3_cleanup,
    dag=dag,
)

to_stage = PythonOperator(
    task_id='S3_to_stage',
    python_callable=s3_to_stage,
    dag=dag,
)

sql_structure = PythonOperator(
    task_id='handle_schema_data_drop',
    python_callable=create_sql,
    dag=dag,
)

# DAG sequence
# sql_structure >> s3_dump >> s3_clean >> to_stage
test_env_var