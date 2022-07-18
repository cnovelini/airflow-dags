"""CUPROD AS400 ETL DAG Definition Module."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from domain.constants.queries.as400_queries import SELECT_FROM_CUPROD_LIMIT_100
from domain.data.coh.tables.cuprod.struct import CUPROD_COLUMNS, CUPROD_TABLE_STRUCTURE
from domain.enumerations.environment import Environment
from domain.enumerations.profile import Profile
from helpers.dataframe_transformation_executioner import DataFrameTransformationExecutioner
from helpers.file_path_manager import FilePathManager
from helpers.profile_manager import ProfileManager
from infrastructure.connections.as400_connector import AS400Connector
from infrastructure.connections.postgres_connector import PostgresConnector
from infrastructure.connections.s3_connector import S3Connector
from infrastructure.logging.airflow_logger import AirflowLogger

# Global variables (action executioners)
profile = ProfileManager.get_profile(Profile.PROFILE_001, Environment.STAGE)
logger = AirflowLogger(profile)
as_400 = AS400Connector(profile, logger)
s3 = S3Connector(profile, logger)
postgres = PostgresConnector(profile, logger)
path_manager = FilePathManager(profile, logger)

# Defining main DAG's config
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now().date(),
    "email": [profile.get("MANAGER_EMAIL")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


def handle_schema_data_drop():
    """CREATE or DROP and CREATE CUPROD schema."""
    logger.info("Starting 'handle_schema_data_drop' task")

    # Opening database session
    with postgres.session_scope() as session:

        # Dropping table
        postgres.drop_table(session, profile.get("CUPROD_TABLE"))

        # Recreating table
        postgres.create_table(session, profile.get("CUPROD_TABLE"), CUPROD_TABLE_STRUCTURE)


def as400_to_s3():
    """Query CUPROD on AS400 and save's it into S3."""
    logger.info("Starting 'as400_to_s3' task")

    # Recovering information from AS400
    response = as_400.query_as_df(SELECT_FROM_CUPROD_LIMIT_100.format(CUPROD_COLUMNS))

    # Saving information on S3
    s3.save_as_csv(response, path_manager.build_new_path(profile.get("CUPROD_DUMP_FILE_PATH")))


def s3_cleanup():
    """Cleanup S3 Data."""
    logger.info("Starting 's3_cleanup' task")

    # Recovering table from S3
    table_df = s3.read_file_as_df(path_manager.recover_last_path(profile.get("CUPROD_DUMP_FILE_PATH")))

    # Executing cleanup routines
    clean_table_df = DataFrameTransformationExecutioner(target_operations=["string"]).transform(table_df)

    # Saving clean table back on S3
    s3.save_as_csv(clean_table_df, path_manager.build_new_path(profile.get("CUPROD_CLEAN_FILE_PATH")))


def s3_to_sql_database():
    """Transfers table from S3 to SQL database."""
    logger.info("Starting 's3_to_sql_database' task")

    # Recovering table from S3
    clean_table_df = s3.read_file_as_df(path_manager.recover_last_path(profile.get("CUPROD_DUMP_FILE_PATH")))

    # Inserting table on Postgres database
    with postgres.session_scope() as session:
        postgres.insert(session, clean_table_df, table=profile.get("CUPROD_TABLE"))


# Dag declaration
with DAG(
    "CUPROD_ETL",
    default_args=default_args,
    description="Fetch data from CUPROD - AS400 and dumps into S3",
    schedule_interval=None,
) as dag:

    sql_structure = PythonOperator(
        task_id="handle_schema_data_drop",
        python_callable=handle_schema_data_drop,
        dag=dag,
    )

    s3_dump = PythonOperator(
        task_id="as400_to_s3",
        python_callable=as400_to_s3,
        dag=dag,
    )

    s3_clean = PythonOperator(
        task_id="s3_cleanup",
        python_callable=s3_cleanup,
        dag=dag,
    )

    to_stage = PythonOperator(
        task_id="S3_to_sql_database",
        python_callable=s3_to_sql_database,
        dag=dag,
    )

    sql_structure >> s3_dump >> s3_clean >> to_stage
