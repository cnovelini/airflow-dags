"""CUPROD AS400 ETL DAG Definition Module."""
from airflow import DAG
from datetime import datetime, timedelta

from domain.constants.queries.as400_queries import SELECT_FROM_CUPROD
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
from operators.as400_to_s3_operator import AS400ToS3Operator
from operators.s3_dataframe_cleanup_operator import S3DataframeCleanupOperator
from operators.s3_to_sql_database_operator import S3ToSqlDatabaseOperator
from operators.uncontrolled_sql_table_reconstruct_operator import UncontrolledSqlTableReconstructOperator


# Global variables (action executioners)
profile = ProfileManager.get_profile(Profile.PROFILE_001, Environment.STAGE)
logger = AirflowLogger(profile)
as_400 = AS400Connector(profile, logger)
s3 = S3Connector(
    logger,
    access_key=profile.get("AWS_ACCESS_KEY_ID"),
    secret_access_key=profile.get("AWS_SECRET_ACCESS_KEY"),
    default_bucket=profile.get("COH_DUMP_BUCKET"),
)
postgres = PostgresConnector(
    logger,
    profile.generate_sqlalchemy_connection_string(),
    current_user=profile.get_user(),
)
path_manager = FilePathManager(profile, logger)

# Defining main DAG's config
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now(),
    "email": [profile.get("MANAGER_EMAIL")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "provide_context": False,
    "retry_delay": timedelta(minutes=5),
}

# Dag declaration
with DAG(
    "CUPROD_ETL",
    default_args=default_args,
    description="Fetch data from CUPROD - AS400 and dumps into S3",
    schedule_interval=None,
) as dag:

    sql_structure = UncontrolledSqlTableReconstructOperator(
        logger=logger,
        database_client=postgres,
        table_name=profile.get("CUPROD_TABLE"),
        table_structure=CUPROD_TABLE_STRUCTURE,
        task_id="handle_schema_data_drop",
        dag=dag,
    )

    s3_dump = AS400ToS3Operator(
        logger=logger,
        as400=as_400,
        s3=s3,
        query=SELECT_FROM_CUPROD.format(CUPROD_COLUMNS),
        target_s3_path=path_manager.build_new_path(profile.get("CUPROD_DUMP_FILE")),
        task_id="as400_to_s3",
        dag=dag,
    )

    s3_clean = S3DataframeCleanupOperator(
        logger=logger,
        s3=s3,
        dataframe_cleaner=DataFrameTransformationExecutioner(logger, target_operations=["string"]),
        original_file_path=path_manager.recover_last_path(profile.get("CUPROD_DUMP_FILE")),
        clean_file_path=path_manager.build_new_path(profile.get("CUPROD_CLEAN_FILE")),
        task_id="s3_cleanup",
        dag=dag,
    )

    to_sql = S3ToSqlDatabaseOperator(
        logger=logger,
        s3=s3,
        database_client=postgres,
        s3_file_path=path_manager.recover_last_path(profile.get("CUPROD_CLEAN_FILE")),
        target_table_name=profile.get("CUPROD_TABLE"),
        task_id="s3_to_sql_database",
        dag=dag,
    )

    sql_structure >> s3_dump >> s3_clean >> to_sql
