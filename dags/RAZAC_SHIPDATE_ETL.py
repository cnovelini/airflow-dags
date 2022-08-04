"""RAZAC SHIPDATE ETL DAG Definition Module."""
import logging
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

from domain.data.razac.tables.shipdate.struct import SHIPDATE_TABLE_STRUCTURE
from domain.enumerations.database_insertion_method import DbInsertionMethod
from domain.enumerations.environment import Environment
from domain.enumerations.profile import Profile
from helpers.dataframe_transformation_executioner import DataFrameTransformationExecutioner
from helpers.profile_manager import ProfileManager
from helpers.skf_controller import SkfController
from infrastructure.connections.postgres_connector import PostgresConnector
from infrastructure.connections.s3_connector import S3Connector
from operators.control_operator import ControlOperator
from operators.razac_shipdate_data_backup_operator import RazacShipdateDataBackupOperator
from operators.razac_shipdate_file_consumer_operator import RazacShipdateFileConsumerOperator
from operators.sql_table_reconstruct_operator import SqlTableReconstructOperator
from operators.sql_to_sql_operator import SqlToSqlOperator


# Global variables (action executioners)
profile = ProfileManager.get_profile(Profile.PROFILE_001, Environment.STAGE)
logger = logging.getLogger(profile.get("logger_name"))
s3 = S3Connector(
    logger,
    access_key=profile.get("IMPORT_FINANCIAL_RAZAC_DATA_S3_AWS_ACCESS_KEY_ID"),
    secret_access_key=profile.get("IMPORT_FINANCIAL_RAZAC_DATA_S3_AWS_SECRET_ACCESS_KEY"),
    default_bucket=profile.get("RAZAC_SHIPDATE_S3_BUCKET"),
)
stage_db = PostgresConnector(
    logger,
    profile.generate_sqlalchemy_connection_string(
        user_key="STAGE_DB_USERNAME",
        password_key="STAGE_DB_PASSWORD",  # pragma: allowlist secret
        host_key="STAGE_DB_HOST_READER",
        schema_key="STAGE_DB_NAME",
    ),
    internal_control_columns=["file", "line"],
    current_user=profile.get("STAGE_DB_USERNAME"),
)
dw = PostgresConnector(
    logger,
    profile.generate_sqlalchemy_connection_string(
        user_key="ODW_AIRFLOW_USERNAME",
        password_key="ODW_AIRFLOW_PASSWORD",  # pragma: allowlist secret
        host_key="ODW_DB_HOST",
        schema_key="ODW_DB_NAME",
    ),
    current_user=profile.get("ODW_AIRFLOW_USERNAME"),
)
skf_controller = SkfController(
    logger,
    stage_db,
    s3,
    xcom_key="razac_shipdate_shared_data",
    error_log_folder=profile.get("RAZAC_SHIPDATE_S3_ERR_FOLDER"),
)

# Defining main DAG's config
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now(),
    "email": [profile.get("MANAGER_EMAIL")],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "provide_context": False,
    "retry_delay": timedelta(minutes=5),
}

# Dag declaration
with DAG(
    "RAZAC_SHIPDATE_ETL",
    default_args=default_args,
    description="Fetch shipdate data from RAZAC S3 and delivers it on SKF DW",
    schedule_interval=None,
) as dag:

    dag_start_control = ControlOperator(
        logger=logger,
        controller=skf_controller,
        action="dag_start",
        last_task=None,
        task_id="dag_start_control",
        dag=dag,
    )

    stage_table_reconstruction = SqlTableReconstructOperator(
        logger=logger,
        controller=skf_controller,
        database_client=stage_db,
        table_name=profile.get("RAZAC_SHIPDATE_STAGE_TABLE_NAME"),
        table_structure=SHIPDATE_TABLE_STRUCTURE,
        last_task="dag_start_control",
        task_id="stage_table_reconstruction",
        dag=dag,
    )

    s3_to_stage = RazacShipdateFileConsumerOperator(
        logger=logger,
        controller=skf_controller,
        s3=s3,
        database_client=stage_db,
        target_table_name=profile.get("RAZAC_SHIPDATE_STAGE_TABLE_NAME"),
        encoding=profile.get("RAZAC_SHIPDATE_CSV_ENCODING"),
        delimiter=profile.get("RAZAC_SHIPDATE_CSV_DELIMITER"),
        dataframe_transformer=DataFrameTransformationExecutioner(logger, target_operations=["razac_shipdate"]),
        last_task="stage_table_reconstruction",
        target_folder=profile.get("RAZAC_SHIPDATE_S3_IN_FOLDER"),
        task_id="s3_to_stage",
        dag=dag,
    )

    dw_table_reconstruction = SqlTableReconstructOperator(
        logger=logger,
        controller=skf_controller,
        database_client=dw,
        table_name=profile.get("RAZAC_SHIPDATE_ODW_TABLE_NAME"),
        table_structure=SHIPDATE_TABLE_STRUCTURE,
        last_task="s3_to_stage",
        task_id="dw_table_reconstruction",
        dag=dag,
    )

    stage_to_dw = SqlToSqlOperator(
        logger=logger,
        controller=skf_controller,
        origin_client=stage_db,
        destination_client=dw,
        origin_table_name=profile.get("RAZAC_SHIPDATE_STAGE_TABLE_NAME"),
        target_table_name=profile.get("RAZAC_SHIPDATE_ODW_TABLE_NAME"),
        insertion_method=DbInsertionMethod.FULL_PD_TO_SQL,
        last_task="dw_table_reconstruction",
        task_id="stage_to_dw",
        dag=dag,
    )

    backup_s3_files = RazacShipdateDataBackupOperator(
        logger=logger,
        controller=skf_controller,
        s3=s3,
        origin_folder=profile.get("RAZAC_SHIPDATE_S3_IN_FOLDER"),
        destination_folder=profile.get("RAZAC_SHIPDATE_S3_BACKUP_FOLDER"),
        last_task="stage_to_dw",
        file_info_task="s3_to_stage",
        task_id="backup_s3_files",
        dag=dag,
    )

    dag_end_success_control = ControlOperator(
        logger=logger,
        controller=skf_controller,
        action="dag_end_success_control",
        last_task="backup_s3_files",
        task_id="dag_end_success_control",
        dag=dag,
    )

    dag_end_failure_control = ControlOperator(
        logger=logger,
        controller=skf_controller,
        action="dag_end_failure_control",
        last_task=None,
        task_id="dag_end_failure_control",
        dag=dag,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    dag_start_control >> stage_table_reconstruction >> s3_to_stage
    s3_to_stage >> dw_table_reconstruction >> stage_to_dw >> backup_s3_files >> dag_end_success_control

    dag_start_control >> dag_end_failure_control
    stage_table_reconstruction >> dag_end_failure_control
    s3_to_stage >> dag_end_failure_control
    dw_table_reconstruction >> dag_end_failure_control
    stage_to_dw >> dag_end_failure_control
    backup_s3_files >> dag_end_failure_control
    dag_end_success_control >> dag_end_failure_control
