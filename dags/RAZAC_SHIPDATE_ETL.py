"""RAZAC SHIPDATE ETL DAG Definition Module."""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

from domain.enumerations.environment import Environment
from domain.enumerations.profile import Profile
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
    "RAZAC_SHIPDATE_ETL",
    default_args=default_args,
    description="Fetch shipdate data from RAZAC S3 and delivers it on SKF DW",
    schedule_interval=None,
) as dag:

    dag_start_control = BashOperator(bash_command="echo starting controls", task_id="dag_start_control", dag=dag)

    s3_to_stage = BashOperator(bash_command="echo s3 to stage", task_id="s3_to_stage", dag=dag)

    stage_to_dw = BashOperator(bash_command="echo stage to dw", task_id="stage_to_dw", dag=dag)

    dag_end_success_control = BashOperator(
        bash_command="echo etl success control",
        task_id="dag_end_success_control",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )

    dag_end_failure_control = BashOperator(
        bash_command="echo etl failure control", task_id="dag_end_failure_control", trigger_rule=TriggerRule.ONE_FAILED
    )

    dag_start_control >> s3_to_stage >> stage_to_dw >> dag_end_success_control
    list(dag.tasks) >> dag_end_failure_control
