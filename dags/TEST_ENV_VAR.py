from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import timedelta
import os
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 5, 10),
    "email": ["novelini@c9apps.com.br"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Dag declaration
dag = DAG(
    "TEST_ENV_VAR",
    default_args=default_args,
    description="Check if necessary variables are being loaded from the env",
    schedule_interval=None,
)


def test_env_var():
    variables = [
        "STAGE_DB_HOST_READER",
        "STAGE_DB_HOST_WRITER",
        "STAGE_DB_NAME",
        "ODW_DB_HOST",
        "ODW_DB_NAME",
        "IMPORT_FINANCIAL_RAZAC_DATA_S3_AWS_ACCESS_KEY_ID",
        "IMPORT_FINANCIAL_RAZAC_DATA_S3_AWS_SECRET_ACCESS_KEY",
        "STAGE_DB_USERNAME",
        "STAGE_DB_PASSWORD",
        "ODW_AIRFLOW_PASSWORD",
        "ODW_AIRFLOW_USERNAME",
    ]

    for variable in variables:
        env_var = os.getenv(variable)
        if not env_var or env_var is None:
            error = f"Failed to import variable - {variable}"
            print(error)
            raise AirflowFailException(error)


def force_fail_env_var():
    variables = ["NON_EXISTENT"]

    for variable in variables:
        env_var = os.getenv(variable)
        if not env_var or env_var is None:
            error = f"Failed to import variable - {variable}"
            print(error)
            raise AirflowFailException(error)


test_env_var = PythonOperator(
    task_id="test_env_var",
    python_callable=test_env_var,
    dag=dag,
)

force_fail_env_var = PythonOperator(
    task_id="force_fail_env_var",
    python_callable=force_fail_env_var,
    dag=dag,
)

[test_env_var, force_fail_env_var]
