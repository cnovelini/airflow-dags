from datetime import datetime, timedelta
from airflow import DAG
import pytest


@pytest.helpers.register
def run_operator(dag, operator):
    dag.clear()
    operator.run(
        start_date=dag.default_args["start_date"],
        end_date=dag.default_args["start_date"],
    )


@pytest.fixture
def test_dag():
    return DAG(
        "test_dag",
        default_args={"owner": "airflow", "start_date": datetime.now()},
        schedule_interval=timedelta(days=1),
    )
