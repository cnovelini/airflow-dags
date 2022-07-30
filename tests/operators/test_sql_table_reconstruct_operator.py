from airflow import DAG

from pytest import mark
import pytest
from pytest_mock import MockerFixture

from operators.sql_table_reconstruct_operator import SqlTableReconstructOperator
from domain.data.coh.tables.cuprod.struct import CUPROD_TABLE_STRUCTURE
from domain.interfaces.credential_management import ICredentialManager
from infrastructure.connections.postgres_connector import PostgresConnector
from infrastructure.logging.airflow_logger import AirflowLogger


@mark.skip
@mark.sql_table_reconstruct_operator
class S3TableReconstructTests:
    def test_s3_table_reconstruct_operator(
        self,
        mocker: MockerFixture,
        test_dag: DAG,
        airflow_logger: AirflowLogger,
        postgres_connector: PostgresConnector,
        dev_credential_manager: ICredentialManager,
    ):

        target_operator = SqlTableReconstructOperator(
            logger=airflow_logger,
            database_client=postgres_connector,
            table_name=dev_credential_manager.get("CUPROD_TABLE"),
            table_structure=CUPROD_TABLE_STRUCTURE,
            task_id="test_sql_table_reconstruct_operator",
            dag=test_dag,
        )

        pytest.helpers.run_operator(test_dag, target_operator)
