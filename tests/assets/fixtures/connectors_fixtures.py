from typing import Iterator
from pytest import fixture

from domain.interfaces.credential_management import ICredentialManager
from domain.interfaces.logging import ILogger
from infrastructure.connections.as400_connector import AS400Connector
from infrastructure.connections.postgres_connector import PostgresConnector
from infrastructure.connections.s3_connector import S3Connector


@fixture
def as400_connector(airflow_logger: ILogger, dev_credential_manager: ICredentialManager) -> Iterator[AS400Connector]:
    yield AS400Connector(dev_credential_manager, airflow_logger)


@fixture
def postgres_connector(
    airflow_logger: ILogger, dev_credential_manager: ICredentialManager
) -> Iterator[PostgresConnector]:
    yield PostgresConnector(dev_credential_manager, airflow_logger)


@fixture
def s3_connector(airflow_logger: ILogger, dev_credential_manager: ICredentialManager) -> Iterator[S3Connector]:
    yield S3Connector(dev_credential_manager, airflow_logger)
