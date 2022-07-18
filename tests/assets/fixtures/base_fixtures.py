from typing import Iterator

from pytest import fixture

from domain.enumerations.environment import Environment
from domain.interfaces.credential_management import ICredentialManager
from domain.interfaces.logging import ILogger
from helpers.dataframe_transformation_executioner import DataFrameTransformationExecutioner
from helpers.file_path_manager import FilePathManager
from helpers.profile_manager import ProfileManager
from helpers.transformers.dataframe_string_transformer import DataFrameStringTransformer
from infrastructure.connections.s3_connector import S3Connector
from infrastructure.credentials.test_credential_manager import TestCredentialManager
from infrastructure.logging.airflow_logger import AirflowLogger


@fixture
def dev_environment() -> Iterator[Environment]:
    yield Environment.DEV


@fixture
def dev_credential_manager(dev_environment: Environment) -> Iterator[ICredentialManager]:
    yield TestCredentialManager(dev_environment)


@fixture
def airflow_logger(dev_credential_manager: ICredentialManager) -> Iterator[AirflowLogger]:
    yield AirflowLogger(dev_credential_manager)


@fixture
def s3_connector(airflow_logger: ILogger, dev_credential_manager: ICredentialManager) -> Iterator[S3Connector]:
    yield S3Connector(dev_credential_manager, airflow_logger)


@fixture
def profile_manager() -> Iterator[ProfileManager]:
    yield ProfileManager()


@fixture
def file_path_manager(airflow_logger: ILogger, dev_credential_manager: ICredentialManager) -> Iterator[FilePathManager]:
    yield FilePathManager(dev_credential_manager, airflow_logger)


@fixture
def df_transf_executioner(airflow_logger: ILogger) -> Iterator[DataFrameTransformationExecutioner]:
    yield DataFrameTransformationExecutioner(airflow_logger)


@fixture
def df_string_transformer(airflow_logger: ILogger) -> Iterator[DataFrameStringTransformer]:
    yield DataFrameStringTransformer(airflow_logger)
