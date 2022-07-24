from decouple import config
from pytest import mark
from pytest_mock import MockerFixture

from domain.enumerations.environment import Environment
from domain.interfaces.credential_management import ICredentialManager
from infrastructure.credentials.airflow_credential_manager import AirflowCredentialManager
from tests.assets.patchers.airflow_variable_patcher import AirflowVariablePatcher


@mark.airflow_credential_manager
class AirflowCredentialManagerTests:
    def test_airflow_credential_manager_instance(self):

        airflow_manager = AirflowCredentialManager(Environment.DEV)

        assert isinstance(airflow_manager, ICredentialManager)

        assert getattr(airflow_manager, "get")
        assert getattr(airflow_manager, "get_user")
        assert getattr(airflow_manager, "get_pass")
        assert getattr(airflow_manager, "get_host")
        assert getattr(airflow_manager, "generate_sqlalchemy_connection_string")

    def test_airflow_credential_manager_db_conn_string_generation(
        self, mocker: MockerFixture, dev_credential_manager: ICredentialManager
    ):

        patcher = AirflowVariablePatcher(mocker)
        patcher.patch_method_side_effect(
            "get",
            [
                config("dev_db_type"),
                config("dev_db_lib"),
                config("dev_user"),
                config("dev_pass"),
                config("dev_host"),
                config("dev_db_port"),
                config("dev_db_schema"),
            ],
        )

        expected_connection_string = dev_credential_manager.generate_sqlalchemy_connection_string()

        airflow_manager = AirflowCredentialManager(Environment.DEV)

        assert airflow_manager.generate_sqlalchemy_connection_string() == expected_connection_string
