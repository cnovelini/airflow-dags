from unittest import mock

from decouple import config
from pytest import mark

from domain.enumerations.environment import Environment
from domain.interfaces.credential_management import ICredentialManager
from infrastructure.credentials.airflow_credential_manager import AirflowCredentialManager


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

    @mock.patch(
        "airflow.models.Variable.get",
        side_effect=[
            config("dev_db_type"),
            config("dev_db_lib"),
            config("dev_user"),
            config("dev_pass"),
            config("dev_host"),
            config("dev_db_port"),
            config("dev_db_schema"),
        ],
    )
    def test_airflow_credential_manager_db_conn_string_generation(self, dev_credential_manager: ICredentialManager):

        expected_connection_string = dev_credential_manager.generate_sqlalchemy_connection_string()

        airflow_manager = AirflowCredentialManager(Environment.DEV)

        assert airflow_manager.generate_sqlalchemy_connection_string() == expected_connection_string
