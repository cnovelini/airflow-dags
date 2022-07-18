from airflow.models import Variable

from domain.enumerations.environment import Environment
from domain.enumerations.naming_convention import NamingConvention
from domain.interfaces.credential_management import ICredentialManager


class AirflowCredentialManager(ICredentialManager):
    """Credential management using Airflow environment vault."""

    def __init__(self, environment: Environment) -> None:
        self.env = NamingConvention.names[environment]

    def __get(self, credential_name: str) -> str:
        return Variable.get(self.env.get(credential_name, credential_name))

    def get(self, name: str) -> str:
        return self.__get(name)  # pragma: no cover

    def get_user(self) -> str:
        return self.__get("user")

    def get_pass(self) -> str:
        return self.__get("pass")

    def get_host(self) -> str:
        return self.__get("host")

    def generate_sqlalchemy_connection_string(self) -> str:
        return "".join(
            [
                f"{self.__get('db_type')}+{self.__get('db_lib')}://",
                f"{self.get_user()}:{self.get_pass()}",
                f"@{self.get_host()}:{self.__get('db_port')}",
                f"/{self.__get('db_schema')}",
            ]
        )
