import os
from airflow.models import Variable

from domain.enumerations.environment import Environment
from domain.enumerations.naming_convention import NamingConvention
from domain.interfaces.credential_management import ICredentialManager


class AirflowCredentialManager(ICredentialManager):
    """Credential management using Airflow environment vault."""

    def __init__(self, environment: Environment) -> None:
        self.env = NamingConvention.names[environment]

    def __get(self, credential_name: str) -> str:
        real_cred_name = self.env.get(credential_name, credential_name)
        try:
            return Variable.get(real_cred_name)
        except Exception:
            return os.getenv(real_cred_name)

    def get(self, name: str) -> str:
        return self.__get(name)  # pragma: no cover

    def get_user(self) -> str:
        return self.__get("user")

    def get_pass(self) -> str:
        return self.__get("pass")

    def get_host(self) -> str:
        return self.__get("host")

    def generate_sqlalchemy_connection_string(
        self,
        user_key: str = None,
        password_key: str = None,
        host_key: str = None,
        schema_key: str = None,
        db_type_key: str = None,
        db_lib_key: str = None,
        db_port_key: str = None,
    ) -> str:

        user = self.__get(user_key) if user_key else self.get_user()
        password = self.__get(password_key) if password_key else self.get_pass()
        host = self.__get(host_key) if host_key else self.get_host()
        schema = self.__get(schema_key or "db_schema")
        db_type = self.__get(db_type_key or "db_type")
        db_lib = self.__get(db_lib_key or "db_lib")
        db_port = self.__get(db_port_key or "db_port")

        return f"{db_type}+{db_lib}://{user}:{password}@{host}:{db_port}/{schema}"
