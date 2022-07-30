from typing import Any

import pyodbc
from pandas import DataFrame, read_sql

from domain.constants.as400 import COMMIT_MODE_CS, CONNECTION_STRING, CONNECTION_TYPE_READWRITE
from domain.exceptions.runtime_exceptions import As400ConnectionError, As400QueryExecutionError
from domain.interfaces.credential_management import ICredentialManager
from domain.interfaces.database_connection import IDatabaseConnector
from domain.interfaces.logging import ILogger


class AS400Connector(IDatabaseConnector):
    """AS400 connection class."""

    def __init__(self, credential_manager: ICredentialManager, logger: ILogger):
        self.host = credential_manager.get("coh_hostname")
        self.username = credential_manager.get("coh_username")
        self.password = credential_manager.get("coh_password")
        self.logger = logger

    def __connection_string(
        self, host: str, username: str, password: str, commit_mode: int = None, connection_type: int = None
    ) -> str:
        """Build AS400 connection string.

        Parameters:
            host: (str)
                The AS400 server URL

            username: (str)
                The username credential

            password: (str)
                The password credential

            commit_mode: (int)
                The commit mode flag

            connection_type: (int)
                The connection type flag

        Returns:
            connection_string: (str)
                The constructed connection string

        """
        connection_string = CONNECTION_STRING.format(host, username, password)
        connection_string += f"CommitMode={commit_mode};" if commit_mode else ""
        connection_string += f"Connection_Type={connection_type};" if connection_type else ""
        return connection_string

    def get_connection(self) -> Any:
        self.logger.info(f"Connecting to AS400 host {self.host} with username {self.username}....")

        try:
            return pyodbc.connect(
                self.__connection_string(
                    self.host, self.username, self.password, COMMIT_MODE_CS, CONNECTION_TYPE_READWRITE
                )
            )

        except Exception as conn_ex:
            error_message = f"{type(conn_ex).__name__} -> {conn_ex}"
            self.logger.error(f"Failed to connect on AS400: {error_message}")
            raise As400ConnectionError(error_message)

    def query_as_df(self, expression: str) -> DataFrame:
        """Recover information from AS400 and transform the result on a Pandas DataFrame.

        Parameters:
            expression: (str)
                The SELECT query to be executed

        Returns:
            result: (pandas.DataFrame)
                The query result parsed on a DataFrame

        Raises:
            As400QueryExecutionError: Any error during query execution
        """
        try:
            self.logger.info(f'{">" * 30} Initializing as400 Query {"<" * 30}')
            result = read_sql(expression, self.get_connection())
            self.logger.info(f'{">" * 30} End as400 Query {" <" * 30}')

        except Exception as err:
            error_message = f"{type(err).__name__} -> {err}"
            self.logger.error(f'{"*" * 30} "Error during as400 Query: {error_message} {"*" * 30}')
            raise As400QueryExecutionError(error_message)

        return result
