from typing import Any

from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from domain.abstractions.sql_database_connection import SQLConnector
from domain.constants.queries.postgres_queries import CREATE_TABLE, DROP_TABLE
from domain.enumerations.database_insertion_method import DbInsertionMethod
from domain.exceptions.runtime_exceptions import (
    DbCreationError,
    DbDropError,
    DbInsertionError,
    UnknownInsertionMethodError,
)
from domain.interfaces.credential_management import ICredentialManager
from domain.interfaces.logging import ILogger


class PostgresConnector(SQLConnector):
    """Postgres database connection class."""

    def __init__(self, credential_manager: ICredentialManager, logger: ILogger):
        self.connection_string = credential_manager.generate_sqlalchemy_connection_string()
        self.logger = logger

        self.insertion_routines = {DbInsertionMethod.PD_TO_SQL: self.__pd_to_sql_insertion}

    def get_connection(self) -> Any:
        return create_engine(self.connection_string, pool_size=5, pool_recycle=3600)

    def drop_table(self, session: Session, target_table: str) -> None:
        """Executes a DROP command on informed target table.

        Parameters:
            session: (sqlalchemy.Session)
                The database session

            target_table: (str)
                The table name to be dropped

        Raises:
            DbDropError: Raised when drop routine failed
        """
        try:
            self.logger.info("Starting Postgres table drop...")
            session.execute(DROP_TABLE.format(target_table))
            self.logger.info("Postgres table drop executed with success!")

        except Exception as create_err:
            error_message = f"{type(create_err).__name__} -> {create_err}"
            self.logger.error(f"Error during Postgres table drop: {error_message}")
            raise DbDropError(error_message)

    def create_table(self, session: Session, target_table: str, table_column_structure: str) -> None:
        """Executes a CREATE command on informed target table.

        Parameters:
            session: (sqlalchemy.Session)
                The database session

            target_table: (str)
                The table name to be created

            table_column_structure: (str)
                The table columns specifications

        Raises:
            DbCreationError: Raised when creation routine failed
        """
        try:
            self.logger.info("Starting Postgres table creation...")
            session.execute(CREATE_TABLE.format(target_table, table_column_structure))
            self.logger.info("Postgres table creation executed with success!")

        except Exception as create_err:
            error_message = f"{type(create_err).__name__} -> {create_err}"
            self.logger.error(f"Error during Postgres table creation: {error_message}")
            raise DbCreationError(error_message)

    def insert_dataframe(
        self,
        session: Session,
        information: DataFrame,
        target_table: str,
        insertion_method: DbInsertionMethod = DbInsertionMethod.PD_TO_SQL,
    ) -> None:
        """Insert information on database. Able to execute multiple insertion methods.

        Parameters:
            session: (sqlalchemy.Session)
                The database session

            information: (pandas.DataFrame)
                The information to be inserted

            target_table: (str)
                The table name to be dropped

            insertion_method: (DbInsertionMethod)
                The method to be applied on insertion

        Raises:
            UnknownInsertionMethodError: Raised when the insertion method informed isn't mapped

            DbInsertionError: Raised when insertion routine failed
        """
        try:
            self.logger.info("Starting Postgres insertion...")
            self.insertion_routines[insertion_method](session, information, target_table)
            self.logger.info("Postgres insertion executed with success!")

        except KeyError:
            error_message = f"Insertion method not mapped yet: {insertion_method}. Please verify"
            self.logger.error(error_message)
            raise UnknownInsertionMethodError(error_message)

        except Exception as insert_err:
            error_message = f"{type(insert_err).__name__} -> {insert_err}"
            self.logger.error(f"Error during Postgres insertion: {error_message}")
            raise DbInsertionError(error_message)

    def __pd_to_sql_insertion(self, session: Session, information: DataFrame, target_table: str):
        """ "Executes INSERT command using Pandas to_sql interface."""
        self.logger.info("Executing pandas to_sql insertion method")
        information.to_sql(target_table, session.connection, if_exists="replace")
