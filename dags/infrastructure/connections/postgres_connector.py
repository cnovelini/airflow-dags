from logging import Logger
from typing import Any, List

from pandas import DataFrame, read_sql
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine

from domain.abstractions.sql_database_connection import SQLConnector
from domain.constants.queries.postgres_queries import CREATE_TABLE, DROP_TABLE
from domain.enumerations.database_insertion_method import DbInsertionMethod
from domain.exceptions.runtime_exceptions import (
    DbCreationError,
    DbDropError,
    DbInsertionError,
    UnknownInsertionMethodError,
    PostgresTableRecoveryExecutionError,
)


class PostgresConnector(SQLConnector):
    """Postgres database connection class."""

    engine: Engine

    def __init__(
        self,
        logger: Logger,
        connection_string: str,
        internal_control_columns: List[str] = None,
        current_user: str = "NOT_INFORMED",
    ) -> None:
        self.logger = logger
        self.connection_string = connection_string
        self.internal_control_columns = internal_control_columns or []
        self.current_user = current_user

        self.engine = None
        self.insertion_routines = {
            DbInsertionMethod.FULL_PD_TO_SQL: self.__full_insertion,
            DbInsertionMethod.LINE_WISE_PD_TO_SQL: self.__line_wise_insertion,
        }

    def get_connection(self) -> Any:
        if not self.engine:
            self.engine = create_engine(self.connection_string, pool_size=5, pool_recycle=3600)
        return self.engine

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
            session.flush()
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
            session.flush()
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
        insertion_method: DbInsertionMethod = DbInsertionMethod.FULL_PD_TO_SQL,
        custom_query: str = None,
        column_types: dict = None,
    ) -> dict:
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
        if insertion_method not in self.insertion_routines:
            error_message = f"Insertion method not mapped yet: {insertion_method}. Please verify"
            self.logger.error(error_message)
            raise UnknownInsertionMethodError(error_message)

        try:
            self.logger.info("Starting Postgres insertion...")
            insertion_info = self.insertion_routines[insertion_method](
                session, information, target_table, custom_query, column_types
            )
            self.logger.info("Postgres insertion execution ended")

            return insertion_info

        except Exception as insert_err:
            error_message = f"{type(insert_err).__name__} -> {insert_err}"
            self.logger.error(f"Unknown error during Postgres insertion: {error_message}")
            raise DbInsertionError(error_message)

    def read_as_df(self, session: Session, table_name: str) -> DataFrame:
        """Reads table information and returns it as pandas DataFrame."""
        try:
            self.logger.info(f'{">" * 30} Initializing Postgres table data recovery {"<" * 30}')
            result = read_sql(table_name, session.connection())
            self.logger.info(f'{">" * 30} End Postgres table data recovery {"<" * 30}')

        except Exception as err:
            error_message = f"{type(err).__name__} -> {err}"
            self.logger.error(f'{"*" * 30} "Error during Postgres table data recovery: {error_message} {"*" * 30}')
            raise PostgresTableRecoveryExecutionError(error_message)

        return result

    def __full_insertion(self, session: Session, information: DataFrame, target_table: str, *args, **kwargs) -> dict:
        """Executes INSERT command using Pandas to_sql interface."""
        self.logger.info("Executing pandas to_sql insertion method")

        try:
            information.to_sql(target_table, session.connection(), if_exists="append", index=False)
            insertion_info = dict(processed=len(information), failed=0, errors=[])
        except Exception as ex:
            insertion_info = dict(processed=0, failed=len(information), errors=[f"{type(ex).__name__}: {ex}"])

        return insertion_info

    def __line_wise_insertion(
        self, session: Session, information: DataFrame, target_table: str, custom_query: str, column_types: dict
    ):
        """Executes INSERT command using line-wise insertion method."""
        self.logger.info("Executing line-wise insertion method")

        insertion_info = dict(processed=0, failed=0, errors=[])

        first_log = True
        for info_row in information.to_dict("records"):
            try:
                DataFrame(
                    {key: value for key, value in info_row.items() if key not in self.internal_control_columns}
                ).to_sql(target_table, if_exists="append", index=False)
                # nan_replacement = "NULL"
                # info_row = {key: nan_replacement if str(value) == "nan" else value for key, value in info_row.items()}
                # info_row = {
                #     key: column_types[key](value) if key in column_types and value != nan_replacement else value
                #     for key, value in info_row.items()
                # }
                # info_row["table_name"] = target_table

                # session.execute(custom_query.format(**info_row).replace("'NULL'", "NULL").replace("'NULL'", "NULL"))
                # session.flush()

                insertion_info["processed"] += 1

            except Exception as insert_err:
                if first_log:
                    print(info_row)
                    first_log = False
                self.logger.error(f"Error to insert line {info_row['line']}")
                insertion_info["failed"] += 1
                insertion_info["errors"].append(
                    " ".join(
                        [
                            f"File: {info_row['file']};",
                            f"Row: {info_row['line']};",
                            f"Error: {type(insert_err).__name__} -> {insert_err}",
                        ]
                    )
                )
                session.rollback()

        return insertion_info
