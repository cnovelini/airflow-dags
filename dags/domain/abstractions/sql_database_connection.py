from abc import abstractmethod
from contextlib import contextmanager
from pandas import DataFrame
from sqlalchemy.orm import Session
from typing import Iterator
from domain.enumerations.database_insertion_method import DbInsertionMethod

from domain.exceptions.runtime_exceptions import DbSessionError
from domain.interfaces.database_connection import IDatabaseConnector


class SQLConnector(IDatabaseConnector):
    """SQL Database connection interface."""

    current_user: str

    @contextmanager
    def session_scope(self) -> Iterator[Session]:
        """Generates a database session context.

        Returns:
            session: (sqlalchemy.Session)
                The database session

        Raises:
            PostgresDbError: Raised when any database operation error occurs
        """
        session = Session(self.get_connection(), autocommit=False)
        try:
            yield session
            session.commit()
        except Exception as session_err:
            session.rollback()
            raise DbSessionError(f"{type(session_err).__name__} -> {session_err}")
        finally:
            session.close()

    @abstractmethod
    def drop_table(self, session: Session, target_table: str) -> None:
        """Executes a DROP command on informed target table."""

    @abstractmethod
    def create_table(self, session: Session, target_table: str, table_column_structure: str) -> None:
        """Executes a CREATE command on informed target table."""

    @abstractmethod
    def insert_dataframe(
        self,
        session: Session,
        information: DataFrame,
        target_table: str,
        insertion_method: DbInsertionMethod = DbInsertionMethod.FULL_PD_TO_SQL,
        custom_query: str = None,
        column_types: dict = None,
    ) -> None:
        """Insert information on database. Able to execute multiple insertion methods."""

    @abstractmethod
    def read_as_df(self, session: Session, table_name: str) -> DataFrame:
        """Reads table information and returns it as pandas DataFrame."""
