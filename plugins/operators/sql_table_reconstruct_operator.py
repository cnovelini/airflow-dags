from airflow.models import BaseOperator

from domain.interfaces.logging import ILogger
from domain.abstractions.sql_database_connection import SQLConnector


class SqlTableReconstructOperator(BaseOperator):
    def __init__(
        self, logger: ILogger, database_client: SQLConnector, table_name: str, table_structure: str, *args, **kwargs
    ):
        self.logger = logger
        self.database_client = database_client
        self.table_name = table_name
        self.new_table_structure = table_structure

        super().__init__(*args, **kwargs)

    def execute(self, *args, **kwargs):
        self.logger.info(f"Starting 'table drop and recreate' task for {self.table_name}")

        # Opening database session
        with self.database_client.session_scope() as session:

            # Dropping table
            self.database_client.drop_table(session, self.table_name)

            # Recreating table
            self.database_client.create_table(session, self.table_name, self.new_table_structure)