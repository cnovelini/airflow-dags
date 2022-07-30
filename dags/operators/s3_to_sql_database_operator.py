from airflow.models import BaseOperator

from domain.abstractions.sql_database_connection import SQLConnector
from domain.interfaces.logging import ILogger
from infrastructure.connections.s3_connector import S3Connector


class S3ToSqlDatabaseOperator(BaseOperator):
    def __init__(
        self,
        logger: ILogger,
        s3: S3Connector,
        database_client: SQLConnector,
        s3_file_path: str,
        target_table_name: str,
        *args,
        **kwargs
    ):
        self.logger = logger
        self.s3 = s3
        self.database_client = database_client
        self.s3_file_path = s3_file_path
        self.target_table_name = target_table_name

        super().__init__(*args, **kwargs)

    def execute(self, *args, **kwargs):
        """Transfers table from S3 to SQL database."""
        self.logger.info("Starting 's3_to_sql_database' task")

        # Recovering table from S3
        clean_table_df = self.s3.read_file_as_df(self.s3_file_path)

        # Inserting table on target SQL database
        with self.database_client.session_scope():
            self.database_client.insert_dataframe(clean_table_df, target_table=self.target_table_name)
