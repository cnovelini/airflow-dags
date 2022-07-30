from airflow.models import BaseOperator

from domain.interfaces.logging import ILogger
from infrastructure.connections.as400_connector import AS400Connector
from infrastructure.connections.s3_connector import S3Connector


class AS400ToS3Operator(BaseOperator):
    def __init__(
        self, logger: ILogger, as400: AS400Connector, s3: S3Connector, query: str, target_s3_path: str, *args, **kwargs
    ):
        self.logger = logger
        self.as400 = as400
        self.s3 = s3
        self.as400_query = query
        self.target_s3_path = target_s3_path

        super().__init__(*args, **kwargs)

    def execute(self, *args, **kwargs):
        """Query CUPROD on AS400 and save's it into S3."""
        self.logger.info("Starting 'as400_to_s3' task")

        # Recovering information from AS400
        response = self.as400.query_as_df(self.as400_query)

        # Saving information on S3
        self.s3.save_as_csv(response, self.target_s3_path)
