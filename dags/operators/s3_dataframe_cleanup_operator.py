from airflow.models import BaseOperator

from domain.interfaces.information_transformation import TransformationExecutioner
from domain.interfaces.logging import ILogger
from infrastructure.connections.s3_connector import S3Connector


class S3DataframeCleanupOperator(BaseOperator):
    def __init__(
        self,
        logger: ILogger,
        s3: S3Connector,
        dataframe_cleaner: TransformationExecutioner,
        original_file_path: str,
        clean_file_path: str,
        *args,
        **kwargs
    ):
        self.logger = logger
        self.s3 = s3
        self.dataframe_cleaner = dataframe_cleaner
        self.original_file_path = original_file_path
        self.clean_file_path = clean_file_path

        super().__init__(*args, **kwargs)

    def execute(self, *args, **kwargs):
        """Cleanup S3 Data."""
        self.logger.info("Starting 's3_cleanup' task")

        # Recovering table from S3
        table_df = self.s3.read_file_as_df(self.original_file_path)

        # Executing cleanup routines
        clean_table_df = self.dataframe_cleaner.transform(table_df)

        # Saving clean table back on S3
        self.s3.save_as_csv(clean_table_df, self.clean_file_path)
