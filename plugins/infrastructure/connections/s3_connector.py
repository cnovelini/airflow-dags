from io import StringIO
from typing import Any

import boto3
from pandas import DataFrame, read_csv

from domain.exceptions.runtime_exceptions import PandasDataFrameGenerationError, S3FileDownloadError, S3FileUploadError
from domain.interfaces.credential_management import ICredentialManager
from domain.interfaces.database_connection import IDatabaseConnector
from domain.interfaces.logging import ILogger


class S3Connector(IDatabaseConnector):
    """AWS S3 bucket connection class."""

    s3_client: boto3.client = None

    def __init__(self, credential_manager: ICredentialManager, logger: ILogger, default_bucket: str = None):
        self.access_key_id = credential_manager.get("AWS_ACCESS_KEY_ID")
        self.secret_access_key_id = credential_manager.get("AWS_SECRET_ACCESS_KEY")
        self.default_bucket = default_bucket or credential_manager.get("COH_DUMP_BUCKET")
        self.logger = logger

    def get_connection(self) -> Any:
        if not self.s3_client:
            self.logger.info("Generating S3 connection object...")
            self.s3_client = boto3.client(
                "s3", aws_access_key_id=self.access_key_id, aws_secret_access_key=self.secret_access_key_id
            )
            self.logger.info("S3 connection object generated with success!")

        return self.s3_client

    def save_as_csv(self, information: DataFrame, target_path: str, target_bucket: str = None) -> None:
        """Saves dataFrame information on S3 as CSV file.

        Parameters:
            information: (pandas.DataFrame)
                The information to be stored as CSV

            target_path: (str)
                The path inside S3 bucket where the file will be stored

            target_bucket: (str)
                The bucket where the information should be stored.
                If bucket not informed, uses the default bucket

        Raises:
            S3FileUploadError: Raised when file upload fails
        """
        try:
            self.logger.info("Starting S3 file upload routine...")
            with StringIO() as csv_buffer:

                self.logger.info("Transforming DataFrame to CSV...")
                information.to_csv(csv_buffer, index=False)

                self.logger.info(
                    f"Sending CSV file to S3 bucket {target_bucket or self.default_bucket} on path {target_path}"
                )
                self.get_connection().put_object(
                    Bucket=target_bucket or self.default_bucket, Key=target_path, Body=csv_buffer.getvalue()
                )
            self.logger.info("S3 file upload executed with success!")

        except Exception as upload_err:
            error_message = f"{type(upload_err).__name__} -> {upload_err}"
            self.logger.error(f"Failed to upload information on S3: {error_message}")
            raise S3FileUploadError(error_message)

    def read_file_as_df(self, target_path: str, target_bucket: str = None) -> DataFrame:
        """Read file from S3 and attempt to transform it on a Pandas DataFrame.

        Parameters:
            target_path: (str)
                The S3 file path to be downloaded

            target_bucket: (str)
                The target S3 bucket. If bucket not informed, uses the default bucket

        Raises:
            S3FileDownloadError: Raised if any error occurs on file download

            PandasDataFrameGenerationError: Raised when DataFrame generation fails
        """
        try:
            self.logger.info(f"Recovering {target_path} file from {target_bucket or self.default_bucket} bucket...")
            s3_dump_file = self.get_connection().get_object(
                Bucket=target_bucket or self.default_bucket, Key=target_path
            )
            self.logger.info("S3 file download executed with success!")
        except Exception as download_err:
            error_message = f"{type(download_err).__name__} -> {download_err}"
            self.logger.error(f"Failed to download information from S3: {error_message}")
            raise S3FileDownloadError(error_message)

        try:
            self.logger.info("Transforming downloaded CSV file into pandas DataFrame...")
            file_df = read_csv(s3_dump_file.get("Body"))
            self.logger.info("CSV to DataFrame transformation executed with success!")
        except Exception as csv_err:
            error_message = f"{type(csv_err).__name__} -> {csv_err}"
            self.logger.error(f"Failed to transform CSV to DataFrame: {error_message}")
            raise PandasDataFrameGenerationError(error_message)

        return file_df
