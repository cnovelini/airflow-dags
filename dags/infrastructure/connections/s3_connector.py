from io import StringIO
from typing import Any, List

import boto3
from pandas import DataFrame, read_csv

from domain.exceptions.runtime_exceptions import (
    PandasDataFrameGenerationError,
    S3FileDownloadError,
    S3FileUploadError,
    S3FileNotFoundForExtensionError,
    S3FileToListGenerationError,
)
from domain.interfaces.database_connection import IDatabaseConnector
from domain.interfaces.logging import ILogger


class S3Connector(IDatabaseConnector):
    """AWS S3 bucket connection class."""

    s3_client: boto3.client = None

    def __init__(self, logger: ILogger, access_key: str, secret_access_key: str, default_bucket: str):
        self.access_key_id = access_key
        self.secret_access_key_id = secret_access_key
        self.default_bucket = default_bucket
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
                    f"Sending CSV file to S3 bucket {(target_bucket or self.default_bucket)} on path {target_path}"
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
            self.logger.info(f"Recovering {target_path} file from {(target_bucket or self.default_bucket)} bucket...")
            s3_dump_file = self.get_connection().get_object(
                Bucket=(target_bucket or self.default_bucket), Key=target_path
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

    def discover_first_file(self, target_folder: str, extension: str, target_bucket: str = None):
        """Search inside target folder for the first file with the informed extension"""

        bucket_iterator = (
            self.get_connection()
            .get_paginator("list_objects_v2")
            .paginate(Bucket=(target_bucket or self.default_bucket))
        )

        target_objects = list(bucket_iterator.search(f"Contents[?contains(Key, '{extension}')][]"))

        if len(target_objects) == 0:
            raise S3FileNotFoundForExtensionError((target_bucket or self.default_bucket), target_folder, extension)

        return "/".join([target_folder, target_objects[0]["Key"]])

    def read_file_as_list(self, target_path: str, target_bucket: str = None) -> List[str]:
        """Reads a file from S3 and convert the file lines as a list of strings."""
        try:
            self.logger.info(f"Recovering {target_path} file from {(target_bucket or self.default_bucket)} bucket...")
            s3_file = self.get_connection().get_object(Bucket=(target_bucket or self.default_bucket), Key=target_path)
            self.logger.info("S3 file download executed with success!")
        except Exception as download_err:
            error_message = f"{type(download_err).__name__} -> {download_err}"
            self.logger.error(f"Failed to download information from S3: {error_message}")
            raise S3FileDownloadError(error_message)

        try:
            self.logger.info("Transforming downloaded file into list of strings...")
            file_lines = [line.decode("utf-8") for line in s3_file["Body"].iter_lines()]
            self.logger.info("file transformation executed with success!")
        except Exception as csv_err:
            error_message = f"{type(csv_err).__name__} -> {csv_err}"
            self.logger.error(f"Failed to transform file into list of strings: {error_message}")
            raise S3FileToListGenerationError(error_message)

        return file_lines

    def move_files(self, all_files: List[str], origin_folder: str, destination_folder: str, bucket: str = None) -> dict:
        """Move all files between informed folders inside the same bucket."""

        move_results = dict(processed=0, failed=0, errors=[], processed_files=[])
        bucket = bucket or self.default_bucket

        for file_path in all_files:
            file_name = file_path.split("/")[-1]
            try:
                self.__move_file(bucket, f"{origin_folder}/{file_name}", f"{destination_folder}/{file_name}")
                move_results["processed"] += 1
                move_results["processed_files"].append(file_name)
            except Exception as ex:
                move_results["failed"] += 1
                move_results["errors"].append(f"Error to move file {file_name}: {type(ex).__name__} => {ex}")

        if move_results["failed"] > 0:
            self.logger.info(f"{move_results['failed']} files failed. Rolling back already processed files")
            for file_name in move_results["processed_files"]:
                self.__move_file(bucket, f"{destination_folder}/{file_name}", f"{origin_folder}/{file_name}")

        return move_results

    def save_log(self, log_file_path, information: List[str], bucket: str = None) -> None:
        """Saves a log file with the information provided."""

        log_info_stream = StringIO()
        for info_line in information:
            log_info_stream.write(info_line)

        try:
            self.get_connection().put_object(
                Bucket=bucket or self.default_bucket, Key=log_file_path, Body=log_info_stream.getvalue().encode("utf-8")
            )
        except Exception as upload_err:
            error_message = f"{type(upload_err).__name__} -> {upload_err}"
            self.logger.error(f"Failed to upload log information on S3: {error_message}")
            raise S3FileUploadError(error_message)

    def __move_file(self, bucket: str, source: str, destination: str):
        """Move files inside S3 Bucket"""
        s3_client = self.get_connection()

        s3_client.copy_object(Bucket=bucket, CopySource=dict(Bucket=bucket, Key=source), Key=destination)
        s3_client.delete_object(Bucket=bucket, Key=source)
