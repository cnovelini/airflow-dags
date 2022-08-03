from typing import List
from airflow import AirflowException


class RuntimeError(AirflowException):
    def __init__(self, message: str) -> None:
        message = f"Runtime error details: {message}"
        super().__init__(message)


class DataFrameStringStripError(RuntimeError):
    pass


class PathConstructionError(RuntimeError):
    pass


class PathNotFoundError(RuntimeError):
    pass


class As400ConnectionError(RuntimeError):
    pass


class As400QueryExecutionError(RuntimeError):
    pass


class DbSessionError(RuntimeError):
    pass


class DbDropError(RuntimeError):
    pass


class DbCreationError(RuntimeError):
    pass


class UnknownInsertionMethodError(RuntimeError):
    pass


class DbInsertionError(RuntimeError):
    pass


class S3FileUploadError(RuntimeError):
    pass


class S3FileDownloadError(RuntimeError):
    pass


class S3FileNotFoundForExtensionError(RuntimeError):
    def __init__(self, target_bucket: str, target_folder: str, extension: str) -> None:
        message = f"No file was found on {target_bucket}/{target_folder} with the extension informed: {extension}"
        super().__init__(message)


class S3FileToListGenerationError(RuntimeError):
    pass


class PandasDataFrameGenerationError(RuntimeError):
    pass


class TransformationExecutionError(RuntimeError):
    pass


class PostgresTableRecoveryExecutionError(RuntimeError):
    pass


class SqlInsertionError(RuntimeError):
    def __init__(self, lines_failed: int, errors: List[str]) -> None:
        self.errors = errors
        message = (
            f"Task failed to insert all information to database. {lines_failed} line(s) failed."
            " Logs can be consulted on origin S3 /err folder."
        )
        super().__init__(message)


class S3FileMoveError(RuntimeError):
    def __init__(self, lines_failed: int, errors: List[str]) -> None:
        self.errors = errors
        message = (
            f"Task failed to move all files requested. {lines_failed} line(s) failed."
            " Logs can be consulted on origin S3 /err folder."
        )
        super().__init__(message)
