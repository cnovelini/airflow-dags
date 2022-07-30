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


class PandasDataFrameGenerationError(RuntimeError):
    pass


class TransformationExecutionError(RuntimeError):
    pass
