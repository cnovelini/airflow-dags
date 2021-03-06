from botocore.stub import ANY, Stubber
from pandas import DataFrame
from pytest import mark, raises

from domain.exceptions.runtime_exceptions import PandasDataFrameGenerationError, S3FileDownloadError, S3FileUploadError
from domain.interfaces.credential_management import ICredentialManager
from domain.interfaces.database_connection import IDatabaseConnector
from domain.interfaces.logging import ILogger
from infrastructure.connections.s3_connector import S3Connector


@mark.s3_connector
class S3ConnectorTests:
    def test_s3_connector_instance(self, s3_connector: S3Connector, dev_credential_manager: ICredentialManager):
        assert isinstance(s3_connector, IDatabaseConnector)
        assert isinstance(s3_connector.logger, ILogger)

        assert getattr(s3_connector, "get_connection")
        assert getattr(s3_connector, "save_as_csv")
        assert getattr(s3_connector, "read_file_as_df")

        assert s3_connector.default_bucket == dev_credential_manager.get("COH_DUMP_BUCKET")
        assert s3_connector.access_key_id == dev_credential_manager.get("AWS_ACCESS_KEY_ID")
        assert s3_connector.secret_access_key_id == dev_credential_manager.get("AWS_SECRET_ACCESS_KEY")

    def test_s3_connection_file_read(
        self,
        s3_connector: S3Connector,
        clean_dataframe_mock: DataFrame,
        file_download_mock: dict,
        dev_credential_manager: ICredentialManager,
    ):
        try:
            s3_stub = Stubber(s3_connector.get_connection())
            s3_stub.add_response(
                "get_object",
                file_download_mock,
                {
                    "Bucket": dev_credential_manager.get("COH_DUMP_BUCKET"),
                    "Key": dev_credential_manager.get("CUPROD_CLEAN_FILE"),
                },
            )
            s3_stub.activate()

            read_df = s3_connector.read_file_as_df(dev_credential_manager.get("CUPROD_CLEAN_FILE"))
            print(read_df)
            print(clean_dataframe_mock)
            assert read_df.equals(clean_dataframe_mock)

        finally:
            s3_stub.deactivate()

    def test_s3_connection_file_upload(
        self,
        s3_connector: S3Connector,
        dataframe_mock: DataFrame,
        file_upload_mock: dict,
        dev_credential_manager: ICredentialManager,
    ):
        try:
            s3_stub = Stubber(s3_connector.get_connection())
            s3_stub.add_response(
                "put_object",
                file_upload_mock,
                {
                    "Bucket": dev_credential_manager.get("COH_DUMP_BUCKET"),
                    "Key": dev_credential_manager.get("CUPROD_DUMP_FILE"),
                    "Body": ANY,
                },
            )
            s3_stub.activate()

            s3_connector.save_as_csv(dataframe_mock, dev_credential_manager.get("CUPROD_DUMP_FILE"))
            assert True

        finally:
            s3_stub.deactivate()

    def test_s3_connector_file_upload_error_protection(
        self,
        s3_connector: S3Connector,
        dataframe_mock: DataFrame,
        dev_credential_manager: ICredentialManager,
    ):
        try:
            s3_stub = Stubber(s3_connector.get_connection())
            s3_stub.add_client_error("put_object")
            s3_stub.activate()

            with raises(S3FileUploadError):
                s3_connector.save_as_csv(dataframe_mock, dev_credential_manager.get("CUPROD_DUMP_FILE"))

        finally:
            s3_stub.deactivate()

    def test_s3_connector_file_download_error_protection(
        self,
        s3_connector: S3Connector,
        dev_credential_manager: ICredentialManager,
    ):
        try:
            s3_stub = Stubber(s3_connector.get_connection())
            s3_stub.add_client_error("get_object")
            s3_stub.activate()

            with raises(S3FileDownloadError):
                s3_connector.read_file_as_df(dev_credential_manager.get("CUPROD_CLEAN_FILE"))

        finally:
            s3_stub.deactivate()

    def test_s3_connector_dataframe_transformation_error_protection(
        self,
        s3_connector: S3Connector,
        broken_file_download_mock: dict,
        dev_credential_manager: ICredentialManager,
    ):
        try:
            s3_stub = Stubber(s3_connector.get_connection())
            s3_stub.add_response(
                "get_object",
                broken_file_download_mock,
                {
                    "Bucket": dev_credential_manager.get("COH_DUMP_BUCKET"),
                    "Key": dev_credential_manager.get("CUPROD_CLEAN_FILE"),
                },
            )
            s3_stub.activate()

            with raises(PandasDataFrameGenerationError):
                tst = s3_connector.read_file_as_df(dev_credential_manager.get("CUPROD_CLEAN_FILE"))
                print(tst)

        finally:
            s3_stub.deactivate()
