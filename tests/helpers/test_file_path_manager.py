import re

import pytest
from pytest import mark

from domain.exceptions.runtime_exceptions import PathNotFoundError
from domain.interfaces.credential_management import ICredentialManager
from domain.interfaces.logging import ILogger
from helpers.file_path_manager import FilePathManager


@mark.file_path_manager
class FilePathManagerTests:
    def test_file_path_manager_instance(
        self, file_path_manager: FilePathManager, dev_credential_manager: ICredentialManager
    ):
        assert isinstance(file_path_manager, FilePathManager)

        assert isinstance(getattr(file_path_manager, "transformed_paths"), dict)
        assert getattr(file_path_manager, "default_prefix")
        assert getattr(file_path_manager, "logger")
        assert getattr(file_path_manager, "build_new_path")
        assert getattr(file_path_manager, "recover_last_path")

        assert isinstance(file_path_manager.transformed_paths, dict)
        assert isinstance(file_path_manager.default_prefix, str)
        assert isinstance(file_path_manager.logger, ILogger)

        assert file_path_manager.default_prefix == dev_credential_manager.get("CUPROD_ETL_DEFAULT_S3_PREFIX")

    def test_file_path_manager_build_new_path(
        self, file_path_manager: FilePathManager, dev_credential_manager: ICredentialManager
    ):
        path_prefix = dev_credential_manager.get("CUPROD_ETL_DEFAULT_S3_PREFIX")
        original_file_name = dev_credential_manager.get("CUPROD_DUMP_FILE")
        target_regex = r"^[a-zA-Z\/]+\d{4}\-\d{2}\-\d{2}\/[a-zA-Z]+\/\d{2}\-\d{2}\-\d{2}\-\d+\_.*\.[a-z]+$"
        new_path = file_path_manager.build_new_path(original_file_name)

        assert path_prefix in new_path
        assert original_file_name in file_path_manager.transformed_paths
        assert re.match(target_regex, new_path)

    def test_file_path_manager_recover_existent_path(
        self, file_path_manager: FilePathManager, dev_credential_manager: ICredentialManager
    ):
        new_path = file_path_manager.build_new_path(dev_credential_manager.get("CUPROD_DUMP_FILE"))

        recovered_path = file_path_manager.recover_last_path(dev_credential_manager.get("CUPROD_DUMP_FILE"))

        assert recovered_path == new_path

    def test_file_path_manager_recover_nonexistent_path(
        self, file_path_manager: FilePathManager, dev_credential_manager: ICredentialManager
    ):
        with pytest.raises(PathNotFoundError):
            file_path_manager.recover_last_path(dev_credential_manager.get("CUPROD_DUMP_FILE"))
