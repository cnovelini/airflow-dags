from datetime import date, datetime
from logging import Logger
from pytz import timezone
from typing import Dict

from domain.exceptions.runtime_exceptions import PathConstructionError, PathNotFoundError
from domain.interfaces.credential_management import ICredentialManager


class FilePathManager:
    """File paths management class."""

    transformed_paths: Dict[str, str]

    def __init__(self, profile: ICredentialManager, logger: Logger) -> None:
        self.transformed_paths = {}
        self.default_prefix = profile.get("CUPROD_ETL_DEFAULT_S3_PREFIX")
        self.logger = logger

    def build_new_path(self, target_file_name: str) -> str:
        """Builds a new path, according with predefined rules.

        Parameters:
            target_file_name: (str)
                The file name to be extended

        Returns:
            transformed_path: (str)
                The file extended with default path, folder structure and timing

        Raises:
            PathConstructionError: Raised when path construction has failed
        """
        try:
            self.transformed_paths[
                target_file_name
            ] = f"{self.default_prefix}/{self.__get_date()}/{self.__insert_time(target_file_name)}"
            return self.transformed_paths[target_file_name]

        except Exception as path_err:
            error_message = f"{type(path_err).__name__} -> {path_err}"
            self.logger.error(f"Failed to build new path: {error_message}")
            raise PathConstructionError(error_message)

    def recover_last_path(self, target_file_name: str) -> str:
        """Recovers last created path.

        Parameters:
            target_file_name: (str)
                The original file name to be searched on created paths history

        Returns:
            transformed_path: (str)
                The previously transformed path

        Raises:
            PathNotFoundError: Raised when the file name is not recorded on paths history

        """
        try:
            return self.transformed_paths[target_file_name]

        except KeyError:
            self.logger.error(f"File name was never transformed: {target_file_name}")
            raise PathNotFoundError(target_file_name)

    def __get_date(self) -> date:
        """Get current date.

        Returns:
            current_date: (datetime.Date)
                The current date
        """
        return datetime.now().date()

    def __insert_time(self, target_file_name: str) -> str:
        """Insert current timestamp time on file name.

        Parameters:
            target_file_name: (str)
                The file name to be modified

        Returns:
            modified_file_name: (str)
                The file name with the current time added

        """
        time_to_be_inserted = datetime.strftime(datetime.now(tz=timezone("America/Sao_Paulo")), "%H-%m")
        parted_file_name = list(target_file_name.rpartition("/"))
        parted_file_name.insert(2, time_to_be_inserted)
        parted_file_name.insert(3, "_")
        return "".join(parted_file_name)
