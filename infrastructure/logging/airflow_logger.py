import logging

from domain.interfaces.credential_management import ICredentialManager
from domain.interfaces.logging import ILogger


class AirflowLogger(ILogger):
    def __init__(self, profile: ICredentialManager) -> None:
        self.logger = logging.getLogger(profile.get("AIRFLOW_LOGGER_NAME"))

    def debug(self, message: str) -> None:
        self.logger.debug(message)

    def info(self, message: str) -> None:
        self.logger.info(message)

    def warning(self, message: str) -> None:
        self.logger.warning(message)

    def error(self, message: str) -> None:
        self.logger.error(message)
