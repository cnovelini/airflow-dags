import logging

from pytest import LogCaptureFixture, mark

from domain.interfaces.credential_management import ICredentialManager
from domain.interfaces.logging import ILogger
from infrastructure.logging.airflow_logger import AirflowLogger


@mark.airflow_logger
class AirflowLoggerTests:
    def test_airflow_logger_instance(
        self, caplog: LogCaptureFixture, airflow_logger: AirflowLogger, dev_credential_manager: ICredentialManager
    ):

        assert isinstance(airflow_logger, ILogger)
        assert isinstance(getattr(airflow_logger, "logger"), logging.Logger)

        assert getattr(airflow_logger, "debug")
        assert getattr(airflow_logger, "info")
        assert getattr(airflow_logger, "warn")
        assert getattr(airflow_logger, "error")

        with caplog.at_level(logging.DEBUG):
            airflow_logger.debug("DEBUG TEST")
            airflow_logger.info("INFO TEST")
            airflow_logger.warning("WARNING TEST")
            airflow_logger.error("ERROR TEST")

        assert len(caplog.records) == 4
        assert all([record.name == dev_credential_manager.get("AIRFLOW_LOGGER_NAME") for record in caplog.records])

        recorded_levels = [record.levelno for record in caplog.records]
        expected_levels = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR]
        assert recorded_levels == expected_levels
