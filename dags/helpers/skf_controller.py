from datetime import datetime
from pytz import timezone

from domain.abstractions.sql_database_connection import SQLConnector
from domain.constants.queries.control_queries import (
    DAG_CONTROL_TABLE_INSERT,
    DAG_CONTROL_TABLE_UPDATE,
    TASK_CONTROL_TABLE_UPDATE,
    TASK_ERROR_TABLE_INSERT,
)
from domain.enumerations.dag_status import DagStatus
from domain.enumerations.task_status import TaskStatus
from domain.interfaces.logging import ILogger
from infrastructure.connections.s3_connector import S3Connector


class SkfController:
    def __init__(
        self, logger: ILogger, database_client: SQLConnector, s3: S3Connector, xcom_key: str, error_log_folder: str
    ) -> None:
        self.logger = logger
        self.database_client = database_client
        self.s3 = s3
        self.xcom_key = xcom_key
        self.error_log_folder = error_log_folder
        self.timezone = timezone("America/Sao_Paulo")

    def start_dag_control(self, dag_name: str, status: DagStatus = DagStatus.STARTED) -> int:
        """Store a DAG start record in control table."""
        with self.database_client.session_scope() as session:

            current_datetime = datetime.now(tz=self.timezone)

            create_info = dict(
                dag_name=dag_name,
                status=int(status),
                process_start_datetime=current_datetime,
                insertion_datetime=current_datetime,
                insertion_user=self.database_client.current_user,
            )

            creation_result = session.execute(DAG_CONTROL_TABLE_INSERT.format(**create_info))

            new_dag_control_id = list(creation_result)[0][0]

            return int(new_dag_control_id)

    def end_dag_control(self, dag_control_id: int, status: DagStatus, processed_lines: int):
        """Store a DAG end record in control table."""
        with self.database_client.session_scope() as session:

            current_datetime = datetime.now(tz=self.timezone)

            update_info = dict(
                dag_control_id=dag_control_id,
                status=int(status),
                process_end_datetime=current_datetime,
                processed_records=processed_lines,
                update_datetime=current_datetime,
                update_user=self.database_client.current_user,
            )

            session.execute(DAG_CONTROL_TABLE_UPDATE.format(**update_info))

    def start_task_control(self, task_name: str, dag_control_id: int, status: TaskStatus = TaskStatus.STARTED) -> int:
        """Store a TASK start record in control table."""
        with self.database_client.session_scope() as session:

            current_datetime = datetime.now(tz=self.timezone)

            create_info = dict(
                dag_control_id=dag_control_id,
                task_name=task_name,
                status=int(status),
                transaction_start_datetime=current_datetime,
                insertion_datetime=current_datetime,
                insertion_user=self.database_client.current_user,
            )

            creation_result = session.execute(TASK_ERROR_TABLE_INSERT.format(**create_info))

            new_task_control_id = list(creation_result)[0][0]

            return int(new_task_control_id)

    def end_task_control(self, task_control_id: int, status: DagStatus, processed_lines: int):
        """Store a TASK end record in control table."""
        with self.database_client.session_scope() as session:

            current_datetime = datetime.now(tz=self.timezone)

            update_info = dict(
                task_control_id=task_control_id,
                status=int(status),
                transaction_end_datetime=current_datetime,
                processed_records=processed_lines,
                update_datetime=current_datetime,
                update_user=self.database_client.current_user,
            )

            session.execute(TASK_CONTROL_TABLE_UPDATE.format(**update_info))

    def inform_task_error(self, task_control_id: int, error_message: str):
        """Store a TASK ERROR record in control table."""
        with self.database_client.session_scope() as session:

            error_info = dict(
                task_id=task_control_id,
                error_details=error_message,
                insertion_datetime=datetime.now(tz=self.timezone),
                insertion_user=self.database_client.current_user,
            )

            session.execute(TASK_ERROR_TABLE_INSERT.format(**error_info))
