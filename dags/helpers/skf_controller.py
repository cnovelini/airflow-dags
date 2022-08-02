from datetime import datetime
from pytz import timezone

from domain.abstractions.sql_database_connection import SQLConnector
from domain.data.model.dag_control_record import DagControlRecord
from domain.data.model.task_control_record import TaskControlRecord
from domain.data.model.task_error_record import TaskErrorRecord
from domain.enumerations.dag_status import DagStatus
from domain.enumerations.task_status import TaskStatus
from domain.exceptions.control_exceptions import DagControlRecordNotFoundError, TaskControlRecordNotFoundError
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

            new_control = DagControlRecord(
                CTTR_NM=dag_name,
                CTTR_ST=int(status),
                CTTR_HR_DT_START_PROCESS=current_datetime,
                CTTR_DT_INSERT=current_datetime,
                CTTR_USER_INSERT=self.database_client.current_user,
            )

            session.add(new_control)

            return new_control.CTTR_ID

    def end_dag_control(self, control_id: int, status: DagStatus, processed_lines: int):
        """Store a DAG end record in control table."""
        with self.database_client.session_scope() as session:

            control_record: DagControlRecord = (
                session.query(DagControlRecord).filter(DagControlRecord.CTTR_ID == control_id).first()
            )

            if not control_record:
                raise DagControlRecordNotFoundError(control_id)

            current_datetime = datetime.now(tz=self.timezone)

            control_record.CTTR_ST = int(status)
            control_record.CTTR_HR_DT_END_PROCESS = current_datetime
            control_record.CTTR_QT_PROCESSED_RECORD = processed_lines
            control_record.CTTR_DT_UPDATE = current_datetime
            control_record.CTTR_USER_UPDATE = self.database_client.current_user

            session.flush()

    def start_task_control(self, task_name: str, dag_control_id: int, status: TaskStatus = TaskStatus.STARTED) -> int:
        """Store a TASK start record in control table."""
        with self.database_client.session_scope() as session:

            current_datetime = datetime.now(tz=self.timezone)

            new_control = TaskControlRecord(
                CTTR_ID=dag_control_id,
                CTTD_NM=task_name,
                CTTD_ST=int(status),
                CTTD_HR_DT_START_TRANSACTION=current_datetime,
                CTTD_DT_INSERT=current_datetime,
                CTTD_USER_INSERT=self.database_client.current_user,
            )

            session.add(new_control)

            return new_control.CTTD_ID

    def end_task_control(self, task_control_id: int, status: DagStatus, processed_lines: int):
        """Store a TASK end record in control table."""
        with self.database_client.session_scope() as session:

            control_record: TaskControlRecord = (
                session.query(TaskControlRecord).filter(TaskControlRecord.CTTD_ID == task_control_id).first()
            )

            if not control_record:
                raise TaskControlRecordNotFoundError(task_control_id)

            current_datetime = datetime.now(tz=self.timezone)

            control_record.CTTD_ST = int(status)
            control_record.CTTD_HR_DT_END_TRANSACTION = current_datetime
            control_record.CTTD_QT_PROCESSED_RECORD = processed_lines
            control_record.CTTD_DT_UPDATE = current_datetime
            control_record.CTTD_USER_UPDATE = self.database_client.current_user

            session.flush()

    def inform_task_error(self, task_control_id: int, error_message: str):
        """Store a TASK ERROR record in control table."""
        with self.database_client.session_scope() as session:

            current_datetime = datetime.now(tz=self.timezone)

            new_error = TaskErrorRecord(
                CTTD_ID=task_control_id,
                CTTE_DS=error_message,
                CTTE_DT_INSERT=current_datetime,
                CTTE_USER_INSERT=self.database_client.current_user,
            )

            session.add(new_error)
