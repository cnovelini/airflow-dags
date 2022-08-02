from airflow.models import BaseOperator
from airflow.models.taskinstance import TaskInstance

from domain.abstractions.sql_database_connection import SQLConnector
from domain.enumerations.task_status import TaskStatus
from domain.interfaces.logging import ILogger
from helpers.skf_controller import SkfController


class SqlTableReconstructOperator(BaseOperator):
    def __init__(
        self,
        logger: ILogger,
        controller: SkfController,
        database_client: SQLConnector,
        table_name: str,
        table_structure: str,
        last_task: str,
        *args,
        **kwargs,
    ):
        self.logger = logger
        self.controller = controller
        self.database_client = database_client
        self.table_name = table_name
        self.new_table_structure = table_structure
        self.last_task = last_task

        super().__init__(*args, **kwargs)

    def execute(self, context, *args, **kwargs):
        self.logger.info(f"Starting 'table drop and recreate' task for {self.table_name}")

        task_execution_status, task_errors = None, None
        processed_lines = 0

        task_instance: TaskInstance = context["ti"]
        xcom = task_instance.xcom_pull(self.last_task, key=self.controller.xcom_key)
        task_control_id = self.controller.start_task_control(task_instance.task_id, xcom["dag_control_record_id"])

        try:
            # Opening database session
            with self.database_client.session_scope() as session:

                # Dropping table
                self.database_client.drop_table(session, self.table_name)

                # Recreating table
                self.database_client.create_table(session, self.table_name, self.new_table_structure)

            task_execution_status = TaskStatus.SUCCESS
            task_errors = []

        except Exception as ex:
            self.logger.info("Sending failure information to error control table")
            error_message = f"{type(ex).__name__}: {ex}"
            self.controller.inform_task_error(task_control_id, error_message)
            task_execution_status = TaskStatus.FAILED
            task_errors = [error_message]

        finally:
            self.logger.info(f"Closing task control record with status: {task_execution_status.name}")
            self.controller.end_task_control(task_control_id, task_execution_status, processed_lines)

            self.logger.info("Updating XCom with task information")
            task_instance.xcom_push(
                self.controller.xcom_key,
                {
                    **xcom,
                    f"{task_instance.task_id}_status": task_execution_status,
                    f"{task_instance.task_id}_errors": task_errors,
                },
            )
