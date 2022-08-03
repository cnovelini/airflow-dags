import json
from airflow.models import BaseOperator
from airflow.models.taskinstance import TaskInstance

from domain.enumerations.task_status import TaskStatus

from domain.exceptions.runtime_exceptions import S3FileMoveError
from domain.interfaces.logging import ILogger
from helpers.skf_controller import SkfController
from infrastructure.connections.s3_connector import S3Connector


class RazacShipdateDataBackupOperator(BaseOperator):
    def __init__(
        self,
        logger: ILogger,
        controller: SkfController,
        s3: S3Connector,
        origin_folder: str,
        destination_folder: str,
        last_task: str,
        file_info_task: str,
        *args,
        **kwargs,
    ):
        self.logger = logger
        self.controller = controller
        self.s3 = s3
        self.origin_folder = origin_folder
        self.destination_folder = destination_folder
        self.last_task = last_task
        self.file_info_task = file_info_task

        super().__init__(*args, **kwargs)

    def execute(self, context: dict, *args, **kwargs):
        self.logger.info("Starting S3 files consumption")

        task_execution_status, task_errors = None, []
        action_status = {}
        processed_files = 0

        task_instance: TaskInstance = context["ti"]
        xcom = json.loads(task_instance.xcom_pull(self.last_task, key=self.controller.xcom_key))
        task_control_id = self.controller.start_task_control(task_instance.task_id, xcom["dag_control_record_id"])

        try:
            self.logger.info("Sending all files to backup folder")
            action_status = self.s3.move_files(self.origin_folder, self.destination_folder)

            if action_status["failed"] > 0:
                task_errors = [f"Backup of {action_status['failed']} files failed."]
                raise S3FileMoveError(action_status["failed"], action_status["errors"])

            task_execution_status = TaskStatus.SUCCESS
            processed_files = action_status["processed"]
            task_errors = []

        except Exception as ex:
            self.logger.info("Sending failure information to error control table")
            task_execution_status = TaskStatus.FAILED
            self.controller.inform_task_error(task_control_id, f"{type(ex).__name__}: {ex}")
            raise ex

        finally:
            self.logger.info("Updating XCom with processed information")

            self.logger.info(f"Closing task control record with status: {task_execution_status.name}")
            self.controller.end_task_control(task_control_id, task_execution_status, processed_files)

            self.logger.info("Updating XCom with task information")
            task_instance.xcom_push(
                self.controller.xcom_key,
                json.dumps(
                    {
                        **xcom,
                        f"{task_instance.task_id}_status": task_execution_status,
                        f"{task_instance.task_id}_errors": task_errors,
                        f"{task_instance.task_id}_details": dict(processed_files=processed_files),
                    }
                ),
            )
