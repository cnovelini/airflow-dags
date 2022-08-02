import json
from airflow.models import BaseOperator
from airflow.models.taskinstance import TaskInstance

from domain.abstractions.sql_database_connection import SQLConnector
from domain.enumerations.database_insertion_method import DbInsertionMethod
from domain.enumerations.task_status import TaskStatus
from domain.exceptions.runtime_exceptions import SqlInsertionError
from domain.interfaces.logging import ILogger
from helpers.skf_controller import SkfController


class SqlToSqlOperator(BaseOperator):
    def __init__(
        self,
        logger: ILogger,
        controller: SkfController,
        origin_client: SQLConnector,
        destination_client: SQLConnector,
        origin_table_name: str,
        target_table_name: str,
        insertion_method: DbInsertionMethod,
        last_task: str,
        *args,
        **kwargs,
    ):
        self.logger = logger
        self.controller = controller
        self.origin_client = origin_client
        self.destination_client = destination_client
        self.origin_table_name = origin_table_name
        self.target_table_name = target_table_name
        self.insertion_method = insertion_method
        self.last_task = last_task

        super().__init__(*args, **kwargs)

    def execute(self, context: dict, *args, **kwargs):
        self.logger.info("Starting S3 files consumption")

        task_execution_status, task_errors = None, []
        processed_lines = 0

        task_instance: TaskInstance = context["ti"]
        xcom = json.loads(task_instance.xcom_pull(self.last_task, key=self.controller.xcom_key))
        task_control_id = self.controller.start_task_control(task_instance.task_id, xcom["dag_control_record_id"])

        try:
            self.logger.info("Recovering information from stage database")
            with self.origin_client.session_scope() as origin_session:
                stage_df = self.origin_client.read_as_df(origin_session, self.origin_table_name)

            self.logger.info("Sending information to target database")
            with self.destination_client.session_scope() as destination_session:
                insertion_info = self.destination_client.insert_dataframe(
                    destination_session, stage_df, self.target_table_name, self.insertion_method
                )

                if insertion_info["failed"] > 0:
                    task_errors = [f"Insertion of {insertion_info['failed']} lines failed."]
                    raise SqlInsertionError(insertion_info["failed"], insertion_info["errors"])

            processed_lines = insertion_info["processed"]
            task_execution_status = TaskStatus.SUCCESS
            task_errors = []

        except SqlInsertionError as insertion_err:
            self.logger.info("Storing insertion errors on control S3")
            errors_file_path = "/".join([self.controller.error_log_folder, f"{task_instance.dag_id}_errors.log"])
            self.controller.s3.save_log(path=errors_file_path, errors=insertion_err.errors)
            raise insertion_err

        except Exception as ex:
            self.logger.info("Sending failure information to error control table")
            task_execution_status = TaskStatus.FAILED
            self.controller.inform_task_error(task_control_id, f"{type(ex).__name__}: {ex}")
            raise ex

        finally:
            self.logger.info(f"Closing task control record with status: {task_execution_status.name}")
            self.controller.end_task_control(task_control_id, task_execution_status, processed_lines)

            self.logger.info("Updating XCom with task information")
            task_instance.xcom_push(
                self.controller.xcom_key,
                json.dumps(
                    {
                        **xcom,
                        f"{task_instance.task_id}_status": task_execution_status,
                        f"{task_instance.task_id}_errors": task_errors,
                        f"{task_instance.task_id}_details": dict(processed_lines=processed_lines),
                    }
                ),
            )
