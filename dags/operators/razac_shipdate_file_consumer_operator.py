import json
from logging import Logger
from airflow.models import BaseOperator
from airflow.models.taskinstance import TaskInstance
from typing import Dict

from domain.abstractions.sql_database_connection import SQLConnector
from domain.enumerations.database_insertion_method import DbInsertionMethod
from domain.enumerations.task_status import TaskStatus
from domain.enumerations.vendor_code import VendorCode
from domain.exceptions.razac_exceptions import RazacShipdateInsertionError
from helpers.skf_controller import SkfController
from infrastructure.connections.s3_connector import S3Connector


class RazacShipdateFileConsumerOperator(BaseOperator):
    def __init__(
        self,
        logger: Logger,
        controller: SkfController,
        s3: S3Connector,
        database_client: SQLConnector,
        target_table_name: str,
        columns_map: Dict[str, str],
        encoding: str,
        delimiter: str,
        insertion_query: str,
        last_task: str,
        target_folder: str,
        *args,
        **kwargs,
    ):
        self.logger = logger
        self.controller = controller
        self.s3 = s3
        self.database_client = database_client
        self.target_table_name = target_table_name
        self.columns_map = columns_map
        self.insertion_query = insertion_query
        self.encoding = encoding
        self.delimiter = delimiter
        self.last_task = last_task
        self.target_folder = target_folder

        super().__init__(*args, **kwargs)

    def execute(self, context: dict, *args, **kwargs):
        self.logger.info("Starting S3 files consumption")

        task_execution_status, task_errors = None, []
        insertion_status = {}
        processed_lines = 0

        task_instance: TaskInstance = context["ti"]
        xcom = json.loads(task_instance.xcom_pull(self.last_task, key=self.controller.xcom_key))
        task_control_id = self.controller.start_task_control(task_instance.task_id, xcom["dag_control_record_id"])

        try:
            self.logger.info("Recovering shipdate latest file")
            target_file_path = self.s3.discover_latest_file(target_folder=self.target_folder, extension=".csv")

            self.logger.info("Reading target file as pandas DataFrames")
            target_file_df = self.s3.read_file_as_df(target_file_path, self.encoding, self.delimiter)
            target_file_df = target_file_df.rename(columns=self.columns_map)
            target_file_df["file"] = target_file_path
            target_file_df["line"] = target_file_df.index + 1

            self.logger.info("Inserting internal control columns")
            target_file_df["cttd_id"] = task_control_id
            target_file_df["impo_cd_despachante"] = int(VendorCode.RAZAC)

            self.logger.info("Sending information to SQL database")
            with self.database_client.session_scope() as session:
                insertion_status = self.database_client.insert_dataframe(
                    session,
                    target_file_df,
                    self.target_table_name,
                    DbInsertionMethod.LINE_WISE_PD_TO_SQL,
                    custom_query=self.insertion_query,
                )
                if insertion_status["failed"] > 0:
                    task_errors = [f"Insertion of {insertion_status['failed']} lines failed."]
                    raise RazacShipdateInsertionError(insertion_status["failed"], insertion_status["errors"])

                processed_lines = insertion_status["processed"]

            self.logger.info("Updating XCom with processed information")

            task_execution_status = TaskStatus.SUCCESS
            task_errors = []

        except RazacShipdateInsertionError as insertion_err:
            self.logger.info("Storing insertion errors on S3")
            errors_file_path = "/".join([self.controller.error_log_folder, f"{task_instance.dag_id}_errors.log"])
            self.controller.s3.save_log(path=errors_file_path, errors=insertion_err.errors)

        except Exception as ex:
            self.logger.info("Sending failure information to error control table")
            task_execution_status = TaskStatus.FAILED
            self.controller.inform_task_error(task_control_id, f"{ex}")
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
