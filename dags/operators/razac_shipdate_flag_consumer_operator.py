import json
from airflow.models import BaseOperator
from airflow.models.taskinstance import TaskInstance
import pandas as pd
from typing import Dict

from domain.abstractions.sql_database_connection import SQLConnector
from domain.enumerations.database_insertion_method import DbInsertionMethod
from domain.enumerations.task_status import TaskStatus
from domain.enumerations.vendor_code import VendorCode
from domain.exceptions.razac_exceptions import RazacShipdateInsertionError
from domain.interfaces.logging import ILogger
from helpers.skf_controller import SkfController
from infrastructure.connections.s3_connector import S3Connector


class RazacShipdateFlagConsumerOperator(BaseOperator):
    def __init__(
        self,
        logger: ILogger,
        controller: SkfController,
        s3: S3Connector,
        database_client: SQLConnector,
        target_table_name: str,
        columns_map: Dict[str, str],
        last_task: str,
        *args,
        **kwargs,
    ):
        self.logger = logger
        self.controller = controller
        self.s3 = s3
        self.database_client = database_client
        self.target_table_name = target_table_name
        self.columns_map = columns_map
        self.last_task = last_task

        super().__init__(*args, **kwargs)

    def execute(self, context: dict, *args, **kwargs):
        self.logger.info("Starting S3 files consumption")

        task_execution_status, task_errors = None, None
        insertion_status = {}
        processed_lines = 0
        processed_files = []
        target_folder = "shipdate"

        task_instance: TaskInstance = context["ti"]
        xcom = json.loads(task_instance.xcom_pull(self.last_task, key=self.controller.xcom_key))
        task_control_id = self.controller.start_task_control(task_instance.task_id, xcom["dag_control_record_id"])

        try:
            self.logger.info("Recovering shipdate flag file (txt)")
            flag_file_path = self.s3.discover_first_file(target_folder=f"{target_folder}/in", extension=".txt")

            self.logger.info("Recovering target file names from flag file")
            flag_file_lines = self.s3.read_file_as_list(flag_file_path)

            self.logger.info("Reading target files as pandas DataFrames")
            target_files_df_list = []
            for file_name in flag_file_lines:
                file_df = self.s3.read_file_as_df("/".join([target_folder, "in", file_name]))
                file_df["file"] = file_name
                file_df["line"] = file_df.index + 1

                target_files_df_list.append(file_df)

            self.logger.info("Joining all files informed inside flag inside single DataFrame")
            target_files_df = pd.concat(target_files_df_list)

            self.logger.info("Inserting internal control columns")
            target_files_df["CTTD_ID"] = task_control_id
            target_files_df["IMPO_CD_DESPACHANTE"] = int(VendorCode.RAZAC)

            self.logger.info("Sending information to SQL database")
            with self.database_client.session_scope() as session:
                insertion_status = self.database_client.insert_dataframe(
                    session, target_files_df, self.target_table_name, DbInsertionMethod.LINE_WISE_PD_TO_SQL
                )
                if insertion_status["failed"] > 0:
                    task_errors = [f"Insertion of {insertion_status['failed']} lines failed."]
                    raise RazacShipdateInsertionError(insertion_status["failed"], insertion_status["errors"])

                processed_lines = insertion_status["processed"]

            self.logger.info("Updating XCom with processed information")

            task_execution_status = TaskStatus.SUCCESS
            task_errors = []
            processed_files = [flag_file_path] + flag_file_lines

        except RazacShipdateInsertionError as insertion_err:
            self.logger.info("Storing insertion errors on S3")
            errors_file_path = "/".join([self.controller.error_log_folder, f"{task_instance.dag_id}_errors.log"])
            self.controller.s3.save_log(path=errors_file_path, errors=insertion_err.errors)

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
                        f"{task_instance.task_id}_details": dict(processed_files=processed_files),
                    }
                ),
            )
