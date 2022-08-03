import json
from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.models.taskinstance import TaskInstance

from domain.enumerations.dag_status import DagStatus
from domain.exceptions.control_exceptions import UnknownControlActionError
from domain.interfaces.logging import ILogger
from helpers.skf_controller import SkfController


class ControlOperator(BaseOperator):
    def __init__(self, logger: ILogger, controller: SkfController, action: str, last_task: str = None, *args, **kwargs):
        self.logger = logger
        self.controller = controller
        self.action = action
        self.last_task = last_task

        self.known_actions = {
            "dag_start": self.__execute_start_control,
            "dag_end_success_control": self.__execute_dag_success_end_control,
            "dag_end_failure_control": self.__execute_dag_failure_end_control,
        }

        super().__init__(*args, **kwargs)

    def execute(self, context, *args, **kwargs):

        if self.action not in self.known_actions:
            raise UnknownControlActionError(self.action, list(self.known_actions.keys()))

        self.known_actions[self.action](context)

    def __execute_start_control(self, context: dict) -> None:

        task_instance: TaskInstance = context["ti"]
        dag_name = context["task"].dag_id

        self.logger.info("Creating a database control record for the started DAG")
        dag_control_record_id = self.controller.start_dag_control(dag_name)

        self.logger.info(
            f"Sending initial Dag's shared info to XCom with key: {self.controller.xcom_key}."
            f" Info populated with control record ID: {dag_control_record_id}"
        )
        task_instance.xcom_push(self.controller.xcom_key, json.dumps(dict(dag_control_record_id=dag_control_record_id)))

    def __execute_dag_success_end_control(self, context: dict) -> None:

        task_instance: TaskInstance = context["ti"]

        self.logger.info(f"Recovering DAG's shared info from XCom with key: {self.controller.xcom_key}")
        shared_info = json.loads(task_instance.xcom_pull(self.last_task, key=self.controller.xcom_key))

        self.logger.info("Saving DAG end record on database with success status")
        self.controller.end_dag_control(
            status=DagStatus.SUCCESS,
            dag_control_id=shared_info["dag_control_record_id"],
            processed_lines=shared_info.get("processed_lines", 0),
        )

    def __execute_dag_failure_end_control(self, context: dict) -> None:

        task_instance: TaskInstance = context["ti"]

        self.logger.info(f"Recovering DAG's shared info from XCom with key: {self.controller.xcom_key}")
        shared_info = json.loads(task_instance.xcom_pull(self.last_task, key=self.controller.xcom_key))

        self.logger.info("Saving DAG end record on database with failure status")
        self.controller.end_dag_control(
            status=DagStatus.FAILED,
            dag_control_id=shared_info["dag_control_record_id"],
            processed_lines=shared_info.get("processed_lines", 0),
        )
        raise AirflowException("DAG failed: some of it`s Tasks have failed. Please verify.")
