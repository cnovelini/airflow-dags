from typing import List
from domain.exceptions.runtime_exceptions import RuntimeError


class UnknownControlActionError(RuntimeError):
    def __init__(self, action: str, known_actions: List[str]) -> None:
        message = (
            f"Invalid action informed to SkfController: {action}. " f"Current actions are: [{', '.join(known_actions)}]"
        )
        super().__init__(message)


class DagControlRecordNotFoundError(RuntimeError):
    def __init__(self, control_id: int) -> None:
        message = f"DAG control record not found for ID: {control_id}."
        super().__init__(message)


class TaskControlRecordNotFoundError(RuntimeError):
    def __init__(self, control_id: int) -> None:
        message = f"TASK control record not found for ID: {control_id}."
        super().__init__(message)
