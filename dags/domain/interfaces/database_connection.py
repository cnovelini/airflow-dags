from abc import ABC, abstractmethod
from typing import Any


class IDatabaseConnector(ABC):
    """Database connection interface."""

    @abstractmethod
    def get_connection(self) -> Any:
        """Delivers the target database connection object

        Returns:
            connection: Any
                The connection action object
        """
