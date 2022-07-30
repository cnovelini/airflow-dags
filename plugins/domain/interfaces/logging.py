from abc import ABC, abstractmethod


class ILogger(ABC):
    """Project logging interface."""

    @abstractmethod
    def debug(self, message: str) -> None:
        """Executes DEBUG log.

        Parameters:
            message: (str)
                The message to be logged
        """

    @abstractmethod
    def info(self, message: str) -> None:
        """Executes INFO log.

        Parameters:
            message: (str)
                The message to be logged
        """

    @abstractmethod
    def warning(self, message: str) -> None:
        """Executes WARNING log.

        Parameters:
            message: (str)
                The message to be logged
        """

    @abstractmethod
    def error(self, message: str) -> None:
        """Executes ERROR log.

        Parameters:
            message: (str)
                The message to be logged
        """
