from abc import ABC, abstractmethod
from typing import List

from pandas import DataFrame

from domain.exceptions.runtime_exceptions import TransformationExecutionError
from domain.interfaces.logging import ILogger


class ITransformer(ABC):
    """Data transformation executioner interface."""

    transformation_scopes: List[str] = []
    logger: ILogger

    def __init__(self, logger: ILogger) -> None:
        self.logger = logger

    @abstractmethod
    def transform(self, information: DataFrame) -> DataFrame:
        """Transformation execution routine

        Parameters:
            information: (pandas.DataFrame)
                The information to be transformed
        """


class TransformationExecutioner(ABC):
    """Data transformation execution chain interface."""

    def __init__(self, logger: ILogger, target_operations: List[str] = None) -> None:
        self.logger = logger
        self.target_operations = target_operations or ["all"]

    def transform(self, information: DataFrame, target_operations: List[str] = None) -> DataFrame:
        """Executes a chain of transformation routines, according to target_operations flag.

        Parameters:
            information: (pandas.DataFrame)
                The information to be transformed

            target_operations: (List[str])
                A list of operation flags to be executed on information

        Returns:
            information: (pandas.DataFrame)
                The transformed information

        Raises:
            TransformationExecutionError: Raised if any transformation routine fails
        """

        # Define all target transformation operators to be executed
        transformers_to_execute = self.define_transformers(target_operations or self.target_operations)
        self.logger.info(f"Transformers to be executed: {transformers_to_execute}")

        # Executing all transformation routines
        try:
            for transformer in transformers_to_execute:
                information = transformer.transform(information)
        except Exception as trans_err:
            error_message = f"{type(trans_err).__name__} -> {trans_err}"
            self.logger.error(f"Error in transformers execution: {error_message}")
            raise TransformationExecutionError(error_message)

        # Returning transformed information
        self.logger.info("All transformations executed with success!")
        return information

    @abstractmethod
    def define_transformers(self, target_operations: List[str]) -> List[ITransformer]:
        """Defines which transformers shall be executed on target information.

        Parameters:
            target_operations: (List[str])
                A list of operation flags to define which transformers should be chosen

        Returns:
            transformers_to_execute: (List[ITransformer])
                A list of transformers to be applied on target information
        """
