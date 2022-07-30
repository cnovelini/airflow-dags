from typing import List

from pandas import DataFrame

from domain.interfaces.information_transformation import ITransformer
from domain.interfaces.logging import ILogger


class EnforcedFailureTransformer(ITransformer):

    transformation_scopes: List[str] = ["string"]

    def __init__(self, logger: ILogger) -> None:
        super().__init__(logger)
        self.transformations = []

    def transform(self, information: DataFrame) -> None:
        raise ValueError("Failed to transform information!")
