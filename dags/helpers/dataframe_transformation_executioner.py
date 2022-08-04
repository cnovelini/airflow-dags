from logging import Logger
from typing import List

from domain.interfaces.information_transformation import ITransformer, TransformationExecutioner
from helpers.transformers.dataframe_string_transformer import DataFrameStringTransformer
from helpers.transformers.razac_shipdate_transformer import RazacShipdateTransformer


class DataFrameTransformationExecutioner(TransformationExecutioner):
    def __init__(self, logger: Logger, target_operations: List[str] = None) -> None:
        super().__init__(logger, target_operations)

        self.known_transformers = [DataFrameStringTransformer, RazacShipdateTransformer]

    def define_transformers(self, target_operations: List[str]) -> List[ITransformer]:
        self.logger.info(f"Defining all eligible transformers for {target_operations} operation types")
        return [
            transformer(self.logger)
            for transformer in self.known_transformers
            if (
                "all" in target_operations
                or any([operation_type in transformer.transformation_scopes for operation_type in target_operations])
            )
        ]
