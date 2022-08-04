from logging import Logger
from pandas import DataFrame

from domain.data.razac.tables.shipdate.transformation_column_map import transformation_column_map
from domain.data.razac.tables.shipdate.shipdate_types import razac_shipdate_types
from domain.interfaces.information_transformation import ITransformer


class RazacShipdateTransformer(ITransformer):

    transformation_scopes = ["razac_shipdate"]

    def __init__(self, logger: Logger) -> None:
        super().__init__(logger)
        self.transformations = [self.rename_columns, self.change_column_types]

    def transform(self, information: DataFrame) -> DataFrame:
        self.logger.info("Applying all DataFrame String transformations...")
        for transformation in self.transformations:
            information = transformation(information)

        self.logger.info("All transformations applied with success!")
        return information

    def rename_columns(self, information: DataFrame) -> DataFrame:
        self.logger.info("Renaming DataFrame columns names")
        return information.rename(columns=transformation_column_map)

    def change_column_types(self, information: DataFrame) -> DataFrame:
        self.logger.info("Adjusting columns corrupted by NAN values")
        for column_name, new_type in razac_shipdate_types.items():
            self.logger.info(f"Transforming column {column_name} into {new_type}")
            if new_type == "safe_int":
                information = self.__apply_safe_integer_protection(information, column_name)
            else:
                information[column_name] = information[column_name].astype(new_type)

        return information

    def __apply_safe_integer_protection(self, information: DataFrame, column_name: str) -> DataFrame:
        information = information.apply(
            lambda x: str(x).replace(".0", "") if x.name == column_name and x is not None else x
        )
        return information
