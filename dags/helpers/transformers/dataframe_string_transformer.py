from logging import Logger
from pandas import DataFrame

from domain.exceptions.runtime_exceptions import DataFrameStringStripError
from domain.interfaces.information_transformation import ITransformer


class DataFrameStringTransformer(ITransformer):

    transformation_scopes = ["string"]

    def __init__(self, logger: Logger) -> None:
        super().__init__(logger)
        self.transformations = [self.apply_strip]

    def transform(self, information: DataFrame) -> DataFrame:
        self.logger.info("Applying all DataFrame String transformations...")
        for transformation in self.transformations:
            information = transformation(information)

        self.logger.info("All transformations applied with success!")
        return information

    def apply_strip(self, information: DataFrame) -> DataFrame:
        """Executes strip() method on all string cells.

        Parameters:
            information: (pandas.DataFrame)
                The information in which strip() will be applied

        Returns:
            information: (pandas.DataFrame)
                the transformed DataFrame

        Raises:
            DataFrameStringStripError: Raised when strip lambda fails
        """
        self.logger.info("Executing strip() method on all DataFrame cells...")
        try:
            information = information.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        except Exception as strip_err:
            error_message = f"{type(strip_err).__name__} -> {strip_err}"
            self.logger.error(f"Error in DF string strip transformation: {error_message}")
            raise DataFrameStringStripError(error_message)

        return information
