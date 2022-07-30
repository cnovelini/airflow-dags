from typing import Any
from pandas import DataFrame
from pytest_mock import MockerFixture


class PandasDataframePatcher:
    def __init__(self, mocker: MockerFixture) -> None:
        self.mocker = mocker

    def patch_method_return_value(self, method_name: str, return_value: Any):
        """Patch Pandas DataFrame class method with any return value."""
        self.mocker.patch.object(DataFrame, method_name, return_value=return_value)

    def patch_method_side_effect(self, method_name: str, side_effect: Any):
        """Patch Pandas DataFrame class method with any return value."""
        self.mocker.patch.object(DataFrame, method_name, side_effect=side_effect)
