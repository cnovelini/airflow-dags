from pandas import DataFrame
from pytest import mark, raises
from pytest_mock import MockerFixture

from domain.exceptions.runtime_exceptions import DataFrameStringStripError
from domain.interfaces.information_transformation import ITransformer
from helpers.transformers.dataframe_string_transformer import DataFrameStringTransformer
from tests.assets.patchers.pandas_dataframe_patcher import PandasDataframePatcher


@mark.dataframe_string_transformer
class DataFrameStringTransformerTests:
    def test_dataframe_string_transformer_instance(self, df_string_transformer: DataFrameStringTransformer):

        assert isinstance(df_string_transformer, ITransformer)

        assert getattr(df_string_transformer, "transformation_scopes")
        assert getattr(df_string_transformer, "transformations")
        assert getattr(df_string_transformer, "transform")
        assert getattr(df_string_transformer, "apply_strip")

        assert isinstance(df_string_transformer.transformation_scopes, list)
        assert df_string_transformer.transformation_scopes == ["string"]
        assert isinstance(df_string_transformer.transformations, list)
        assert df_string_transformer.transformations == [df_string_transformer.apply_strip]

    def test_dataframe_string_transformer_transformation(
        self,
        df_string_transformer: DataFrameStringTransformer,
        dataframe_mock: DataFrame,
        clean_dataframe_mock: DataFrame,
    ):
        dataframe_mock = df_string_transformer.transform(dataframe_mock)
        assert dataframe_mock.equals(clean_dataframe_mock)

    def test_dataframe_string_transformer_transformation_error(
        self, mocker: MockerFixture, df_string_transformer: DataFrameStringTransformer, dataframe_mock: DataFrame
    ):

        patcher = PandasDataframePatcher(mocker)
        patcher.patch_method_side_effect("applymap", ValueError("Failed to parse"))

        with raises(DataFrameStringStripError):
            df_string_transformer.transform(dataframe_mock)
