from pandas import DataFrame
from pytest import mark, raises

from domain.exceptions.runtime_exceptions import TransformationExecutionError
from domain.interfaces.information_transformation import TransformationExecutioner
from domain.interfaces.logging import ILogger
from helpers.dataframe_transformation_executioner import DataFrameTransformationExecutioner
from helpers.transformers.dataframe_string_transformer import DataFrameStringTransformer
from tests.assets.mocks.enforced_failure_transformer import EnforcedFailureTransformer


@mark.dataframe_transformation_executioner
class DataFrameTransformationExecutionerTests:
    def test_df_transf_exec_instance(self, df_transf_executioner: DataFrameTransformationExecutioner):
        assert isinstance(df_transf_executioner, TransformationExecutioner)

        assert getattr(df_transf_executioner, "known_transformers")
        assert getattr(df_transf_executioner, "logger")
        assert getattr(df_transf_executioner, "define_transformers")

        assert isinstance(df_transf_executioner.known_transformers, list)
        assert isinstance(df_transf_executioner.logger, ILogger)

    @mark.parametrize(
        "transformers_info",
        [
            dict(target_operations=["all"], transformers=[DataFrameStringTransformer]),
            dict(target_operations=["string"], transformers=[DataFrameStringTransformer]),
        ],
    )
    def test_df_transf_exec_transformers_definition(self, airflow_logger: ILogger, transformers_info: dict):

        definer = DataFrameTransformationExecutioner(airflow_logger)

        transformers = definer.define_transformers(transformers_info["target_operations"])

        assert all(
            [
                isinstance(transformer, transformer_class)
                for transformer, transformer_class in zip(transformers, transformers_info["transformers"])
            ]
        )

    def test_df_transf_exec_successful_transformation_action(
        self,
        dataframe_mock: DataFrame,
        clean_dataframe_mock: DataFrame,
        df_transf_executioner: DataFrameTransformationExecutioner,
    ):
        transformed_df = df_transf_executioner.transform(dataframe_mock)

        assert transformed_df.equals(clean_dataframe_mock)

    def test_df_transf_exec_failed_transformation_action(
        self, dataframe_mock: DataFrame, df_transf_executioner: DataFrameTransformationExecutioner
    ):
        df_transf_executioner.known_transformers.append(EnforcedFailureTransformer)

        with raises(TransformationExecutionError):
            df_transf_executioner.transform(dataframe_mock)
