from typing import Iterator

from pandas import DataFrame
from pytest import fixture

from tests.assets.mocks.clean_dataframe_information_mock import clean_dataframe_information_list
from tests.assets.mocks.dataframe_information_mock import dataframe_information_list
from tests.assets.mocks.file_download_mock import file_download_dict, broken_file_download_dict
from tests.assets.mocks.file_upload_mock import file_upload_dict


@fixture
def file_upload_mock() -> Iterator[dict]:
    yield file_upload_dict


@fixture
def file_download_mock() -> Iterator[dict]:
    yield file_download_dict


@fixture
def broken_file_download_mock() -> Iterator[dict]:
    yield broken_file_download_dict


@fixture
def dataframe_information() -> Iterator[list]:
    yield dataframe_information_list


@fixture
def clean_dataframe_information() -> Iterator[list]:
    yield clean_dataframe_information_list


@fixture
def dataframe_mock(dataframe_information: list) -> Iterator[DataFrame]:
    yield DataFrame(dataframe_information)


@fixture
def clean_dataframe_mock(clean_dataframe_information: list) -> Iterator[DataFrame]:
    yield DataFrame(clean_dataframe_information)
