from decouple import config
from io import BytesIO
from typing import Dict
from botocore.response import StreamingBody

with open(config("CUPROD_TABLE_CLEAN_CSV_PATH"), "rb") as csv_mock_file:
    csv_mock_binary = csv_mock_file.read()

with open(config("CUPROD_TABLE_BROKEN_CSV_PATH"), "rb") as csv_mock_file:
    broken_csv_mock_binary = csv_mock_file.read()

file_download_dict: Dict[str, str] = {"Body": StreamingBody(BytesIO(csv_mock_binary), len(csv_mock_binary))}

broken_file_download_dict: Dict[str, str] = {
    "Body": StreamingBody(BytesIO(broken_csv_mock_binary), len(broken_csv_mock_binary))
}
