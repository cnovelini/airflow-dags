from datetime import datetime, timedelta
from decouple import config
from io import BytesIO
from typing import Dict
from botocore.response import StreamingBody

with open(config("CUPROD_TABLE_CLEAN_CSV_PATH"), "rb") as csv_mock_file:
    csv_mock_binary = csv_mock_file.read()

file_download_dict: Dict[str, str] = {
    "Body": StreamingBody(BytesIO(csv_mock_binary), len(csv_mock_binary)),
    "DeleteMarker": False,
    "AcceptRanges": "string",
    "Expiration": "string",
    "Restore": "string",
    "LastModified": datetime.now(),
    "ContentLength": 999,
    "ETag": "string",
    "ChecksumCRC32": "string",
    "ChecksumCRC32C": "string",
    "ChecksumSHA1": "string",
    "ChecksumSHA256": "string",
    "MissingMeta": 123,
    "VersionId": "string",
    "CacheControl": "string",
    "ContentDisposition": "string",
    "ContentEncoding": "string",
    "ContentLanguage": "string",
    "ContentRange": "string",
    "ContentType": "string",
    "Expires": datetime.now() + timedelta(days=3),
    "WebsiteRedirectLocation": "string",
    "ServerSideEncryption": "aws:kms",
    "Metadata": {"string": "string"},
    "SSECustomerAlgorithm": "string",
    "SSECustomerKeyMD5": "string",
    "SSEKMSKeyId": "string",
    "BucketKeyEnabled": True,
    "StorageClass": "STANDARD",
    "RequestCharged": "requester",
    "ReplicationStatus": "COMPLETE",
    "PartsCount": 1,
    "TagCount": 0,
    "ObjectLockMode": "COMPLIANCE",
    "ObjectLockRetainUntilDate": datetime.now(),
    "ObjectLockLegalHoldStatus": "ON",
}
