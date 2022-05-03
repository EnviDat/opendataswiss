import os
import logging
import boto3

from typing import Any, NoReturn
from io import BytesIO
from botocore.config import Config


log = logging.getLogger(__name__)


def get_s3_connection() -> "boto3.resource":
    "Get an S3 resource client using S3 credentials in environment."

    log.debug(
        f"Initialise S3 resource client with endpoint: {os.getenv('AWS_ENDPOINT')}"
    )
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
        endpoint_url=os.getenv("AWS_ENDPOINT"),
        region_name=os.getenv("AWS_REGION", default=""),
        config=Config(signature_version="s3v4"),
    )

    return s3


def get_s3_bucket(
    s3: "boto3.resource", bucket_name: str = None
) -> "boto3.resource.Bucket":
    "Get an S3 bucket."

    if bucket_name is None:
        log.debug("Getting bucket name from environment variable.")
        bucket_name = os.getenv("AWS_BUCKET_NAME")

    try:
        log.debug("Getting bucket...")
        bucket = s3.Bucket(bucket_name)
    except Exception as e:
        log.error(e)
        log.error(f"Failed to access bucket named: {bucket_name}")
        bucket = None

    return bucket


def create_s3_bucket(
    s3: "boto3.resource", bucket_name: str = None, public: bool = False
) -> dict:
    """Create a new S3 bucket.

    Response Syntax:
    {
        'Location': 'string'
    }
    """

    if bucket_name is None:
        log.debug("Getting bucket name from environment variable.")
        bucket_name = os.getenv("AWS_BUCKET_NAME")

    try:
        log.debug("Creating bucket...")
        bucket = s3.create_bucket(
            ACL="public-read" if public else "private",
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "zh"},
            ObjectLockEnabledForBucket=False,
        )
        log.debug(f"Created bucket: {bucket_name}")
        return bucket
    except s3.meta.client.exceptions.BucketAlreadyExists as e:
        log.error(e)
        log.error(f"Bucket named {bucket_name} already exists. Creation failed.")
    except Exception as e:
        log.error(e)
        log.error(f"Failed to create bucket named: {bucket_name}")

    return None


def download_s3_object_to_memory(path: str, bucket: "boto3.resource.Bucket") -> BytesIO:
    """
    Download an S3 object into a binary memory object.

    To use:
    download_s3_object_to_memory(
        "index.html"
    ).read().decode("utf_8")

    """

    log.debug(f"Attempting download of key: {path} to memory.")
    file = bucket.Object(path)
    buf = BytesIO()

    try:
        file.download_fileobj(buf)
        log.info(f"Successful download: {path}")
    except Exception as e:
        log.error(e)
        log.error(f"Failed to download {path}")

    buf.seek(0)
    return buf.read().decode("utf_8")


def upload_to_s3_from_memory(
    bucket: "boto3.resource.Bucket", key: str, data: Any
) -> bool:
    "Upload memory object to S3 bucket."

    try:
        bucket.upload_fileobj(BytesIO(data), key)
        log.info(f"Successful upload: {key}")
    except Exception as e:
        log.error(e)
        log.error(f"Failed to upload {key}")
        return False

    return True


def set_s3_static_config(s3: "boto3.resource", bucket_name: str = None) -> NoReturn:
    "Add static website hosting config to an S3 bucket."

    if bucket_name is None:
        log.debug("Getting bucket name from environment variable.")
        bucket_name = os.getenv("AWS_BUCKET_NAME")

    try:
        log.debug("Setting S3 static website configuration...")
        s3.meta.client.put_bucket_website(
            Bucket=bucket_name,
            ContentMD5="",
            WebsiteConfiguration={
                "ErrorDocument": {
                    "Key": "error.html",
                },
                "IndexDocument": {
                    "Suffix": "index.html",
                },
            },
        )
        log.debug(f"Static website configured for bucket: {bucket_name}")
    except Exception as e:
        log.error(e)
        log.error(f"Failed to set static hosting on bucket named: {bucket_name}")
