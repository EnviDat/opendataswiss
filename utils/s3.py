import os
import logging
import json
import boto3

from typing import Any, NoReturn, Union
from io import BytesIO
from textwrap import dedent
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
    bucket: "boto3.resource.Bucket", key: str, data: Any, content_type: str = None
) -> bool:
    "Upload memory object to S3 bucket."

    buf = BytesIO()
    log.debug("Writing memory object buffer.")
    buf.write(data.encode("utf_8"))
    buf.seek(0)

    try:
        extra_args = {"ContentType": content_type} if content_type else None
        bucket.upload_fileobj(buf, key, ExtraArgs=extra_args)
        log.info(f"Successful upload: {key}")
    except Exception as e:
        log.error(e)
        log.error(f"Failed to upload {key}")
        return False

    return True


def set_s3_static_config(s3: "boto3.resource", bucket_name: str = None) -> NoReturn:
    """
    Add static website hosting config to an S3 bucket.

    Note: WARNING this will set all data to public read policy.
    """

    if bucket_name is None:
        log.debug("Getting bucket name from environment variable.")
        bucket_name = os.getenv("AWS_BUCKET_NAME")

    try:
        log.debug("Setting public read access policy for static website.")
        public_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicRead",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket_name}/*",
                }
            ],
        }
        bucket_policy = json.dumps(public_policy)
        s3.meta.client.put_bucket_policy(Bucket=bucket_name, Policy=bucket_policy)

        log.debug("Setting S3 static website configuration...")
        s3.meta.client.put_bucket_website(
            Bucket=bucket_name,
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


def generate_index_html(
    title: str, file_list: Union[list, str], bucket_name: str = None
) -> BytesIO:
    "Write index.html to root of S3 bucket, with embedded S3 download links."

    if bucket_name is None:
        log.debug("Getting bucket name from environment variable.")
        bucket_name = os.getenv("AWS_BUCKET_NAME")

    if isinstance(file_list, str):
        file_list = [file_list]

    buf = BytesIO()

    # Start HTML
    html_block = dedent(
        f"""
        <html>
        <head>
        <meta charset="utf-8">
        <title>{title}</title>
        </head>
        <body>
        """
    ).strip()
    log.debug(f"Writing start HTML block to buffer: {html_block}")
    buf.write(html_block.encode("utf_8"))

    # Files
    log.info("Iterating file list to write S3 links to index.")
    for file_name in file_list:
        log.debug(f"File name: {file_name}")
        html_block = dedent(
            f"""
            <div class='flex py-2 xs6'>
            <a href='https://{bucket_name}.s3-zh.os.switch.ch/{file_name}'>
                https://{bucket_name}.s3-zh.os.switch.ch/{file_name}
            </a>
            </div>"""
        )
        log.debug(f"Writing file link HTML to buffer: {html_block}")
        buf.write(html_block.encode("utf_8"))

    # Close
    html_block = dedent(
        """
        </body>
        </html>"""
    )
    log.debug(f"Writing end HTML block to buffer: {html_block}")
    buf.write(html_block.encode("utf_8"))

    buf.seek(0)

    return buf.read().decode("utf_8")
