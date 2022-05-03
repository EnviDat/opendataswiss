import os
import sys
import logging
import requests

from io import BytesIO
from textwrap import dedent

# from utils.s3 import (
#     get_s3_connection,
#     create_s3_bucket,
#     set_s3_static_config,
#     upload_to_s3_from_memory,
# )


log = logging.getLogger(__name__)


def _get_url(url: str) -> requests.Response:
    "Helper wrapper to get a URL with additional error handling."

    try:
        log.debug(f"Attempting to get {url}")
        r = requests.get(url)
        r.raise_for_status()
        return r
    except requests.exceptions.ConnectionError as e:
        log.error(f"Could not connect to internet on get: {r.request.url}")
        log.error(e)
    except requests.exceptions.HTTPError as e:
        log.error(f"HTTP response error on get: {r.request.url}")
        log.error(e)
    except requests.exceptions.RequestException as e:
        log.error(f"Request error on get: {r.request.url}")
        log.error(f"Request: {e.request}")
        log.error(f"Response: {e.response}")
    except Exception as e:
        log.error(e)
        log.error(f"Unhandled exception occured on get: {r.request.url}")

    return None


def get_logger() -> logging.basicConfig:
    "Set logger parameters with log level from environment."

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", default="DEBUG"),
        format=(
            "%(asctime)s.%(msecs)03d [%(levelname)s] "
            "%(name)s | %(funcName)s:%(lineno)d | %(message)s"
        ),
        datefmt="%y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


def get_package_list_from_ckan(host: str = None) -> list:
    "Get package list from CKAN API, with given host url."

    if host is None:
        host = os.getenv("CKAN_HOST", default="https://www.envidat.ch")

    log.info(f"Getting package list from {host}.")
    try:
        package_names = _get_url(f"{host}/api/3/action/package_list").json()
    except AttributeError as e:
        log.error(e)
        log.error(f"Getting package names from CKAN failed. Returned: {package_names}")
        raise AttributeError("Failed to extract package names as JSON.")

    log.debug("Extracting [result] key from JSON.")
    package_names = list(package_names["result"])

    return package_names


def get_xml_from_ckan(package_name: str, host: str = None) -> bool:
    "Get XML for a CKAN package."

    if host is None:
        host = os.getenv("CKAN_HOST", default="https://www.envidat.ch")
    # url = f"{host}/dataset/{package_name}/export/gcmd_dif.xml"
    # log.debug(f"CKAN url to download: {url}")
    # data = _get_url(url).content
    # TODO Rebeccas code to generate XML


def generate_index_html(package_names: list) -> BytesIO:
    "Write index.html to root of S3 bucket, with embedded S3 download links."

    buf = BytesIO()

    # Start HTML
    html_block = dedent(
        """
        <html>
        <head>
        <meta charset="utf-8">
        <title>EnviDat Metadata List</title>
        </head>
        <body>
        """
    ).strip()
    log.debug(f"Writing start HTML block to buffer: {html_block}")
    buf.write(html_block.encode("utf_8"))

    # Packages
    log.info("Iterating package list to write S3 links to index.")
    for package_name in package_names:
        log.debug(f"Package name: {package_name}")
        html_block = dedent(
            f"""
            <div class='flex py-2 xs6'>
            <a href='https://opendataswiss.s3-zh.os.switch.ch/{package_name}.xml'>
                https://opendataswiss.s3-zh.os.switch.ch/{package_name}.xml
            </a>
            </div>"""
        )
        log.debug(f"Writing package link HTML to buffer: {html_block}")
        buf.write(html_block.encode("utf_8"))

    # Close
    html_block = dedent(
        """
        </body>
        </html>"""
    )
    log.debug(f"Writing end HTML block to buffer: {html_block}")
    buf.write(html_block.encode("utf_8"))

    # Reset read pointer.
    # DOT NOT FORGET THIS, for reading afterwards!
    buf.seek(0)

    return buf


def main():
    "Main script logic."

    get_logger()

    # s3_client = get_s3_connection()
    # bucket = create_s3_bucket(s3_client, public=True)
    # set_s3_static_config(s3_client)

    # log.debug(f"Attempting upload of {package_name}.xml to S3 bucket.")
    # upload_status = upload_to_s3_from_memory(bucket, f"{package_name}.xml", data)

    # # Create index.html
    # index_html = generate_index_html(packages_in_ckan)
    # log.info("Uploading generated index.html to S3 bucket.")
    # bucket.upload_fileobj(
    #     index_html, "index.html", ExtraArgs={"ContentType": "text/html"}
    # )

    log.info("Done.")


if __name__ == "__main__":
    main()
