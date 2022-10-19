"""Main script to execute directly."""

import logging

from envidat.metadata import get_all_metadata_record_list
from envidat.s3.bucket import Bucket
from envidat.utils import get_logger, load_dotenv_if_in_debug_mode


log = logging.getLogger(__name__)


def main():
    """For direct execution of file."""
    load_dotenv_if_in_debug_mode(env_file=".env.secret")
    get_logger()

    log.info("Starting main opendataswiss script.")

    xml_str = get_all_metadata_record_list(convert="dcat-ap", content_only=True)
    xml_name = "dcat-ap-ch.xml"

    s3_bucket = Bucket(bucket_name="opendataswiss", is_new=True, is_public=True)
    s3_bucket.put(xml_name, xml_str)

    s3_bucket.configure_static_website()
    s3_bucket.generate_index_html("EnviDat OpenDataSwiss XML", xml_name)

    log.info("Finished main opendataswiss script.")


if __name__ == "__main__":
    main()
