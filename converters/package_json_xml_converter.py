import json


# Converter valid only for metadata schema of EnviDat
def envidat_to_opendataswiss_converter(metadata_record):
    """
    Converts JSON data to XML fil

    :param metadata_record: JSON data from EnviDat metadata record from
    https://www.envidat.ch/api/action/current_package_list_with_resources
    :return: XML file in OpenDataSwiss format like this https://www.envidat.ch/opendata/export/dcat-ap-ch.xml
    """

    # TODO try using call to resource_show

    with open(metadata_record) as json_format_file:
        # data = json.load(json_format_file)
        # print(data)
        pass


# ========================================== TESTING ===========================================================

envidat_to_opendataswiss_converter("test_record.json")
