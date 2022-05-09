import json
from urllib import request
import ssl
import collections

# WARNING: Converter valid only for metadata schema of EnviDat!
def envidat_to_opendataswiss_converter(package_list_url):
    """
    Converts JSON data to XML file

    :param package_list_url: API URL that has EnviDat metadata records data in JSON format
    Package list URL: https://www.envidat.ch/api/action/current_package_list_with_resources
    testing with https://www.envidat.ch/api/action/package_show?id=d6939be3-ed78-4714-890d-d974ae2e58be
    :return: XML file in OpenDataSwiss format like this https://www.envidat.ch/opendata/export/dcat-ap-ch.xml
    """

    # TODO fix SSL certificates issue
    ssl._create_default_https_context = ssl._create_unverified_context

    # Assign package_list API JSON data to Python dictionary
    with request.urlopen(package_list_url) as metadata:
        package_list = json.load(metadata)

    # Convert each record in package_list to XML format
    for package in package_list['result']:

        md_metadata_dict = collections.OrderedDict()

        # Header
        md_metadata_dict['@xmlns:dct'] = "http://purl.org/dc/terms/"
        md_metadata_dict['@xmlns:dc'] = "http://purl.org/dc/elements/1.1/"
        md_metadata_dict['@xmlns:dcat'] = "http://www.w3.org/ns/dcat#"
        md_metadata_dict['@xmlns:foaf'] = "http://xmlns.com/foaf/0.1/"
        md_metadata_dict['@xmlns:xsd'] = "http://www.w3.org/2001/XMLSchema#"
        md_metadata_dict['@xmlns:rdfs'] = "http://www.w3.org/2000/01/rdf-schema#"
        md_metadata_dict['@xmlns:rdf'] = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        md_metadata_dict['@xmlns:vcard'] = "http://www.w3.org/2006/vcard/ns#"
        md_metadata_dict['@xmlns:odrs'] = "http://schema.theodi.org/odrs#"
        md_metadata_dict['@xmlns:schema'] = "http://schema.org/"

        # URL
        package_name = package['name']
        md_metadata_dict['dcat:Dataset'] = {'@rdf:about': f'https://www.envidat.ch/#/metadata/{package_name}'}

        # ID
        package_id = package['id']
        md_metadata_dict['dcat:Dataset']['dct:identifier'] = f'{package_id}@envidat'
  


# ========================================== TESTING ===========================================================

# envidat_to_opendataswiss_converter("https://www.envidat.ch/api/action/package_show?id=d6939be3-ed78-4714-890d-d974ae2e58be")
envidat_to_opendataswiss_converter("https://www.envidat.ch/api/action/current_package_list_with_resources?limit=2")
