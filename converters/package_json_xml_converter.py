import json
from urllib import request
import ssl
import collections
from dateutil.parser import parse

from logging import getLogger

log = getLogger(__name__)


# WARNING: Converter valid only for metadata schema of EnviDat!
def envidat_to_opendataswiss_converter(package_list_url):
    """
    Converts JSON data to XML file

    :param package_list_url: API URL that has EnviDat metadata records data in JSON format
    Package list URL: https://www.envidat.ch/api/action/current_package_list_with_resources
    testing with https://www.envidat.ch/api/action/package_show?id=d6939be3-ed78-4714-890d-d974ae2e58be
    :return: XML file in OpenDataSwiss format like this https://www.envidat.ch/opendata/export/dcat-ap-ch.xml
    """

    # TODO check which tags are mandatory

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

        # Dataset URL
        package_name = package['name']
        package_url = f'https://www.envidat.ch/#/metadata/{package_name}'
        md_metadata_dict['dcat:Dataset'] = {'@rdf:about': package_url}

        # identifier
        package_id = package['id']
        md_metadata_dict['dcat:Dataset']['dct:identifier'] = f'{package_id}@envidat'

        # title
        title = package['title']
        md_metadata_dict['dcat:Dataset']['dct:title'] = {'@xml:lang': "en", '#text': title}

        # description
        description = clean_text(package.get('notes', ''))
        md_metadata_dict['dcat:Dataset']['dct:description'] = {'@xml:lang': "en", '#text': description}

        # issued
        creation_date = package.get('metadata_created')
        if creation_date:
            md_metadata_dict['dcat:Dataset']['dct:issued'] = {
                '@rdf:datatype': "http://www.w3.org/2001/XMLSchema#dateTime",
                '#text': parse(creation_date).strftime("%Y-%m-%dT%H:%M:%SZ")
            }

        # modified
        modification_date = package.get('metadata_modified', creation_date)
        if modification_date:
            md_metadata_dict['dcat:Dataset']['dct:modified'] = {
                '@rdf:datatype': "http://www.w3.org/2001/XMLSchema#dateTime",
                '#text': parse(modification_date).strftime("%Y-%m-%dT%H:%M:%SZ")
            }

        # publication (MANDATORY)
        publisher_name = json.loads(package.get('publication', '{}')).get('publisher', '')
        md_metadata_dict['dcat:Dataset']['dct:publisher'] = {'rdf:Description': {'rdfs:label': publisher_name}}

        # TODO possibly change contact point to envidat@wsl.ch????
        # contact point (MANDATORY)
        maintainer = json.loads(package.get('maintainer', '{}'))
        maintainer_name = ""

        if maintainer.get('given_name'):
            maintainer_name += maintainer['given_name'].strip() + ' '

        maintainer_name += maintainer['name']
        maintainer_email = "mailto:" + maintainer['email']
        individual_contact_point = {'vcard:Individual': {'vcard:fn': maintainer_name,
                                                         'vcard:hasEmail': {'@rdf:resource': maintainer_email}}}

        if maintainer_email == 'mailto:envidat@wsl.ch':
            md_metadata_dict['dcat:Dataset']['dcat:contactPoint'] = [individual_contact_point]
        else:
            organization_contact_point = {'vcard:Organization': {'vcard:fn': 'EnviDat Support',
                                                                 'vcard:hasEmail': {
                                                                     '@rdf:resource': 'mailto:envidat@wsl.ch'}}}
            md_metadata_dict['dcat:Dataset']['dcat:contactPoint'] = [individual_contact_point,
                                                                     organization_contact_point]

        # theme (MANDATORY)
        md_metadata_dict['dcat:Dataset']['dcat:theme'] = {'@rdf:resource': "http://opendata.swiss/themes/education"}

        # TODO check are records really always in English?
        # language
        md_metadata_dict['dcat:Dataset']['dct:language'] = {'#text': 'en'}

        # keyword
        keywords_list = []
        keywords = get_keywords(package)
        for keyword in keywords:
            keywords_list += [{'@xml:lang': "en", '#text': keyword}]
        md_metadata_dict['dcat:Dataset']['dcat:keyword'] = keywords_list

        # landing page
        md_metadata_dict['dcat:Dataset']['dcat:landingPage'] = package_url

        # Distribution - iterate through package resources (MANDATORY) and obtain package license
        # Call get_distribution_list(package) to get distibution list
        md_metadata_dict['dcat:Dataset']['dcat:distribution'] = get_distribution_list(package, package_url)
      

# ======================================= Distribution List Function ==================================================

# Return distribution_list created from package resources list and license_id
def get_distribution_list(package, package_url):

    distribution_list = []

    dataset_license = package.get('license_id', 'odc-odbl')

    license_mapping = {'wsl-data': 'NonCommercialWithPermission-CommercialWithPermission-ReferenceRequired',
                       'odc-odbl': 'NonCommercialAllowed-CommercialAllowed-ReferenceRequired',
                       'cc-by': 'NonCommercialAllowed-CommercialAllowed-ReferenceRequired',
                       'cc-by-sa': 'NonCommercialAllowed-CommercialAllowed-ReferenceRequired',
                       'cc-zero': 'NonCommercialAllowed-CommercialAllowed-ReferenceNotRequired',
                       'CC0-1.0': 'NonCommercialAllowed-CommercialAllowed-ReferenceNotRequired',
                       }
    resource_license = license_mapping.get(dataset_license,
                                           'NonCommercialWithPermission-CommercialWithPermission-ReferenceRequired')

    for resource in package.get('resources', []):

        resource_id = resource.get('id')
        resource_name = resource.get('name', resource_id)
        resource_notes = clean_text(resource.get('description', 'No description'))
        resource_page_url = package_url + '/resource/' + resource.get('id', '')
        # resource_url = protocol + '://' + host + toolkit.url_for(controller='resource', action='read',
        #                                                          id=package.get('id', ''),
        #                                                          resource_id=resource.get('id', ''))
        # TODO check resource_url is correct
        resource_url = resource.get('url')

        resource_creation = parse(resource['created']).strftime("%Y-%m-%dT%H:%M:%SZ")
        resource_modification = resource_creation
        if resource.get('last_modified', resource.get('metadata_modified', '')):
            resource_modification = parse(resource.get('last_modified', resource.get('metadata_modified', ''))) \
                .strftime("%Y-%m-%dT%H:%M:%SZ")

        resource_size = resource.get('size', False)

        if not resource_size:
            resource_size = 0
            try:
                if len(resource.get('resource_size', '')) > 0:
                    resource_size_obj = json.loads(resource.get('resource_size', "{'size_value': '0'}"))
                    sizes_dict = {'KB': 1024, 'MB': 1048576, 'GB': 1073741824, 'TB': 1099511627776}
                    resource_size_str = resource_size_obj.get('size_value', '')
                    if len(resource_size_str) > 0:
                        resource_size = float(resource_size_obj.get('size_value', '0')) * sizes_dict[
                            resource_size_obj.get('size_units', 'KB').upper()]
            except:
                log.error('resource {0} unparseable resource_size: {1}'.format(resource_url,
                                                                               resource.get('resource_size')))
                resource_size = 0

        resource_mimetype = resource.get('mimetype', '')
        if not resource_mimetype or len(resource_mimetype) == 0:
            resource_mimetype = resource.get('mimetype_inner')

        resource_format = resource.get('format')

        distribution = {'dcat:Distribution':
                        {'@rdf:about': resource_page_url,
                         'dct:identifier': package['name'] + '.' + resource_id,
                         'dct:title': {'@xml:lang': "en", '#text': resource_name},
                         'dct:description': {'@xml:lang': "en", '#text': resource_notes},
                         'dct:issued': {'@rdf:datatype': "http://www.w3.org/2001/XMLSchema#dateTime",
                                        '#text': resource_creation},
                         'dct:modified': {'@rdf:datatype': "http://www.w3.org/2001/XMLSchema#dateTime",
                                          '#text': resource_modification},
                         'dct:language': 'en',
                         'dcat:accessURL': {'@rdf:datatype': "http://www.w3.org/2001/XMLSchema#anyURI",
                                            '#text': resource_url},
                         'dct:rights': resource_license,
                         'dcat:byteSize': resource_size
                         }
                        }
        # mediaType
        if resource_mimetype:
            distribution['dcat:Distribution']['dcat:mediaType'] = resource_mimetype

        # format
        if resource_format:
            distribution['dcat:Distribution']['dct:format'] = resource_format

        distribution_list += [distribution]

    return distribution_list


# =============================================== Helper Functions=====================================================

# Returns text cleaned of hashes and with modified characters
def clean_text(text):
    cleaned_text = text \
        .replace('###', '') \
        .replace('##', '') \
        .replace(' #', ' ') \
        .replace('# ', ' ') \
        .replace('__', '') \
        .replace('  ', ' ') \
        .replace('\r', '\n'). \
        replace('\n\n', '\n')
    return cleaned_text


# Rturns keywords from tags in package (metadata record)
def get_keywords(package):
    keywords = []
    for tag in package.get('tags', []):
        name = tag.get('display_name', '').upper()
        keywords += [name]
    return keywords


# ========================================== TESTING ===========================================================

# envidat_to_opendataswiss_converter("https://www.envidat.ch/api/action/package_show?id=d6939be3-ed78-4714-890d-d974ae2e58be")
envidat_to_opendataswiss_converter("https://www.envidat.ch/api/action/current_package_list_with_resources?limit=2")
