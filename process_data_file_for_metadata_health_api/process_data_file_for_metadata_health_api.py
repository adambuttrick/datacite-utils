import sys
import json
import gzip
import logging
import argparse
import orjson
import requests
from pathlib import Path
from datetime import datetime
from multiprocessing import Pool, cpu_count, Manager


class ArgumentConfig:
    def __init__(self):
        self.input_dir = None
        self.output_dir = None
        self.cache_dir = None
        self.log_level = None

    @classmethod
    def parse_arguments(cls):
        parser = argparse.ArgumentParser(
            description='Process DataCite metadata files'
        )

        parser.add_argument('-i', '--input-dir', required=True, help='Directory containing DataCite data files (recursive search for jsonl.gz)')
        parser.add_argument('-o', '--output-dir', required=True, help='Output directory for JSON files')
        parser.add_argument('-c', '--cache-dir', help='Directory for caching API responses')
        parser.add_argument('-l', '--log-level', default='INFO', help='Logging level')

        args = parser.parse_args()

        config = cls()
        config.input_dir = args.input_dir
        config.output_dir = args.output_dir
        config.cache_dir = args.cache_dir
        config.log_level = args.log_level

        return config


class LoggerSetup:
    """Configures application logging."""

    LOGGER_NAME = 'datacite'

    @classmethod
    def configure(cls, level_name):
        level = getattr(logging, level_name.upper(), logging.INFO)
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        logger = logging.getLogger(cls.LOGGER_NAME)
        logger.setLevel(level)

        return logger


class DataCiteAPIClient:
    """Client for retrieving providers and clients metadata from DataCite API."""

    def __init__(self, cache_dir=None):
        """Initialize the DataCite API client.

        Args:
            cache_dir (str, optional): Directory path for caching API responses.
                If provided, responses will be cached to and loaded from this directory.
        """
        self.base_url = 'https://api.datacite.org'
        self.cache_dir = Path(cache_dir) if cache_dir else None

        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_providers(self):
        return self.get_all_pages('providers')

    def get_clients(self):
        return self.get_all_pages('clients')

    def get_all_pages(self, endpoint, page_size=1000):
        if self.cache_dir:
            cache_file = self.cache_dir / f"{endpoint}.json"
            if cache_file.exists():
                logging.info(f"Loading cached {endpoint} data")
                with open(cache_file, 'r') as f:
                    return json.load(f)

        logging.info(f"Fetching {endpoint} data from API")
        all_items = []
        page = 1
        total_pages = 1

        while page <= total_pages:
            url = f"{self.base_url}/{endpoint}"
            params = {
                'page[size]': page_size,
                'page[number]': page,
                'include': 'prefixes'
            }

            response = requests.get(url, params=params)
            if response.status_code != 200:
                raise Exception(f"API request failed: {response.status_code}")

            data = response.json()
            all_items.extend(data.get('data', []))

            meta = data.get('meta', {})
            total_pages = meta.get('totalPages', 1)
            total = meta.get('total', 0)

            logging.info(
                f"Fetched page {page} of {total_pages} for {endpoint} "
                f"({len(all_items)}/{total} items)"
            )
            page += 1

        if self.cache_dir:
            with open(self.cache_dir / f"{endpoint}.json", 'w') as f:
                json.dump(all_items, f)

        return all_items

    def _get_single_page(self, url, params):
        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise Exception(f"API request failed: {response.status_code}")
        return response.json()


class StatsContainer:
    """Container class for metadata statistics."""

    FIELD_STATUS = {
        'identifier': 'mandatory', 'creators': 'mandatory', 'titles': 'mandatory',
        'publisher': 'mandatory', 'publicationYear': 'mandatory', 'resourceType': 'mandatory',
        'subjects': 'recommended', 'contributors': 'recommended', 'date': 'recommended',
        'relatedIdentifiers': 'recommended', 'description': 'recommended', 'geoLocations': 'recommended',
        'language': 'optional', 'alternateIdentifiers': 'optional', 'sizes': 'optional',
        'formats': 'optional', 'version': 'optional', 'rights': 'optional',
        'fundingReferences': 'optional', 'relatedItems': 'optional'
    }

    SUBFIELD_STATS = {
        'creators': {
            'multiple': True,
            'nameType': {'Personal', 'Organizational'},
            'nameIdentifier': {'scheme': False},
            'nameIdentifierScheme': {'ORCID', 'ROR', 'ISNI'},
            'affiliation': {'scheme': False},
            'affiliationIdentifier': {'scheme': False},
            'affiliationIdentifierScheme': {'ROR', 'GRID', 'ISNI'}
        },
        'contributors': {
            'multiple': True,
            'contributorType': {'ContactPerson', 'DataCollector', 'DataCurator', 'DataManager',
                                'Distributor', 'Editor', 'HostingInstitution', 'Producer',
                                'ProjectLeader', 'ProjectManager', 'ProjectMember', 'RegistrationAgency',
                                'RegistrationAuthority', 'RelatedPerson', 'Researcher', 'ResearchGroup',
                                'RightsHolder', 'Sponsor', 'Supervisor', 'WorkPackageLeader', 'Other'},
            'nameIdentifier': {'scheme': False},
            'nameIdentifierScheme': {'ORCID', 'ROR', 'ISNI'},
            'affiliation': {'scheme': False},
            'affiliationIdentifier': {'scheme': False},
            'affiliationIdentifierScheme': {'ROR', 'GRID', 'ISNI'}
        },
        'resourceType': {
            'multiple': False,
            'resourceTypeGeneral': {'Audiovisual', 'Book', 'BookChapter', 'Collection',
                                    'ComputationalNotebook', 'ConferencePaper', 'ConferenceProceeding',
                                    'Dataset', 'Dissertation', 'Event', 'Image', 'InteractiveResource',
                                    'Journal', 'JournalArticle', 'Model', 'OutputManagementPlan',
                                    'PeerReview', 'PhysicalObject', 'Preprint', 'Report', 'Service',
                                    'Software', 'Sound', 'Standard', 'Text', 'Workflow', 'Other',
                                    'Unknown'}
        },
        'relatedIdentifiers': {
            'multiple': True,
            'relationType': {'IsCitedBy', 'Cites', 'IsSupplementTo', 'IsSupplementedBy',
                             'IsContinuedBy', 'Continues', 'IsDescribedBy', 'Describes',
                             'HasMetadata', 'IsMetadataFor', 'HasVersion', 'IsVersionOf',
                             'IsNewVersionOf', 'IsPreviousVersionOf', 'IsPartOf', 'HasPart',
                             'IsPublishedIn', 'IsReferencedBy', 'References', 'IsDocumentedBy',
                             'Documents', 'IsCompiledBy', 'Compiles', 'IsVariantFormOf',
                             'IsOriginalFormOf', 'IsIdenticalTo', 'IsReviewedBy', 'Reviews',
                             'IsDerivedFrom', 'IsSourceOf', 'Requires', 'IsRequiredBy',
                             'Obsoletes', 'IsObsoletedBy'},
            'relatedIdentifierType': {'ARK', 'arXiv', 'bibcode', 'DOI', 'EAN13', 'EISSN',
                                      'Handle', 'IGSN', 'ISBN', 'ISSN', 'ISTC', 'LISSN',
                                      'LSID', 'PMID', 'PURL', 'UPC', 'URL', 'URN', 'w3id'},
            'resourceTypeGeneral': {'Audiovisual', 'Book', 'BookChapter', 'Collection',
                                    'ComputationalNotebook', 'ConferencePaper', 'ConferenceProceeding',
                                    'Dataset', 'Dissertation', 'Event', 'Image', 'InteractiveResource',
                                    'Journal', 'JournalArticle', 'Model', 'OutputManagementPlan',
                                    'PeerReview', 'PhysicalObject', 'Preprint', 'Report', 'Service',
                                    'Software', 'Sound', 'Standard', 'Text', 'Workflow', 'Other',
                                    'Unknown'}
        },
        'fundingReferences': {
            'multiple': True,
            'funderName': {'scheme': False},
            'funderIdentifier': {'scheme': False},
            'funderIdentifierType': {'Crossref Funder ID', 'ROR', 'Other'},
            'awardNumber': {'scheme': False},
            'awardURI': {'scheme': False},
            'awardTitle': {'scheme': False}
        }
    }

    def __init__(self):
        self.stats = self.create_empty_stats()

    @classmethod
    def create_empty_field_stats(cls, field_name, field_status):
        """Create empty stats structure for a field.

        Args:
            field_name (str): Name of the field
            field_status (str): Status of the field (mandatory/recommended/optional)

        Returns:
            dict: Empty stats structure for the field
        """
        stats = {
            'count': 0,
            'instances': 0,
            'fieldStatus': field_status,
            'completeness': 0.0,
            'missing': 0
        }

        if field_name in cls.SUBFIELD_STATS:
            subfields = cls.SUBFIELD_STATS[field_name]
            stats['subfields'] = {}

            for subfield, values in subfields.items():
                if subfield == 'multiple':
                    continue

                if isinstance(values, set):
                    stats['subfields'][subfield] = {
                        'count': 0,
                        'instances': 0,
                        'missing': 0,
                        'completeness': 0.0,
                        'values': {val: 0 for val in values}
                    }
                elif isinstance(values, dict):
                    base_stats = {
                        'count': 0,
                        'instances': 0,
                        'missing': 0,
                        'completeness': 0.0
                    }

                    if values.get('scheme'):
                        scheme_field = f'{subfield}Scheme'
                        if scheme_field in subfields:
                            base_stats['values'] = {
                                val: 0 for val in subfields[scheme_field]}

                    stats['subfields'][subfield] = base_stats

        return stats

    @classmethod
    def create_empty_stats(cls):
        stats = {
            'stats': {
                'summary': {
                    'count': 0,
                    'fields': {},
                    'categories': {
                        'mandatory': {'completeness': 0.0},
                        'recommended': {'completeness': 0.0},
                        'optional': {'completeness': 0.0}
                    }
                },
                'byResourceType': {
                    'resourceTypes': {}
                }
            }
        }

        for field_name, field_status in cls.FIELD_STATUS.items():
            stats['stats']['summary']['fields'][field_name] = \
                cls.create_empty_field_stats(field_name, field_status)

        resource_types = list(
            cls.SUBFIELD_STATS['resourceType']['resourceTypeGeneral'])
        for resource_type in resource_types:
            stats['stats']['byResourceType']['resourceTypes'][resource_type] = {
                'count': 0,
                'categories': {
                    'mandatory': {'completeness': 0.0},
                    'recommended': {'completeness': 0.0},
                    'optional': {'completeness': 0.0}
                },
                'fields': {}
            }

            for field_name, field_status in cls.FIELD_STATUS.items():
                stats['stats']['byResourceType']['resourceTypes'][resource_type]['fields'][field_name] = \
                    cls.create_empty_field_stats(field_name, field_status)

        return stats

    def remove_zero_count_resource_types_and_clean(self, stats_obj=None):
        """Remove resource types with zero counts and clean up the stats structure.

        Args:
            stats_obj (dict, optional): Stats object to clean. If None, cleans internal stats.

        Returns:
            dict: Cleaned stats object if stats_obj was provided, None otherwise
        """
        target_stats = stats_obj if stats_obj is not None else self.stats['stats']

        if 'byResourceType' in target_stats and 'resourceTypes' in target_stats['byResourceType']:
            target_stats['byResourceType']['resourceTypes'] = {
                rt: data for rt, data in target_stats['byResourceType']['resourceTypes'].items()
                if data['count'] > 0
            }

        def clean_subfields(fields_dict):
            for field_data in fields_dict.values():
                if 'subfields' in field_data:
                    for subfield_data in field_data['subfields'].values():
                        if 'values' in subfield_data:
                            subfield_data['values'] = {
                                value: count for value, count in subfield_data['values'].items()
                                if count > 0
                            }

        if 'summary' in target_stats and 'fields' in target_stats['summary']:
            clean_subfields(target_stats['summary']['fields'])

        for rt_data in target_stats['byResourceType']['resourceTypes'].values():
            if 'fields' in rt_data:
                clean_subfields(rt_data['fields'])

        self._round_completeness_values(target_stats)

        return stats_obj if stats_obj is not None else None

    def _round_completeness_values(self, stats_obj):
        if isinstance(stats_obj, dict):
            for key, value in stats_obj.items():
                if key == 'completeness' and isinstance(value, float):
                    stats_obj[key] = round(value, 4)
                elif isinstance(value, (dict, list)):
                    self._round_completeness_values(value)
        elif isinstance(stats_obj, list):
            for item in stats_obj:
                if isinstance(item, (dict, list)):
                    self._round_completeness_values(item)

    def get_stats(self):
        return self.stats


class StatsUpdater:
    """Class for updating and merging stats."""

    def __init__(self, container):
        self.container = container

    def update_subfield_stats(self, field_value, field_name, stats, stat_key='all'):
        """Update subfield statistics for a given field value.

        Args:
            field_value: The value of the field to analyze
            field_name: Name of the field being analyzed
            stats: Statistics object to update
            stat_key: Key for specific stats section ('all' or resource type)
        """
        if not field_value or field_name not in self.container.SUBFIELD_STATS:
            return

        field_stats = stats['stats']['summary']['fields'][field_name] if stat_key == 'all' else stats[
            'stats']['byResourceType']['resourceTypes'][stat_key]['fields'][field_name]
        field_config = self.container.SUBFIELD_STATS[field_name]
        total_dois = stats['stats']['summary']['count'] if stat_key == 'all' else stats[
            'stats']['byResourceType']['resourceTypes'][stat_key]['count']

        if isinstance(field_value, list):
            field_stats['instances'] += len(field_value)
            seen_subfields = set()

            for item in field_value:
                for subfield, expected_values in field_config.items():
                    if subfield == 'multiple':
                        continue

                    if field_name == 'fundingReferences':
                        value = item.get(subfield)
                        if value:
                            if subfield not in seen_subfields:
                                field_stats['subfields'][subfield]['count'] += 1
                                seen_subfields.add(subfield)
                            field_stats['subfields'][subfield]['instances'] += 1
                            if isinstance(expected_values, set):
                                if value not in expected_values:
                                    value = 'Other'
                                field_stats['subfields'][subfield]['values'][value] += 1

                    elif subfield == 'nameType':
                        value = item.get('nameType', '')
                        if value:
                            if subfield not in seen_subfields:
                                field_stats['subfields'][subfield]['count'] += 1
                                seen_subfields.add(subfield)
                            field_stats['subfields'][subfield]['instances'] += 1
                            field_stats['subfields'][subfield]['values'][value] += 1

                    elif subfield == 'nameIdentifier':
                        identifiers = item.get('nameIdentifiers', [])
                        if identifiers:
                            identifiers = identifiers if isinstance(
                                identifiers, list) else [identifiers]
                            if subfield not in seen_subfields:
                                field_stats['subfields'][subfield]['count'] += 1
                                seen_subfields.add(subfield)
                            field_stats['subfields'][subfield]['instances'] += len(
                                identifiers)

                            for identifier in identifiers:
                                scheme = identifier.get('nameIdentifierScheme')
                                if scheme and scheme in field_config['nameIdentifierScheme']:
                                    if 'nameIdentifierScheme' not in seen_subfields:
                                        field_stats['subfields']['nameIdentifierScheme']['count'] += 1
                                        seen_subfields.add(
                                            'nameIdentifierScheme')
                                    field_stats['subfields']['nameIdentifierScheme']['instances'] += 1
                                    field_stats['subfields']['nameIdentifierScheme']['values'][scheme] += 1

                    elif subfield == 'affiliation':
                        affiliations = item.get('affiliation', [])
                        if affiliations:
                            affiliations = affiliations if isinstance(
                                affiliations, list) else [affiliations]
                            if subfield not in seen_subfields:
                                field_stats['subfields'][subfield]['count'] += 1
                                seen_subfields.add(subfield)
                            field_stats['subfields'][subfield]['instances'] += len(
                                affiliations)

                            for affiliation in affiliations:
                                if isinstance(affiliation, dict):
                                    identifier = affiliation.get(
                                        'affiliationIdentifier')
                                    if identifier:
                                        if 'affiliationIdentifier' not in seen_subfields:
                                            field_stats['subfields']['affiliationIdentifier']['count'] += 1
                                            seen_subfields.add(
                                                'affiliationIdentifier')
                                        field_stats['subfields']['affiliationIdentifier']['instances'] += 1

                                        scheme = affiliation.get(
                                            'affiliationIdentifierScheme')
                                        if scheme and scheme in field_config['affiliationIdentifierScheme']:
                                            if 'affiliationIdentifierScheme' not in seen_subfields:
                                                field_stats['subfields']['affiliationIdentifierScheme']['count'] += 1
                                                seen_subfields.add(
                                                    'affiliationIdentifierScheme')
                                            field_stats['subfields']['affiliationIdentifierScheme']['instances'] += 1
                                            field_stats['subfields']['affiliationIdentifierScheme']['values'][scheme] += 1

                    elif subfield in ['contributorType', 'relationType', 'relatedIdentifierType', 'resourceTypeGeneral']:
                        value = item.get(subfield)
                        if value and value in expected_values:
                            if subfield not in seen_subfields:
                                field_stats['subfields'][subfield]['count'] += 1
                                seen_subfields.add(subfield)
                            field_stats['subfields'][subfield]['instances'] += 1
                            field_stats['subfields'][subfield]['values'][value] += 1

            for subfield in field_config:
                if subfield != 'multiple' and subfield in field_stats['subfields']:
                    subfield_stats = field_stats['subfields'][subfield]
                    subfield_stats['missing'] = total_dois - \
                        subfield_stats['count']
                    subfield_stats['completeness'] = subfield_stats['count'] / \
                        total_dois if total_dois > 0 else 0.0

        else:
            # Skip processing if this is a fundingReferences field since it should only be processed as a list
            if field_name == 'fundingReferences':
                return

            field_stats['instances'] += 1
            for subfield, expected_values in field_config.items():
                if subfield == 'multiple':
                    continue

                value = field_value.get(subfield)
                if value and (isinstance(expected_values, dict) or value in expected_values):
                    if subfield not in field_stats['subfields']:
                        continue

                    field_stats['subfields'][subfield]['count'] += 1
                    field_stats['subfields'][subfield]['instances'] += 1
                    if 'values' in field_stats['subfields'][subfield]:
                        field_stats['subfields'][subfield]['values'][value] += 1
                    field_stats['subfields'][subfield]['missing'] = total_dois - \
                        field_stats['subfields'][subfield]['count']
                    field_stats['subfields'][subfield]['completeness'] = field_stats['subfields'][subfield]['count'] / \
                        total_dois if total_dois > 0 else 0.0

    def update_stats_single_record(self, stats_obj, record):
        stats_obj['stats']['summary']['count'] += 1
        total_dois = stats_obj['stats']['summary']['count']

        def update_field_stats(field_value, field_stats, field_name):
            """Helper function to update stats for a single field"""
            if not field_value:
                return

            has_subfields = field_name in self.container.SUBFIELD_STATS

            if isinstance(field_value, (list, tuple)):
                if field_value:
                    field_stats['count'] += 1
                    if not has_subfields:
                        field_stats['instances'] += len(field_value)
            elif isinstance(field_value, dict):
                if field_value:
                    field_stats['count'] += 1
                    if not has_subfields:
                        field_stats['instances'] += 1
            else:
                if field_value not in (None, ''):
                    field_stats['count'] += 1
                    if not has_subfields:
                        field_stats['instances'] += 1

        for field_name, field_status in self.container.FIELD_STATUS.items():
            field_value = record.get(field_name)
            field_stats = stats_obj['stats']['summary']['fields'][field_name]

            update_field_stats(field_value, field_stats, field_name)

            if field_name in self.container.SUBFIELD_STATS and field_value:
                self.update_subfield_stats(
                    field_value, field_name, stats_obj, 'all')

            field_stats['missing'] = total_dois - field_stats['count']
            field_stats['completeness'] = field_stats['count'] / \
                total_dois if total_dois > 0 else 0.0

        stats_obj['stats']['summary']['categories'] = self.calculate_category_metrics(
            stats_obj['stats']['summary']['fields'],
            total_dois
        )

        resource_type = None
        if record.get('resourceType'):
            if isinstance(record['resourceType'], dict):
                resource_type = record['resourceType'].get(
                    'resourceTypeGeneral')
            elif isinstance(record['resourceType'], str):
                resource_type = record['resourceType']

        if resource_type and resource_type in stats_obj['stats']['byResourceType']['resourceTypes']:
            type_stats = stats_obj['stats']['byResourceType']['resourceTypes'][resource_type]
            type_stats['count'] += 1
            total_type_dois = type_stats['count']

            for field_name in self.container.FIELD_STATUS:
                field_value = record.get(field_name)
                field_stats = type_stats['fields'][field_name]

                update_field_stats(field_value, field_stats, field_name)

                if field_name in self.container.SUBFIELD_STATS and field_value:
                    self.update_subfield_stats(
                        field_value, field_name, stats_obj, resource_type)

                field_stats['missing'] = total_type_dois - field_stats['count']
                field_stats['completeness'] = field_stats['count'] / \
                    total_type_dois if total_type_dois > 0 else 0.0

            type_stats['categories'] = self.calculate_category_metrics(
                type_stats['fields'],
                total_type_dois
            )

    def calculate_category_metrics(self, fields, total_dois):
        """Calculate metrics for each category (mandatory/recommended/optional).

        Args:
            fields (dict): Fields to calculate metrics for
            total_dois (int): Total number of DOIs

        Returns:
            dict: Category metrics
        """
        categories = {
            'mandatory': {'count': 0, 'total': 0, 'num_fields': 0},
            'recommended': {'count': 0, 'total': 0, 'num_fields': 0},
            'optional': {'count': 0, 'total': 0, 'num_fields': 0}
        }

        for field_name, field_stats in fields.items():
            status = field_stats['fieldStatus']
            if status in categories:
                categories[status]['num_fields'] += 1
                categories[status]['count'] += field_stats['count']

        for status in categories:
            if categories[status]['num_fields'] > 0:
                categories[status]['total'] = total_dois * \
                    categories[status]['num_fields']

        return {
            'mandatory': {
                'completeness': categories['mandatory']['count'] / categories['mandatory']['total']
                if categories['mandatory']['total'] > 0 else 0.0
            },
            'recommended': {
                'completeness': categories['recommended']['count'] / categories['recommended']['total']
                if categories['recommended']['total'] > 0 else 0.0
            },
            'optional': {
                'completeness': categories['optional']['count'] / categories['optional']['total']
                if categories['optional']['total'] > 0 else 0.0
            }
        }

    def merge_fields(self, fields1, fields2):
        """Merge field statistics with updated metrics.

        Args:
            fields1: First fields dictionary containing field statistics
            fields2: Second fields dictionary containing field statistics

        Returns:
            dict: Merged field statistics
        """
        if not fields1:
            return fields2
        if not fields2:
            return fields1

        merged = {}
        all_fields = set(fields1.keys()) | set(fields2.keys())

        total_dois = max(
            (field_stats1.get('count', 0) + field_stats1.get('missing', 0) +
             field_stats2.get('count', 0) + field_stats2.get('missing', 0))
            for field_stats1, field_stats2 in ((fields1.get(field, {}), fields2.get(field, {}))
                                               for field in all_fields)
        )

        for field in all_fields:
            field_stats1 = fields1.get(field, {'count': 0, 'instances': 0})
            field_stats2 = fields2.get(field, {'count': 0, 'instances': 0})

            count = field_stats1.get('count', 0) + field_stats2.get('count', 0)

            merged[field] = {
                'count': count,
                'instances': field_stats1.get('instances', 0) + field_stats2.get('instances', 0),
                'missing': total_dois - count,
                'fieldStatus': field_stats1.get('fieldStatus', field_stats2.get('fieldStatus')),
                'completeness': count / total_dois if total_dois > 0 else 0.0
            }

            if ('subfields' in field_stats1 or 'subfields' in field_stats2):
                subfields1 = field_stats1.get('subfields', {})
                subfields2 = field_stats2.get('subfields', {})
                merged[field]['subfields'] = {}

                all_subfields = set(subfields1.keys()) | set(subfields2.keys())

                for subfield in all_subfields:
                    subfield_stats1 = subfields1.get(
                        subfield, {'count': 0, 'instances': 0})
                    subfield_stats2 = subfields2.get(
                        subfield, {'count': 0, 'instances': 0})

                    subfield_count = subfield_stats1.get(
                        'count', 0) + subfield_stats2.get('count', 0)
                    merged[field]['subfields'][subfield] = {
                        'count': subfield_count,
                        'instances': subfield_stats1.get('instances', 0) + subfield_stats2.get('instances', 0),
                        'missing': total_dois - subfield_count,
                        'completeness': subfield_count / total_dois if total_dois > 0 else 0.0
                    }

                    if 'values' in subfield_stats1 or 'values' in subfield_stats2:
                        merged[field]['subfields'][subfield]['values'] = {}
                        values1 = subfield_stats1.get('values', {})
                        values2 = subfield_stats2.get('values', {})
                        all_values = set(values1.keys()) | set(values2.keys())

                        for value in all_values:
                            merged[field]['subfields'][subfield]['values'][value] = \
                                values1.get(value, 0) + values2.get(value, 0)

        return merged

    def merge_stats(self, stats1, stats2):
        """Merge statistics with new structure.

        Args:
            stats1: First statistics dictionary
            stats2: Second statistics dictionary

        Returns:
            dict: Merged statistics
        """
        if not stats1:
            return stats2
        if not stats2:
            return stats1

        merged = {
            'stats': {
                'summary': {
                    'count': stats1['stats']['summary']['count'] + stats2['stats']['summary']['count'],
                    'fields': self.merge_fields(
                        stats1['stats']['summary']['fields'],
                        stats2['stats']['summary']['fields']
                    ),
                    'categories': {}
                },
                'byResourceType': {
                    'resourceTypes': {}
                }
            }
        }

        all_resource_types = set(stats1['stats']['byResourceType']['resourceTypes'].keys()) | \
            set(stats2['stats']['byResourceType']['resourceTypes'].keys())

        for resource_type in all_resource_types:
            type_stats1 = stats1['stats']['byResourceType']['resourceTypes'].get(
                resource_type, {})
            type_stats2 = stats2['stats']['byResourceType']['resourceTypes'].get(
                resource_type, {})

            merged['stats']['byResourceType']['resourceTypes'][resource_type] = {
                'count': type_stats1.get('count', 0) + type_stats2.get('count', 0),
                'fields': self.merge_fields(
                    type_stats1.get('fields', {}),
                    type_stats2.get('fields', {})
                ),
                'categories': {}
            }

            total_dois = merged['stats']['byResourceType']['resourceTypes'][resource_type]['count']
            merged['stats']['byResourceType']['resourceTypes'][resource_type]['categories'] = \
                self.calculate_category_metrics(
                    merged['stats']['byResourceType']['resourceTypes'][resource_type]['fields'],
                    total_dois
            )

        merged['stats']['summary']['categories'] = self.calculate_category_metrics(
            merged['stats']['summary']['fields'],
            merged['stats']['summary']['count']
        )

        return merged


class ProviderClientManager:
    """Manager for provider and client data."""

    def __init__(self, stats_container):
        self.stats_container = stats_container
        self.providers = {}
        self.clients = {}
        self.stats_updater = StatsUpdater(self.stats_container)

    def initialize_provider_entry(self, attributes=None):
        stats = self.stats_container.create_empty_stats()['stats']
        entry = {
            'id': '',
            'type': 'providers',
            'attributes': attributes if attributes else {},
            'relationships': {'clients': []},
            'stats': stats
        }
        return entry

    def initialize_client_entry(self, attributes=None):
        stats = self.stats_container.create_empty_stats()['stats']
        entry = {
            'id': '',
            'type': 'clients',
            'attributes': attributes if attributes else {},
            'relationships': {'provider': None},
            'stats': stats
        }
        return entry

    def initialize_output_structure(self, api_client):
        providers = api_client.get_providers()
        clients = api_client.get_clients()

        for provider in providers:
            provider_id = provider['id']
            self.providers[provider_id] = self.initialize_provider_entry(
                provider.get('attributes', {})
            )
            self.providers[provider_id]['id'] = provider_id

        for client in clients:
            client_id = client['id']
            self.clients[client_id] = self.initialize_client_entry(
                client.get('attributes', {})
            )
            self.clients[client_id]['id'] = client_id

            provider_rel = client.get('relationships', {}).get(
                'provider', {}).get('data', {})
            if provider_rel:
                provider_id = provider_rel.get('id')
                self.clients[client_id]['relationships']['provider'] = provider_id
                if provider_id in self.providers:
                    self.providers[provider_id]['relationships']['clients'].append(
                        client_id
                    )

    def merge_provider_stats(self, provider_id, stats):
        if provider_id in self.providers:
            self.providers[provider_id]['stats'] = self.stats_updater.merge_stats(
                {'stats': self.providers[provider_id]['stats']},
                stats
            )['stats']

    def merge_client_stats(self, client_id, stats):
        if client_id in self.clients:
            self.clients[client_id]['stats'] = self.stats_updater.merge_stats(
                {'stats': self.clients[client_id]['stats']},
                stats
            )['stats']

    def create_aggregate_entries(self):
        """
        Create a aggregate 'aggregate' provider and 'aggregate.all' client 
        that aggregate all providers/clients stats.
        """
        all_providers_aggregator = self.stats_container.create_empty_stats()
        stats_updater = StatsUpdater(self.stats_container)
        
        for provider in self.providers.values():
            # Each provider has {'stats': ...}; so wrap it such that merge_stats expects {'stats': ...}
            all_providers_aggregator = stats_updater.merge_stats(
                all_providers_aggregator,
                {'stats': provider['stats']}
            )
        
        all_clients_aggregator = self.stats_container.create_empty_stats()
        for client in self.clients.values():
            all_clients_aggregator = stats_updater.merge_stats(
                all_clients_aggregator,
                {'stats': client['stats']}
            )
        
        aggregate_provider = self.initialize_provider_entry()
        aggregate_provider['id'] = 'aggregate'
        aggregate_provider['type'] = 'providers'
        aggregate_provider['attributes'] = {
            'symbol': 'AGGREGATE',
            'name': 'All DataCite Organizations (All Providers Aggregated)'
        }
        aggregate_provider['relationships']['clients'] = ['aggregate.all']
        aggregate_provider['stats'] = all_providers_aggregator['stats']
        
        aggregate_client = self.initialize_client_entry()
        aggregate_client['id'] = 'aggregate.all'
        aggregate_client['type'] = 'clients'
        aggregate_client['attributes'] = {
            'symbol': 'AGGREGATE.ALL',
            'name': 'All DataCite Repositories (All Clients Aggregated)'
        }
        aggregate_client['stats'] = all_clients_aggregator['stats']

        self.providers['aggregate'] = aggregate_provider
        self.clients['aggregate.all'] = aggregate_client

    def filter_active_only(self):
        self.providers = {
            pid: p for pid, p in self.providers.items()
            if p['stats']['summary']['count'] > 0
        }
        self.clients = {
            cid: c for cid, c in self.clients.items()
            if c['stats']['summary']['count'] > 0
        }

    def clean_resource_types(self):
        for provider in self.providers.values():
            provider['stats'] = self.stats_container.remove_zero_count_resource_types_and_clean(
                provider['stats']
            )

        for client in self.clients.values():
            client['stats'] = self.stats_container.remove_zero_count_resource_types_and_clean(
                client['stats']
            )

    def get_providers(self):
        return self.providers

    def get_clients(self):
        return self.clients


class FileScanner:
    """Checks input directory for JSONL.gz files (format used in the data files)"""

    def __init__(self):
        self.logger = logging.getLogger('datacite.file_scanner')

    def scan_jsonl_files(self, directory):
        try:
            directory = Path(directory)
            if not directory.is_dir():
                self.logger.error(
                    f"Input directory does not exist or is not a directory: {directory}"
                )
                return {'files': []}

            files = list(directory.rglob('*.jsonl.gz'))
            if not files:
                self.logger.warning(f"No .jsonl.gz files found in {directory}")
                return {'files': []}

            return {'files': [str(f) for f in files]}

        except Exception as e:
            self.logger.error(f"Error scanning directory {directory}: {str(e)}")
            return {'files': []}


class BatchGzipReader:
    def __init__(self, filepath, chunk_size=64 * 1024):
        """Initialize the reader.

        Args:
            filepath: Path to .jsonl.gz file
            chunk_size: Size of chunks to read in bytes (default 64KB)
        """
        self.filepath = filepath
        self.chunk_size = chunk_size
        self.buffer = ""
        self.logger = logging.getLogger('datacite.batch_reader')

    def __iter__(self):
        with gzip.open(self.filepath, 'rt', encoding='utf-8') as f:
            # Read initial chunk
            chunk = f.read(self.chunk_size)

            while chunk:
                # Split on newlines, keeping any partial line in buffer
                lines = (self.buffer + chunk).split('\n')

                # If chunk doesn't end with newline, last line is partial
                if chunk.endswith('\n'):
                    self.buffer = ""
                else:
                    self.buffer = lines[-1]
                    lines = lines[:-1]
                for line in lines:
                    if line.strip():
                        try:
                            yield orjson.loads(line)
                        except orjson.JSONDecodeError as e:
                            self.logger.warning(
                                f"Error decoding JSON in {self.filepath}: {str(e)}"
                            )
                            continue
                chunk = f.read(self.chunk_size)

            if self.buffer.strip():
                try:
                    yield orjson.loads(self.buffer)
                except orjson.JSONDecodeError as e:
                    self.logger.warning(
                        f"Error decoding final JSON in {self.filepath}: {str(e)}"
                    )


class FileProcessor:
    """Processor class for individual jsonl.gz files."""

    def __init__(self, stats_container, counter=None, lock=None, total_files=None):
        """Initialize the file processor.

        Args:
            stats_container (StatsContainer): Container for stats operations
            counter: Shared counter for tracking progress
            lock: Lock for thread-safe operations
            total_files: Total number of files to process
        """
        self.stats_container = stats_container
        self._counter = counter
        self._lock = lock
        self._total_files = total_files
        self.logger = logging.getLogger('datacite.file_processor')

    def get_fields(self, item):
        attributes = item.get('attributes', item)
        return {
            'identifier': attributes.get('doi'),
            'creators': attributes.get('creators', []),
            'titles': attributes.get('titles', []),
            'publisher': attributes.get('publisher'),
            'publicationYear': attributes.get('publicationYear'),
            'resourceType': attributes.get('types', {}),
            'subjects': attributes.get('subjects', []),
            'contributors': attributes.get('contributors', []),
            'date': attributes.get('dates', []),
            'relatedIdentifiers': attributes.get('relatedIdentifiers', []),
            'description': attributes.get('descriptions', []),
            'geoLocations': attributes.get('geoLocations', []),
            'language': attributes.get('language'),
            'alternateIdentifiers': attributes.get('alternateIdentifiers', []),
            'sizes': attributes.get('sizes', []),
            'formats': attributes.get('formats', []),
            'version': attributes.get('version'),
            'rights': attributes.get('rightsList', []),
            'fundingReferences': attributes.get('fundingReferences', []),
            'relatedItems': attributes.get('relatedItems', [])
        }

    def log_progress(self, message):
        sys.stdout.write(f"{message}\n")
        sys.stdout.flush()

    def process_file(self, filepath):
        try:
            client_stats = {}  # client_id -> stats
            provider_stats = {}  # provider_id -> stats
            skipped_count = 0
            processed_count = 0
            line_number = 0
            reader = BatchGzipReader(filepath)
            for item in reader:
                line_number += 1
                try:
                    record_state = item.get('attributes', {}).get('state')
                    if record_state != 'findable':
                        skipped_count += 1
                        continue
                    relationships = item.get('relationships', {})
                    client_id = relationships.get(
                        'client', {}).get('data', {}).get('id')
                    provider_id = relationships.get(
                        'provider', {}).get('data', {}).get('id')

                    if not client_id and not provider_id:
                        self.logger.warning(f"No client or provider relationship found in line {line_number}")
                        continue
                    normalized = self.get_fields(item)

                    if client_id and client_id not in client_stats:
                        client_stats[client_id] = self.stats_container.create_empty_stats(
                        )
                    if provider_id and provider_id not in provider_stats:
                        provider_stats[provider_id] = self.stats_container.create_empty_stats(
                        )

                    stats_updater = StatsUpdater(self.stats_container)
                    if client_id:
                        stats_updater.update_stats_single_record(
                            client_stats[client_id], normalized
                        )
                    if provider_id:
                        stats_updater.update_stats_single_record(
                            provider_stats[provider_id], normalized
                        )

                    processed_count += 1

                    if line_number % 100000 == 0:
                        self.logger.debug(
                            f"Processed {line_number} lines in {Path(filepath).name}: "
                            f"{processed_count} findable records, {skipped_count} skipped"
                        )

                except Exception as e:
                    self.logger.warning(f"Error processing line {line_number} in {filepath}: {str(e)}")
                    continue

            if self._lock and self._counter and self._total_files:
                with self._lock:
                    self._counter.value += 1
                    self.log_progress(
                        f"Completed {self._counter.value}/{self._total_files} files "
                        f"({Path(filepath).name}): {processed_count} findable records, "
                        f"{skipped_count} skipped"
                    )

            return filepath, client_stats, provider_stats

        except Exception as e:
            self.logger.error(f"Error processing file {filepath}: {str(e)}")
            return filepath, {}, {}


class OutputWriter:
    """Handles final output writing and validation for  metadata statistics."""

    def __init__(self):
        self.logger = logging.getLogger('datacite.output_writer')

    def validate_output(self, data, data_type, output_type):
        """Validate output data structure.

        Args:
            data (list): List of data items to validate
            data_type (str): Type of data ('providers' or 'clients')
            output_type (str): Type of output ('attributes' or 'stats')

        Returns:
            bool: True if valid, False otherwise
        """
        required_fields = {
            'providers_attributes': {'id', 'type', 'attributes', 'relationships'},
            'clients_attributes': {'id', 'type', 'attributes', 'relationships'},
            'providers_stats': {'id', 'stats'},
            'clients_stats': {'id', 'stats'}
        }

        validation_key = f"{data_type}_{output_type}"

        for item in data:
            missing_fields = required_fields[validation_key] - set(item.keys())
            if missing_fields:
                self.logger.error(
                    f"Missing required fields in {validation_key}: {missing_fields}"
                )
                return False
        return True

    def split_data(self, data, keep_stats=False):
        """Split data into attributes and stats components.

        Args:
            data (list): List of data items to split
            keep_stats (bool): Whether to keep stats (True) or attributes (False)

        Returns:
            list: Split data items
        """
        result = []
        for item in data:
            new_item = {}
            if keep_stats:
                new_item = {'id': item['id'], 'stats': item['stats']}
            else:
                new_item = {
                    'id': item['id'],
                    'type': item['type'],
                    'attributes': item['attributes'],
                    'relationships': item['relationships']
                }
            result.append(new_item)
        return result

    def write_output(self, provider_data, client_data, output_dir):
        try:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().isoformat()

            provider_output = list(provider_data.values())
            client_output = list(client_data.values())

            provider_attributes = self.split_data(
                provider_output, keep_stats=False)
            provider_stats = self.split_data(provider_output, keep_stats=True)
            client_attributes = self.split_data(
                client_output, keep_stats=False)
            client_stats = self.split_data(client_output, keep_stats=True)

            validations = [
                ('providers', 'attributes', provider_attributes),
                ('providers', 'stats', provider_stats),
                ('clients', 'attributes', client_attributes),
                ('clients', 'stats', client_stats)
            ]

            for data_type, output_type, data in validations:
                if not self.validate_output(data, data_type, output_type):
                    self.logger.error(f"Validation failed for {data_type}_{output_type}")
                    return False

            output_files = {
                'providers_attributes.json': provider_attributes,
                'providers_stats.json': provider_stats,
                'clients_attributes.json': client_attributes,
                'clients_stats.json': client_stats
            }

            for filename, data in output_files.items():
                output_path = output_dir / filename
                with open(output_path, 'w') as f:
                    json.dump({
                        'data': data,
                        'meta': {
                            'total': len(data),
                            'timestamp': timestamp
                        }
                    }, f, indent=2)

                self.logger.info(f"Wrote {len(data)} records to {output_path}")

            self.logger.info(f"Successfully wrote all output files to {output_dir}")
            return True

        except Exception as e:
            self.logger.error(f"Error writing output: {str(e)}")
            return False

    def validate_and_write_single(self, output_path, data, data_type, output_type):
        try:
            if not self.validate_output(data, data_type, output_type):
                return False

            timestamp = datetime.now().isoformat()

            with open(output_path, 'w') as f:
                json.dump({
                    'data': data,
                    'meta': {
                        'total': len(data),
                        'timestamp': timestamp
                    }
                }, f, indent=2)

            self.logger.info(f"Wrote {len(data)} records to {output_path}")
            return True

        except Exception as e:
            self.logger.error(f"Error writing {output_path}: {str(e)}")
            return False


class DataCiteDataFileProcessor:
    """Main class for processing."""

    def __init__(self):
        self.logger = None

    def init_worker(self, counter, lock, total_files):
        """Initialize the worker processes (w/ shared state).

        Args:
            counter: Shared counter for progress tracking
            lock: Lock for thread-safe operations
            total_files: Total number of files to process
        """
        global _counter, _lock, _total_files
        _counter = counter
        _lock = lock
        _total_files = total_files

    def run(self):
        try:
            config = ArgumentConfig.parse_arguments()
            
            self.logger = LoggerSetup.configure(config.log_level)
            
            api_client = DataCiteAPIClient(config.cache_dir)
            stats_container = StatsContainer()
            provider_client_manager = ProviderClientManager(stats_container)
            
            provider_client_manager.initialize_output_structure(api_client)
            
            file_scanner = FileScanner()
            files = file_scanner.scan_jsonl_files(config.input_dir)
            total_files = len(files['files'])
            
            if total_files == 0:
                self.logger.error(f"No .jsonl.gz files found in {config.input_dir}")
                return 1
            
            self.logger.info(f"Found {total_files} files to process")
            
            counter = Manager().Value('i', 0)
            lock = Manager().Lock()
            file_processor = FileProcessor(
                stats_container=stats_container,
                counter=counter,
                lock=lock,
                total_files=total_files
            )
            
            try:
                pool = Pool(
                    processes=max(1, cpu_count() - 1),
                    initializer=self.init_worker,
                    initargs=(counter, lock, total_files)
                )
                
                for filepath, file_client_stats, file_provider_stats in pool.imap_unordered(
                    file_processor.process_file,
                    files['files']
                ):
                    for client_id, stats in file_client_stats.items():
                        provider_client_manager.merge_client_stats(client_id, stats)

                    for provider_id, stats in file_provider_stats.items():
                        provider_client_manager.merge_provider_stats(provider_id, stats)

                    del file_client_stats
                    del file_provider_stats

            finally:
                pool.close()
                pool.join()
            
            self.logger.info("Filtering active providers and clients")
            provider_client_manager.filter_active_only()
            
            self.logger.info("Creating aggregate entries for all DataCite providers and clients")
            provider_client_manager.create_aggregate_entries()
            
            self.logger.info("Cleaning resource types")
            provider_client_manager.clean_resource_types()
            
            self.logger.info(f"Writing output to {config.output_dir}")
            output_writer = OutputWriter()
            success = output_writer.write_output(
                provider_data=provider_client_manager.get_providers(),
                client_data=provider_client_manager.get_clients(),
                output_dir=config.output_dir
            )

            if not success:
                self.logger.error("Failed to write output files")
                return 1

            self.logger.info("Processing completed successfully")
            return 0

        except Exception as e:
            if self.logger:
                self.logger.error(f"Application error: {str(e)}", exc_info=True)
            else:
                print(f"Error before logger initialization: {str(e)}", file=sys.stderr)
            return 1


def main():
    app = DataCiteDataFileProcessor()
    sys.exit(app.run())


if __name__ == '__main__':
    main()
