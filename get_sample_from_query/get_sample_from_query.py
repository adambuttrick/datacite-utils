import os
import re
import csv
import json
import logging
import argparse
import requests
from time import sleep
from datetime import datetime
from urllib.parse import urljoin, quote_plus


def setup_logging():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)


def normalize_name_for_path(query_string):
    normalized = re.sub(r'[^\w\s-]', '_', query_string)
    normalized = re.sub(r'\s+', '_', normalized)
    return normalized.strip('_')


def get_default_output_dir(query_string):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    normalized_query = normalize_name_for_path(query_string)
    return f"{timestamp}_{normalized_query}"


def parse_arguments():
    parser = argparse.ArgumentParser(description='Query DataCite API and save DOIs and associated files')
    parser.add_argument('-q', '--query', required=True, help='Query string (e.g. "client-id=datacite.datacite" or "query=climate")')
    parser.add_argument('-s', '--sample_size', type=int, required=True, help='Number of records per group (or total if no group)')
    parser.add_argument('-g', '--group_by', choices=['client', 'provider', 'resource-type'], help='Optional grouping parameter')
    parser.add_argument('-n', '--num_groups', type=int, default=20, help='Number of groups to sample (default: 20)')
    parser.add_argument('-d', '--delay', type=float, default=1.0, help='Delay between API requests in seconds')
    parser.add_argument('-o', '--output_dir', help='Directory for output files')
    args = parser.parse_args()
    if args.output_dir is None:
        args.output_dir = get_default_output_dir(args.query)
    return args


def create_output_directories(base_dir):
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
    csv_dir = os.path.join(base_dir, 'csv')
    json_dir = os.path.join(base_dir, 'json')
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)
    if not os.path.exists(json_dir):
        os.makedirs(json_dir)
    return csv_dir, json_dir


def get_doi_prefix(doi):
    try:
        return doi.split('/')[0]
    except (AttributeError, IndexError):
        return 'unknown'


def create_prefix_directory(json_dir, prefix):
    prefix_dir = os.path.join(json_dir, prefix)
    if not os.path.exists(prefix_dir):
        os.makedirs(prefix_dir)
    return prefix_dir


def parse_query_string(query_string):
    params = {}
    try:
        if '=' in query_string:
            key, value = query_string.split('=', 1)
            params[key.strip()] = value.strip()
        else:
            params['query'] = query_string.strip()
    except Exception as e:
        logging.error(f"Failed to parse query string: {e}")
    return params


def query_datacite_api(query_params, sample_size, group_by=None, num_groups=None, logger=None):
    base_url = 'https://api.datacite.org/dois'
    params = {
        **query_params
    }
    
    if group_by:
        params.update({
            'sample': sample_size,
            'sample-group': group_by,
            'page[size]': num_groups
        })
    else:
        params.update({
            'random': 'true',
            'page[size]': sample_size
        })
        
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        return None


def save_json_record(record, prefix_dir, logger):
    try:
        doi = record.get('id', 'unknown')
        filename = f"{doi.replace('/', '_')}.json"
        filepath = os.path.join(prefix_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Failed to save JSON record: {e}")


def save_dois_to_csv(dois, csv_dir, query_string, logger):
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        normalized_query = normalize_name_for_path(query_string)
        filename = f"{timestamp}_{normalized_query}.csv"
        filepath = os.path.join(csv_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['DOI'])
            for doi in dois: writer.writerow([doi])
    except Exception as e:
        logger.error(f"Failed to save CSV: {e}")


def process_response(response_data, json_dir, dois, logger):
    if not response_data or 'data' not in response_data:
        return
    for record in response_data['data']:
        try:
            doi = record.get('id')
            if doi:
                dois.add(doi)
                prefix = get_doi_prefix(doi)
                prefix_dir = create_prefix_directory(json_dir, prefix)
                save_json_record(record, prefix_dir, logger)
        except Exception as e:
            logger.error(f"Failed to process record: {e}")


def main():
    args = parse_arguments()
    logger = setup_logging()
    csv_dir, json_dir = create_output_directories(args.output_dir)
    collected_dois = set()
    query_params = parse_query_string(args.query)
    group_info = f" grouped by {args.group_by}" if args.group_by else ""
    logger.info(f"Starting collection with query parameters: {query_params}{group_info}")
    response_data = query_datacite_api(
        query_params, 
        args.sample_size,
        group_by=args.group_by,
        num_groups=args.num_groups,
        logger=logger
    )
    if response_data:
        process_response(response_data, json_dir, collected_dois, logger)
    save_dois_to_csv(collected_dois, csv_dir, args.query, logger)
    group_str = f" across {args.num_groups} {args.group_by}s" if args.group_by else ""
    logger.info(f"Collection complete. Total DOIs collected: {len(collected_dois)}{group_str}")


if __name__ == '__main__':
    main()