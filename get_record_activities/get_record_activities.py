import os
import re
import csv
import sys
import json
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime
from urllib.parse import quote_plus
from contextlib import contextmanager
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed


try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    print("Required package 'requests' not found. Install it using: pip install requests")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('doi_tracker')

API_BASE_URL = "https://api.datacite.org"
API_CALLS_LIMIT = 1000
API_WINDOW_SECONDS = 300
API_TIMEOUT_SECONDS = 30
MAX_JSON_SIZE = 32768
DEFAULT_ENCODING = 'utf-8'
DOI_REGEX = r'(10\.\d{4,}(?:\.\d+)*\/(?:(?!["&\'<>])\S)+)'
REQUIRED_CSV_COLUMNS = {'doi', 'state', 'client_id', 'updated'}
ACTIONS_OF_INTEREST = {'update', 'delete'}


@dataclass
class DOIRecord:
    doi: str
    state: str
    client_id: str
    updated: str
    activities = field(default_factory=list)
    normalized_doi = None
    errors = field(default_factory=list)
    has_changes = False

    def __post_init__(self):
        self.normalized_doi = normalize_doi(self.doi)


def normalize_doi(doi):
    if not doi:
        return None
        
    match = re.search(DOI_REGEX, doi)
    if match:
        return match.group(1)
    
    prefixes = ['https://doi.org/', 'http://doi.org/', 'doi:', 'DOI:']
    result = doi
    for prefix in prefixes:
        if doi.startswith(prefix):
            result = doi[len(prefix):]
            break
    
    if re.match(DOI_REGEX, result):
        return result
    
    return None


class RateLimitedSession:
    def __init__(self, base_url, calls_limit=API_CALLS_LIMIT, period_seconds=API_WINDOW_SECONDS, timeout=API_TIMEOUT_SECONDS):
        self.base_url = base_url
        self.calls_limit = calls_limit
        self.period_seconds = period_seconds
        self.timeout = timeout
        self.request_times = []
        self.session = self._create_session()
        
    def _create_session(self):
        session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
    
    def _enforce_rate_limit(self):
        current_time = time.time()
        self.request_times = [t for t in self.request_times if current_time - t < self.period_seconds]
        
        if len(self.request_times) >= self.calls_limit:
            sleep_time = self.period_seconds - (current_time - self.request_times[0]) + 0.1
            if sleep_time > 0:
                logger.info(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds")
                time.time()
                current_time = time.time()
                self.request_times = [t for t in self.request_times if current_time - t < self.period_seconds]
    
    def get(self, url_path):
        self._enforce_rate_limit()
        self.request_times.append(time.time())
        full_url = f"{self.base_url}/{url_path.lstrip('/')}"
        return self.session.get(full_url, timeout=self.timeout)


class DataCiteAPI:
    def __init__(self, base_url=API_BASE_URL):
        self.base_url = base_url
        self.session = RateLimitedSession(base_url)

    def get_activities(self, doi):
        if not doi:
            return {"data": []}
            
        encoded_doi = quote_plus(doi)
        url_path = f"dois/{encoded_doi}/activities"
        logger.debug(f"Querying activities for DOI: {doi}")
        
        max_retries = 5
        retry_count = 0
        backoff_factor = 1.5
        
        while retry_count < max_retries:
            try:
                response = self.session.get(url_path)
                
                if response.status_code == 404:
                    logger.warning(f"DOI not found: {doi}")
                    return {"data": []}
                elif response.status_code == 429:
                    retry_count += 1
                    sleep_time = backoff_factor ** retry_count
                    logger.warning(f"Rate limit exceeded for DOI: {doi}. Retrying in {sleep_time:.2f}s")
                    time.sleep(sleep_time)
                    continue
                else:
                    response.raise_for_status()
                    
                return response.json()
            except requests.RequestException as e:
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(f"Error retrieving activities for DOI {doi} after {max_retries} retries: {e}")
                    return {"data": []}
                
                sleep_time = backoff_factor ** retry_count
                logger.warning(f"Request failed for DOI {doi}: {e}. Retrying in {sleep_time:.2f}s")
                time.sleep(sleep_time)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON response for DOI {doi}")
                return {"data": []}


@contextmanager
def safe_open_dictcsv(file_path, mode, fieldnames=None, encoding=DEFAULT_ENCODING, **kwargs):
    file = None
    try:
        file = open(file_path, mode, newline='', encoding=encoding)
        if 'r' in mode:
            yield csv.DictReader(file, **kwargs)
        else:
            if fieldnames is None:
                raise ValueError("fieldnames parameter is required for DictWriter")
            yield csv.DictWriter(file, fieldnames=fieldnames, **kwargs)
    except OSError as e:
        logger.error(f"Error opening file {file_path}: {e}")
        raise
    finally:
        if file is not None:
            file.close()


class CSVProcessor:
    @staticmethod
    def read_doi_records(filepath):
        records = []
        logger.info(f"Reading DOI records from {filepath}")
        try:
            with safe_open_dictcsv(filepath, 'r') as reader:
                if not set(REQUIRED_CSV_COLUMNS).issubset(set(reader.fieldnames or [])):
                    missing = set(REQUIRED_CSV_COLUMNS) - set(reader.fieldnames or [])
                    raise ValueError(f"CSV missing required columns: {', '.join(missing)}")
                
                for row in reader:
                    if all(row.get(col) for col in REQUIRED_CSV_COLUMNS):
                        records.append(DOIRecord(
                            doi=row['doi'],
                            state=row['state'],
                            client_id=row['client_id'],
                            updated=row['updated']
                        ))
            
            logger.info(f"Read {len(records)} valid DOI records")
            return records
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise

    @staticmethod
    def safe_json_dumps(obj, max_size=MAX_JSON_SIZE):
        try:
            result = json.dumps(obj)
            if len(result) > max_size:
                return json.dumps({"error": "Object too large", "size": len(result)})
            return result
        except (TypeError, OverflowError) as e:
            return json.dumps({"error": str(e)})

    @staticmethod
    def write_unchanged_dois(doi_records, output_file, overwrite=False):
        unchanged_dois = [record.doi for record in doi_records if not record.has_changes]
        
        if not unchanged_dois:
            logger.info("No unchanged DOIs found")
            return
            
        logger.info(f"Writing {len(unchanged_dois)} unchanged DOIs to {output_file}")
        
        output_path = Path(output_file)
        if output_path.exists() and not overwrite:
            logger.error(f"Output file already exists: {output_file}")
            raise FileExistsError(f"Output file already exists: {output_file}")
            
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with safe_open_dictcsv(output_file, 'w', fieldnames=['doi']) as writer:
                writer.writeheader()
                for doi in unchanged_dois:
                    writer.writerow({'doi': doi})
                    
            logger.info(f"Successfully wrote unchanged DOIs to {output_file}")
        except Exception as e:
            logger.error(f"Error writing unchanged DOIs to file: {e}")
            raise


class ChangeProcessor:
    @staticmethod
    def write_all_changes_csv(doi_records, output_file, overwrite=False):
        logger.info(f"Processing all changes to {output_file}")
        
        output_path = Path(output_file)
        if output_path.exists() and not overwrite:
            logger.error(f"Output file already exists: {output_file}")
            raise FileExistsError(f"Output file already exists: {output_file}")
            
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        fieldnames = [
            'doi', 'activity_id', 'timestamp', 'action', 'version', 
            'change_type', 'old_value', 'new_value'
        ]
        
        try:
            with safe_open_dictcsv(output_file, 'w', fieldnames=fieldnames) as writer:
                writer.writeheader()
                
                for record in doi_records:
                    has_activity_with_changes = False
                    
                    for activity in record.activities:
                        attrs = activity.get('attributes', {})
                        action = attrs.get('action', '')
                        
                        if action not in ACTIONS_OF_INTEREST:
                            continue
                            
                        changes = attrs.get('changes', {})
                        
                        if not isinstance(changes, dict) or not changes:
                            continue
                            
                        activity_id = activity.get('id', '')
                        timestamp = attrs.get('prov:generatedAtTime', '')
                        version = attrs.get('version', '')
                        
                        for change_type, change_data in changes.items():
                            if not change_data:
                                continue
                                
                            has_activity_with_changes = True
                            
                            try:
                                if isinstance(change_data, list) and len(change_data) == 2:
                                    row = {
                                        'doi': record.doi,
                                        'activity_id': activity_id,
                                        'timestamp': timestamp,
                                        'action': action,
                                        'version': version,
                                        'change_type': change_type,
                                        'old_value': CSVProcessor.safe_json_dumps(change_data[0]),
                                        'new_value': CSVProcessor.safe_json_dumps(change_data[1])
                                    }
                                    writer.writerow(row)
                                else:
                                    row = {
                                        'doi': record.doi,
                                        'activity_id': activity_id,
                                        'timestamp': timestamp,
                                        'action': action,
                                        'version': version,
                                        'change_type': change_type,
                                        'old_value': '',
                                        'new_value': CSVProcessor.safe_json_dumps(change_data)
                                    }
                                    writer.writerow(row)
                            except Exception as e:
                                logger.error(f"Error writing change data for DOI {record.doi}: {e}")
                                continue
                    
                    record.has_changes = has_activity_with_changes
                    
            logger.info(f"Successfully wrote all changes to {output_file}")
        except Exception as e:
            logger.error(f"Error writing changes to CSV file: {e}")
            raise


class DOIProvenanceTracker:
    def __init__(self, input_file, output_dir, threads=5, overwrite=False):
        self.input_file = input_file
        self.output_dir = output_dir
        self.threads = max(1, min(threads, 20))
        self.overwrite = overwrite
        self.api = DataCiteAPI()
        self.doi_records = []

    def process_doi(self, doi_record):
        try:
            if not doi_record.normalized_doi:
                doi_record.errors.append("Invalid DOI format")
                return doi_record
                
            activities_data = self.api.get_activities(doi_record.normalized_doi)
            
            all_activities = activities_data.get('data', [])
            doi_record.activities = [
                activity for activity in all_activities
                if activity.get('attributes', {}).get('action') in ACTIONS_OF_INTEREST
            ]
            
            if len(all_activities) != len(doi_record.activities):
                logger.debug(f"Filtered {len(all_activities) - len(doi_record.activities)} non-update/delete activities")
            
            return doi_record
        except Exception as e:
            doi_record.errors.append(f"Processing error: {str(e)}")
            return doi_record

    def run(self):
        logger.info("Starting metadata provenance tracking")

        try:
            os.makedirs(self.output_dir, exist_ok=True)
        except OSError as e:
            logger.error(f"Cannot create output directory: {e}")
            raise
        
        changes_file = os.path.join(self.output_dir, "all_changes.csv")
        unchanged_file = os.path.join(self.output_dir, "unchanged_dois.csv")
        
        try:
            self.doi_records = CSVProcessor.read_doi_records(self.input_file)
        except Exception as e:
            logger.error(f"Failed to read input file: {e}")
            raise
            
        if not self.doi_records:
            logger.warning("No valid DOI records found in input file")
            return
        
        logger.info(f"Processing {len(self.doi_records)} DOIs with {self.threads} threads")
        processed_records = []
        
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            future_to_doi = {executor.submit(self.process_doi, record): record 
                             for record in self.doi_records}
            
            for future in as_completed(future_to_doi):
                doi_record = future_to_doi[future]
                try:
                    result = future.result()
                    processed_records.append(result)
                except Exception as e:
                    logger.error(f"Thread error processing DOI {doi_record.doi}: {e}")
                    doi_record.errors.append(f"Thread error: {str(e)}")
                    processed_records.append(doi_record)
        
        try:
            ChangeProcessor.write_all_changes_csv(
                processed_records, changes_file, overwrite=self.overwrite
            )
            
            CSVProcessor.write_unchanged_dois(
                processed_records, unchanged_file, overwrite=self.overwrite
            )
        except Exception as e:
            logger.error(f"Failed to write output files: {e}")
            raise
        
        logger.info("Completed metadata provenance tracking")
        
        error_records = [r for r in processed_records if r.errors]
        if error_records:
            logger.warning(f"{len(error_records)} DOIs had errors during processing")
            
        changed_count = len([r for r in processed_records if r.has_changes])
        unchanged_count = len(processed_records) - changed_count
        logger.info(f"Summary: {changed_count} DOIs with changes, {unchanged_count} unchanged DOIs")


def main():
    parser = argparse.ArgumentParser(
        description='Track and process metadata provenance from DataCite DOIs',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-i', '--input-file', required=True, help='Input CSV file with DOI information')
    parser.add_argument('-o', '--output-dir', help='Directory to store output files (default: <input_name>_output)')
    parser.add_argument('-t', '--threads', type=int, default=5, help='Number of threads to use')
    parser.add_argument('-l', '--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO')
    parser.add_argument('--overwrite', action='store_true', help='Overwrite existing files')
    
    args = parser.parse_args()
    
    output_dir = args.output_dir
    if not output_dir:
        output_dir = f"{Path(args.input_file).stem}_output"
    
    logger.setLevel(getattr(logging, args.log_level))
    
    try:
        tracker = DOIProvenanceTracker(
            input_file=args.input_file,
            output_dir=output_dir,
            threads=args.threads,
            overwrite=args.overwrite
        )
        
        tracker.run()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Critical error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()