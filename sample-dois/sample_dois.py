import io
import os
import re
import csv
import sys
import gzip
import random
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict


class LoggerSetup:
    LOGGER_NAME = 'doi_sampler'
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


class DoiSampler:
    def __init__(self, config):
        self.config = config
        self.logger = LoggerSetup.configure(config.log_level)
        self.samples = []
        self.total_processed = 0
        self.total_matched = 0

    def process_file(self, file_path):
        self.logger.info(f"Processing file: {file_path}")
        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                csv_reader = csv.DictReader(f)
                for row in csv_reader:
                    self.total_processed += 1
                    if not self._match_filters(row):
                        continue
                    self.total_matched += 1
                    self.samples.append(row)
                    if self.config.sample_size and len(self.samples) >= self.config.sample_size:
                        return True
        
        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {str(e)}")
        
        return False
    
    def _match_filters(self, row):
        if self.config.provider or self.config.client:
            client_id = row.get('client_id', '')
            parts = client_id.split('.')
            if len(parts) >= 2:
                provider = parts[0]
                client = '.'.join(parts[1:])
                if self.config.provider and provider != self.config.provider:
                    return False
                if self.config.client and client != self.config.client:
                    return False
            else:
                if self.config.provider or self.config.client:
                    return False
        
        if self.config.prefix:
            doi = row.get('doi', '')
            if not doi.startswith(self.config.prefix):
                return False
        
        if self.config.start_date or self.config.end_date:
            updated = row.get('updated', '')
            if not updated:
                return False
            
            try:
                row_date = datetime.fromisoformat(updated.replace('Z', '+00:00'))
                if self.config.start_date and row_date < self.config.start_date:
                    return False
                if self.config.end_date and row_date > self.config.end_date:
                    return False
            except ValueError:
                return False
        
        return True
    
    def scan_directory(self):
        input_dir = Path(self.config.input_dir)
        if not input_dir.is_dir():
            self.logger.error(f"Input directory is not valid: {input_dir}")
            return False
        
        csv_files = []
        for month_dir in input_dir.iterdir():
            if month_dir.is_dir():
                for file in month_dir.glob("*.csv.gz"):
                    csv_files.append(file)
        
        if not csv_files:
            self.logger.error(f"No .csv.gz files found in {input_dir}")
            return False
        
        self.logger.info(f"Found {len(csv_files)} CSV files to process")
        
        if self.config.sample_size:
            random.shuffle(csv_files)
        
        for file_path in csv_files:
            if self.process_file(file_path):
                break
        
        return True
    
    def finalize_sample(self):
        if not self.samples:
            self.logger.warning("No samples found matching the criteria")
            return False
        
        if self.config.sample_size and len(self.samples) > self.config.sample_size:
            self.samples = random.sample(self.samples, self.config.sample_size)
        
        self.logger.info(f"Processed {self.total_processed} records, found {self.total_matched} matches")
        self.logger.info(f"Final sample size: {len(self.samples)}")
        
        if self.config.output_file:
            self._write_output()
        else:
            self._print_samples()
        
        return True
    
    def _write_output(self):
        try:
            output_path = Path(self.config.output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            if output_path.suffix == '.gz':
                opener = gzip.open
                mode = 'wt'
            else:
                opener = open
                mode = 'w'
            
            with opener(output_path, mode, newline='', encoding='utf-8') as f:
                if self.samples:
                    writer = csv.DictWriter(f, fieldnames=self.samples[0].keys())
                    writer.writeheader()
                    writer.writerows(self.samples)
            
            self.logger.info(f"Wrote {len(self.samples)} samples to {output_path}")
            
        except Exception as e:
            self.logger.error(f"Error writing output file: {str(e)}")
            return False
        
        return True
    
    def _print_samples(self):
        if not self.samples:
            return
        
        print(f"Sample of {len(self.samples)} DOIs:")
        print("------------------------------")
        
        for i, sample in enumerate(self.samples, 1):
            print(f"{i}. DOI: {sample.get('doi', 'N/A')}")
            print(f"   State: {sample.get('state', 'N/A')}")
            print(f"   Client: {sample.get('client_id', 'N/A')}")
            print(f"   Updated: {sample.get('updated', 'N/A')}")
            print("------------------------------")


def parse_date(date_str):
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_str}. Use YYYY-MM-DD")


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Sample DOIs from DataCite CSV files with filtering options'
    )
    
    parser.add_argument('-i', '--input-dir', required=True,
                        help='Directory containing DataCite CSV files (organized in YYYY-MM subdirectories)')
    
    parser.add_argument('-o', '--output-file',
                        help='Output file for sampled DOIs (CSV format, use .gz extension for compressed output)')
    
    parser.add_argument('-s', '--sample-size', type=int,
                        help='Number of DOIs to sample (if not specified, returns all matching DOIs)')
    
    parser.add_argument('-p', '--provider',
                        help='Filter by provider ID (first part of client_id)')
    
    parser.add_argument('-c', '--client',
                        help='Filter by client ID (second part of client_id)')
    
    parser.add_argument('--prefix',
                        help='Filter by DOI prefix (e.g., 10.5555)')
    
    parser.add_argument('--start-date', type=parse_date,
                        help='Filter by start date (YYYY-MM-DD)')
    
    parser.add_argument('--end-date', type=parse_date,
                        help='Filter by end date (YYYY-MM-DD)')
    
    parser.add_argument('-l', '--log-level', default='INFO',
                        help='Logging level (INFO, DEBUG, etc.)')
    
    parser.add_argument('--seed', type=int,
                        help='Random seed for reproducible sampling')
    
    return parser.parse_args()


def main():
    try:
        config = parse_arguments()
        if config.seed is not None:
            random.seed(config.seed)
        sampler = DoiSampler(config)
        if sampler.scan_directory() and sampler.finalize_sample():
            logging.info("DOI sampling completed successfully")
        else:
            logging.error("DOI sampling failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logging.info("Operation cancelled by user")
        sys.exit(130)
    except Exception as e:
        logging.error(f"Application error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()