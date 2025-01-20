import io
import os
import sys
import json
import gzip
import shutil
import hashlib
import logging
import argparse
import orjson
import requests
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from multiprocessing import Pool, cpu_count, Manager


class LoggerSetup:
    LOGGER_NAME = 'datacite_datafile_processor'

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


class ArgumentConfig:
    def __init__(self):
        self.input_dir = None
        self.output_dir = None
        self.cache_dir = None
        self.log_level = None
        self.all = False
        self.providers = []
        self.clients = []

    @classmethod
    def parse_arguments(cls):
        parser = argparse.ArgumentParser(
            description='Process DataCite metadata files into hierarchical directory structure'
        )

        parser.add_argument('--input-dir', '-i', required=True,
                            help='Directory containing DataCite data files (.jsonl.gz or .json.lz).')
        parser.add_argument('--output-dir', '-o', required=True,
                            help='Output directory for data.')
        parser.add_argument('--cache-dir', '-c',
                            help='Directory for caching API responses (optional).')
        parser.add_argument('--log-level', '-l', default='INFO',
                            help='Logging level (INFO, DEBUG, etc.).')

        mode_group = parser.add_mutually_exclusive_group(required=True)
        mode_group.add_argument('-a', '--all', action='store_true',
                                help='Process all records.')
        mode_group.add_argument('-p', '--providers', nargs='+',
                                help='Process only records for the given provider ID(s).')
        mode_group.add_argument('-r', '--clients', nargs='+',
                                help='Process only records for the given repositories/client ID(s).')

        args = parser.parse_args()

        config = cls()
        config.input_dir = args.input_dir
        config.output_dir = args.output_dir
        config.cache_dir = args.cache_dir
        config.log_level = args.log_level

        config.all = args.all
        if args.providers:
            config.providers = args.providers
        if args.clients:
            config.clients = args.clients

        return config


class DirectoryManager:
    def __init__(self, output_dir):
        self.output_dir = Path(output_dir)
        self.logger = logging.getLogger('datacite.directory_manager')
        self.created_dirs = set()

    def _mkdir_once(self, path: Path):
        if path not in self.created_dirs:
            path.mkdir(parents=True, exist_ok=True)
            self.created_dirs.add(path)

    def setup_base_directory(self):
        try:
            self._mkdir_once(self.output_dir)
            hashed_records_dir = self.output_dir / "hashed_records"
            self._mkdir_once(hashed_records_dir)
            return True
        except Exception as e:
            self.logger.error(f"Error creating base directory: {str(e)}")
            return False

    def get_provider_directory(self, provider_id):
        provider_dir = self.output_dir / provider_id
        self._mkdir_once(provider_dir)
        return provider_dir

    def get_client_directory(self, provider_id, client_id):
        provider_dir = self.get_provider_directory(provider_id)
        client_dir = provider_dir / client_id
        self._mkdir_once(client_dir)
        return client_dir

    def write_provider_data(self, provider_id, attributes, stats=None):
        provider_dir = self.get_provider_directory(provider_id)
        try:
            with open(provider_dir / 'provider_attributes.json', 'wb', buffering=2**20) as f:
                f.write(orjson.dumps(attributes, option=orjson.OPT_INDENT_2))

            if stats is not None:
                with open(provider_dir / 'provider_stats.json', 'wb', buffering=2**20) as f:
                    f.write(orjson.dumps(stats, option=orjson.OPT_INDENT_2))

            return True
        except Exception as e:
            self.logger.error(f"Error writing provider data for {provider_id}: {str(e)}")
            return False

    def write_client_data(self, provider_id, client_id, attributes, stats=None):
        client_dir = self.get_client_directory(provider_id, client_id)
        try:
            with open(client_dir / 'client_attributes.json', 'wb', buffering=2**20) as f:
                f.write(orjson.dumps(attributes, option=orjson.OPT_INDENT_2))

            if stats is not None:
                with open(client_dir / 'client_stats.json', 'wb', buffering=2**20) as f:
                    f.write(orjson.dumps(stats, option=orjson.OPT_INDENT_2))

            return True
        except Exception as e:
            self.logger.error(f"Error writing client data for {client_id}: {str(e)}")
            return False

    def _stable_subdir_name(self, provider_id, client_id):
        key = f"{provider_id}:{client_id}"
        md5_hex = hashlib.md5(key.encode('utf-8')).hexdigest()
        return md5_hex[:2]

    def get_hashed_records_file(self, provider_id, client_id):
        hashed_records_dir = self.output_dir / "hashed_records"
        hashed_subdir_name = self._stable_subdir_name(provider_id, client_id)
        hashed_subdir = hashed_records_dir / hashed_subdir_name
        self._mkdir_once(hashed_subdir)

        file_name = f"{provider_id}_{client_id}.jsonl"
        return hashed_subdir / file_name


class FileWriter:
    def __init__(self, directory_manager, batch_size=500_000):
        self.directory_manager = directory_manager
        self.batch_size = batch_size
        self.logger = logging.getLogger('datacite.file_writer')
        self.record_buffers = defaultdict(list)
        self.buffer_count = 0

    def add_to_batch(self, provider_id, client_id, record):
        if not provider_id or not client_id:
            return
        self.record_buffers[(provider_id, client_id)].append(record)
        self.buffer_count += 1

        if self.buffer_count >= self.batch_size:
            self.flush_batch()

    def flush_batch(self):
        if not self.record_buffers:
            return

        for (provider_id, client_id), records in self.record_buffers.items():
            filepath = self.directory_manager.get_hashed_records_file(provider_id, client_id)
            try:
                with open(filepath, 'ab', buffering=2**20) as f:
                    for rec in records:
                        f.write(orjson.dumps(rec))
                        f.write(b"\n")
            except Exception as e:
                self.logger.error(f"Error writing to {filepath}: {str(e)}")

        self.record_buffers.clear()
        self.buffer_count = 0

    def write_provider_metadata(self, provider_id, attributes, stats=None):
        return self.directory_manager.write_provider_data(provider_id, attributes, stats)

    def write_client_metadata(self, provider_id, client_id, attributes, stats=None):
        return self.directory_manager.write_client_data(provider_id, client_id, attributes, stats)


class DataCiteAPIClient:
    def __init__(self, cache_dir=None):
        self.base_url = 'https://api.datacite.org'
        self.cache_dir = Path(cache_dir) if cache_dir else None
        self.logger = logging.getLogger('datacite.api_client')
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_all_pages(self, endpoint, page_size=1000, include_prefixes=True):
        cache_file = None
        if self.cache_dir:
            cache_file = self.cache_dir / f"{endpoint}.json"
            if cache_file.exists():
                self.logger.info(f"Loading cached {endpoint} data")
                with open(cache_file, 'r') as f:
                    return json.load(f)

        self.logger.info(f"Fetching ALL from /{endpoint} ...")
        all_items = []
        page = 1
        total_pages = 1
        while page <= total_pages:
            url = f"{self.base_url}/{endpoint}"
            params = {
                'page[size]': page_size,
                'page[number]': page
            }
            if include_prefixes:
                params['include'] = 'prefixes'

            response = requests.get(url, params=params)
            if response.status_code != 200:
                raise Exception(f"API request failed: {response.status_code}")

            data = response.json()
            items = data.get('data', [])
            all_items.extend(items)

            meta = data.get('meta', {})
            total_pages = meta.get('totalPages', 1)
            total = meta.get('total', 0)
            self.logger.info(f"Fetched page {page}/{total_pages} - got {len(items)} items "
                             f"(cumulative {len(all_items)}/{total}).")
            page += 1

        if cache_file:
            with open(cache_file, 'w') as f:
                json.dump(all_items, f)

        return all_items

    def get_provider(self, provider_id):
        if self.cache_dir:
            cache_file = self.cache_dir / f"provider_{provider_id}.json"
            if cache_file.exists():
                with open(cache_file, 'r') as f:
                    return json.load(f)

        url = f"{self.base_url}/providers/{provider_id}"
        self.logger.info(f"Fetching single provider: {url}")
        resp = requests.get(url)
        if resp.status_code != 200:
            self.logger.warning(f"Failed to fetch provider {provider_id}: {resp.status_code}")
            return None

        data = resp.json().get('data')
        if data is None:
            return None

        if self.cache_dir:
            with open(cache_file, 'w') as f:
                json.dump(data, f)

        return data

    def get_client(self, client_id):
        if self.cache_dir:
            cache_file = self.cache_dir / f"client_{client_id}.json"
            if cache_file.exists():
                with open(cache_file, 'r') as f:
                    return json.load(f)

        url = f"{self.base_url}/clients/{client_id}"
        self.logger.info(f"Fetching single client: {url}")
        resp = requests.get(url)
        if resp.status_code != 200:
            self.logger.warning(f"Failed to fetch client {client_id}: {resp.status_code}")
            return None

        data = resp.json().get('data')
        if data is None:
            return None

        if self.cache_dir:
            cache_file = self.cache_dir / f"client_{client_id}.json"
            with open(cache_file, 'w') as f:
                json.dump(data, f)

        return data

    def get_clients_for_provider(self, provider_id, page_size=1000):
        if self.cache_dir:
            cache_file = self.cache_dir / f"provider_{provider_id}_clients.json"
            if cache_file.exists():
                self.logger.info(f"Loading cached clients for provider {provider_id}")
                with open(cache_file, 'r') as f:
                    return json.load(f)

        all_items = []
        page = 1
        total_pages = 1

        while page <= total_pages:
            url = f"{self.base_url}/providers/{provider_id}/clients"
            params = {'page[size]': page_size, 'page[number]': page}
            resp = requests.get(url, params=params)
            if resp.status_code != 200:
                self.logger.warning(f"Failed to fetch clients for provider {provider_id}: {resp.status_code}")
                break
            data = resp.json()
            items = data.get('data', [])
            all_items.extend(items)
            meta = data.get('meta', {})
            total_pages = meta.get('totalPages', 1)
            total = meta.get('total', 0)
            self.logger.info(
                f"Fetched page {page}/{total_pages} of clients for provider {provider_id} "
                f"({len(all_items)}/{total} so far)."
            )
            page += 1

        if self.cache_dir:
            cache_file = self.cache_dir / f"provider_{provider_id}_clients.json"
            with open(cache_file, 'w') as f:
                json.dump(all_items, f)

        return all_items

    def get_providers(self):
        return self.get_all_pages('providers', include_prefixes=True)

    def get_clients(self):
        return self.get_all_pages('clients', include_prefixes=True)

    def get_providers_by_ids(self, provider_ids):
        results = []
        for pid in provider_ids:
            data = self.get_provider(pid)
            if data is not None:
                results.append(data)
        return results

    def get_clients_by_ids(self, client_ids):
        results = []
        for cid in client_ids:
            data = self.get_client(cid)
            if data is not None:
                results.append(data)
        return results

    def get_all_clients_for_providers(self, provider_ids):
        all_clients = []
        for pid in provider_ids:
            cdata = self.get_clients_for_provider(pid)
            all_clients.extend(cdata)
        return all_clients


class BatchGzipReader:
    def __init__(self, filepath, chunk_size=2**24):
        self.filepath = filepath
        self.logger = logging.getLogger('datacite.batch_reader')
        self.chunk_size = chunk_size

    def __iter__(self):
        buffer = bytearray()
        try:
            with gzip.open(self.filepath, 'rb') as gz_file:
                while True:
                    chunk = gz_file.read(self.chunk_size)
                    if not chunk:
                        break
                    buffer.extend(chunk)
                    lines = buffer.split(b'\n')
                    buffer = bytearray(lines.pop())

                    for line in lines:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            yield orjson.loads(line)
                        except orjson.JSONDecodeError as e:
                            self.logger.warning(
                                f"JSON decode error in {self.filepath}: {str(e)}"
                            )

                if buffer.strip():
                    try:
                        yield orjson.loads(buffer.strip())
                    except orjson.JSONDecodeError as e:
                        self.logger.warning(
                            f"JSON decode error (leftover) in {self.filepath}: {str(e)}"
                        )
        except Exception as e:
            self.logger.error(f"Error reading gzip file {self.filepath}: {str(e)}")


class FileProcessor:
    def __init__(self, file_writer, config, counter=None, lock=None, total_files=None):
        self.file_writer = file_writer
        self.config = config
        self._counter = counter
        self._lock = lock
        self._total_files = total_files
        self.logger = logging.getLogger('datacite.file_processor')

    def log_progress(self, message):
        if self._lock:
            with self._lock:
                self.logger.info(message)
        else:
            self.logger.info(message)

    def _in_scope(self, provider_id, client_id):
        """Filter by user mode."""
        if self.config.all:
            return True
        elif self.config.providers:
            return provider_id in self.config.providers
        elif self.config.clients:
            return client_id in self.config.clients
        return False

    def process_file(self, filepath):
        try:
            processed_count = 0
            skipped_count = 0
            reader = BatchGzipReader(filepath)

            for item in reader:
                if item.get('attributes', {}).get('state') != 'findable':
                    skipped_count += 1
                    continue

                relationships = item.get('relationships', {})
                client_id = relationships.get('client', {}).get('data', {}).get('id')
                provider_id = relationships.get('provider', {}).get('data', {}).get('id')
                if not client_id or not provider_id:
                    skipped_count += 1
                    continue

                if not self._in_scope(provider_id, client_id):
                    skipped_count += 1
                    continue

                self.file_writer.add_to_batch(provider_id, client_id, item)
                processed_count += 1

            self.file_writer.flush_batch()

            if self._counter is not None and self._lock and self._total_files:
                with self._lock:
                    self._counter.value += 1
                    current_count = self._counter.value
                self.log_progress(
                    f"Completed {current_count}/{self._total_files} "
                    f"({Path(filepath).name}): {processed_count} findable, {skipped_count} skipped"
                )
            else:
                self.logger.info(
                    f"Processed file {filepath}: {processed_count} findable, {skipped_count} skipped"
                )

        except Exception as e:
            self.logger.error(f"Error processing file {filepath}: {str(e)}")


class FileScanner:
    def __init__(self):
        self.logger = logging.getLogger('datacite.file_scanner')

    def scan_jsonl_files(self, directory):
        directory = Path(directory)
        if not directory.is_dir():
            self.logger.error(f"Input directory is not valid: {directory}")
            return {'files': []}

        files_gz = list(directory.rglob('*.jsonl.gz'))
        files_lz = list(directory.rglob('*.json.lz'))
        all_files = set(files_gz + files_lz)

        if not all_files:
            self.logger.warning(f"No .jsonl.gz or .json.lz files found in {directory}")
            return {'files': []}

        return {'files': [str(f) for f in all_files]}


class RecordReorganizer:
    def __init__(self, directory_manager):
        self.directory_manager = directory_manager
        self.logger = logging.getLogger('datacite.record_reorganizer')

    def move_hashed_files(self, compress=False):
        hashed_dir = self.directory_manager.output_dir / "hashed_records"
        if not hashed_dir.is_dir():
            self.logger.info("No hashed_records directory found; skipping reorganization.")
            return

        self.logger.info(f"Reorganizing hashed files from {hashed_dir}...")

        for subdir in hashed_dir.iterdir():
            if not subdir.is_dir():
                continue

            for hashed_file in subdir.glob("*.jsonl"):
                filename = hashed_file.name
                if "_" not in filename:
                    continue

                stem = hashed_file.stem
                parts = stem.split("_", 1)
                if len(parts) != 2:
                    continue

                provider_id, client_id = parts
                client_dir = self.directory_manager.get_client_directory(provider_id, client_id)
                records_dir = client_dir / "records"
                self.directory_manager._mkdir_once(records_dir)

                if compress:
                    final_path = records_dir / "records.jsonl.gz"
                    try:
                        with open(hashed_file, 'rb') as infile, gzip.open(final_path, 'wb') as outfile:
                            shutil.copyfileobj(infile, outfile)
                        hashed_file.unlink()
                    except Exception as e:
                        self.logger.error(f"Error compressing {hashed_file} -> {final_path}: {str(e)}")
                else:
                    final_path = records_dir / "records.jsonl"
                    try:
                        hashed_file.rename(final_path)
                    except Exception as e:
                        self.logger.error(f"Error moving {hashed_file} -> {final_path}: {str(e)}")

        try:
            shutil.rmtree(hashed_dir)
            self.logger.info("Removed temporary hashed_records directory.")
        except Exception as e:
            self.logger.warning(f"Unable to remove hashed_records directory: {e}")


class DataCiteDataFileProcessor:
    def __init__(self):
        self.logger = None

    def init_worker(self, counter, lock, total_files):
        global _counter, _lock, _total_files
        _counter = counter
        _lock = lock
        _total_files = total_files

    def run(self):
        try:
            config = ArgumentConfig.parse_arguments()
            self.logger = LoggerSetup.configure(config.log_level)

            directory_manager = DirectoryManager(config.output_dir)
            if not directory_manager.setup_base_directory():
                self.logger.error("Failed to create base output directory.")
                return 1

            api_client = DataCiteAPIClient(cache_dir=config.cache_dir)

            if config.all:
                self.logger.info("Fetching ALL providers and clients...")
                providers_data = api_client.get_providers()
                clients_data = api_client.get_clients()

            elif config.providers:
                self.logger.info(f"Fetching only for providers {config.providers}")
                providers_data = api_client.get_providers_by_ids(config.providers)
                provider_ids_we_have = {p['id'] for p in providers_data if p is not None}

                clients_data = []
                for pid in provider_ids_we_have:
                    pclients = api_client.get_clients_for_provider(pid)
                    clients_data.extend(pclients)

            else:
                self.logger.info(f"Fetching only for clients {config.clients}")
                clients_data = api_client.get_clients_by_ids(config.clients)

                provider_ids = set()
                for c in clients_data:
                    prov_id = c.get('relationships', {}).get('provider', {}).get('data', {}).get('id')
                    if prov_id:
                        provider_ids.add(prov_id)

                providers_data = api_client.get_providers_by_ids(provider_ids)

            provider_map = {}
            for p in providers_data:
                if not p:
                    continue
                pid = p.get('id')
                provider_map[pid] = p.get('attributes', {})

            client_map = {}
            for c in clients_data:
                if not c:
                    continue
                cid = c.get('id')
                provider_id = c.get('relationships', {}).get('provider', {}).get('data', {}).get('id')
                client_map[cid] = {
                    'provider_id': provider_id,
                    'attributes': c.get('attributes', {})
                }

            self.logger.info("Writing provider metadata...")
            for pid, attr in provider_map.items():
                directory_manager.write_provider_data(pid, attr)

            self.logger.info("Writing client metadata...")
            for cid, info in client_map.items():
                prov_id = info['provider_id']
                if prov_id is not None:
                    directory_manager.write_client_data(prov_id, cid, info['attributes'])

            file_scanner = FileScanner()
            files_info = file_scanner.scan_jsonl_files(config.input_dir)
            files_to_process = files_info['files']
            total_files = len(files_to_process)

            if total_files == 0:
                self.logger.error(f"No suitable .jsonl.gz or .json.lz files found in {config.input_dir}")
                return 1

            self.logger.info(f"Found {total_files} files to process.")

            manager = Manager()
            counter = manager.Value('i', 0)
            lock = manager.Lock()

            file_writer = FileWriter(directory_manager, batch_size=500_000)
            processes_count = max(1, cpu_count())
            self.logger.info(f"Using up to {processes_count} processes.")

            pool = None
            try:
                pool = Pool(
                    processes=processes_count,
                    initializer=self.init_worker,
                    initargs=(counter, lock, total_files)
                )

                file_processor = FileProcessor(
                    file_writer=file_writer,
                    config=config,
                    counter=counter,
                    lock=lock,
                    total_files=total_files
                )

                for _ in pool.imap_unordered(file_processor.process_file, files_to_process):
                    pass

            finally:
                if pool:
                    pool.close()
                    pool.join()

            reorganizer = RecordReorganizer(directory_manager)
            reorganizer.move_hashed_files(compress=(not config.all))

            self.logger.info("Processing completed successfully.")
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