# Fast Affiliation Parser

A high-performance Rust tool for extracting and analyzing affiliation metadata from DataCite snapshot files.

## Features

- Multithreaded processing with Rayon
- Processes compressed (`.jsonl.gz`) files recursively
- Extracts affiliations from creators and contributors
- Generates statistics on providers, clients, and unique entities
- Memory-efficient batch processing
- Platform-specific memory usage reporting
- Progress visualization with ETA

## Installation

```bash
cargo install --path .
```

## Usage

```bash
fast-affiliation-parser --input <INPUT_DIR> --output <OUTPUT_FILE_OR_DIR> [OPTIONS]
```

### Options

```
-i, --input <INPUT>              Directory containing JSONL.gz files (required)
-o, --output <OUTPUT>            Output CSV file or directory [default: affiliation_metadata.csv]
-l, --log-level <LEVEL>          Logging level (DEBUG, INFO, WARN, ERROR) [default: INFO]
-t, --threads <THREADS>          Number of threads to use (0 for auto) [default: 0]
-b, --batch-size <SIZE>          Number of records per batch [default: 10000]
-s, --stats-interval <INTERVAL>  Interval in seconds to log statistics [default: 60]
-g, --organize                   Organize output by provider/client
    --provider <PROVIDER_ID>     Filter by provider ID
    --client <CLIENT_ID>         Filter by client ID
    --max-open-files <MAX_FILES> Maximum open files when using --organize [default: 100]
```

## Output Format

CSV with the following columns:
- doi
- name
- category
- role
- affiliation_name
- affiliation_id
- affiliation_scheme
- provider_id
- client_id

## Examples

Basic usage:
```bash
fast-affiliation-parser -i ./data -o affiliations.csv
```

Organize by provider/client:
```bash
fast-affiliation-parser -i ./data -o ./output -g
```

Filter by provider:
```bash
fast-affiliation-parser -i ./data -o affiliations.csv --provider example-provider
```

## Performance Tuning

- Adjust `--threads` to match your CPU cores
- Modify `--batch-size` based on available memory
- Set `--max-open-files` to control file handle usage when using `--organize`