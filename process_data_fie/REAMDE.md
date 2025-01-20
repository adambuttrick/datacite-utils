# DataCite Metadata File Processor

Processes DataCite metadata files into a hierarchical directory structure based on provider and client IDs.

⚠️ **Note:** DataCite data files can be very large. If processing all records or querying large providers, ensure you have sufficient disk space (100GB+ recommended) for both processing and output.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
python process_data_file.py [-h] --input-dir INPUT_DIR --output-dir OUTPUT_DIR 
                           [--cache-dir CACHE_DIR] [--log-level LOG_LEVEL]
                           (-a | -p PROVIDERS [PROVIDERS ...] | -r CLIENTS [CLIENTS ...])
```

### Required Arguments
- `--input-dir`: Directory containing DataCite data files (.jsonl.gz)
- `--output-dir`: Output directory for processed data

### Optional Arguments
- `--cache-dir`: Input directory for using cached API responses for provider and client data
- `--log-level`: Logging level (default: INFO)

### Processing Modes (mutually exclusive)
- `-a, --all`: Process all records
- `-p, --providers`: Process records for specific provider ID(s)
- `-r, --clients`: Process records for specific client ID(s)

## Examples

Process all records:
```bash
python process_data_file.py --input-dir /data/datacite --output-dir /data/processed -a
```

Process specific providers:
```bash
python process_data_file.py --input-dir /data/datacite --output-dir /data/processed \
  -p cern cdl
```

Process specific clients with debug logging:
```bash
python process_data_file.py --input-dir /data/datacite --output-dir /data/processed \
  -r bl.imperial bl.ucl --log-level DEBUG
```

## Output Structure

```
output_dir/
├── provider_id/
│   ├── provider_attributes.json
│   └── client_id/
│       ├── client_attributes.json
│       └── records/
│           └── records.jsonl[.gz]
```