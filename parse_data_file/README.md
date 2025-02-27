# DataCite Data File Parser

Parse DataCite data files into a hierarchical directory structure, with options for organizing by provider, client, and resource type.

## Installation
```bash
pip -r requirements.txt
```

## Usage
```bash
python general_process_data_file.py -i INPUT_DIR -o OUTPUT_DIR [options]
```

### Required Arguments
- `-i, --input-dir`: Directory containing DataCite .jsonl.gz/.json.lz files
- `-o, --output-dir`: Output directory for processed data

### Processing Mode (Required, Choose One)
- `-a, --all`: Process all records
- `-p, --providers`: Process specific provider ID(s)
- `-r, --clients`: Process specific client ID(s)

### Optional Arguments
- `-c, --cache-dir`: Cache directory for API responses
- `-l, --log-level`: Logging level (default: INFO)
- `-n, --processes`: Number of processes to use (default: number of CPU cores - 1)
- `-rtgo, --sort-rtg-only`: Sort by resourceTypeGeneral only
- `-rtgpc, --sort-provider-client-and-rtg`: Sort by provider/client and then resourceTypeGeneral

## Output Structure
- Standard: `provider_id/client_id/records/records.jsonl[.gz]`
- RTG-only mode: `resourceTypeGeneral/records.jsonl[.gz]`
- Provider/Client+RTG mode: `provider_id/client_id/resourceTypeGeneral/records.jsonl[.gz]`

Provider and client metadata are stored in respective directories as JSON files.