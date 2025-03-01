# DataCite Field Extractor

A high-performance Rust tool for extracting and analyzing field data from DataCite snapshot files.

## Features

- Multithreaded processing with Rayon
- Processes compressed (`.jsonl.gz`) files recursively
- Extracts any field data from DataCite metadata
- Flexible field path specifications
- Generates statistics on providers, clients, and extracted fields
- Memory-efficient batch processing
- Platform-specific memory usage reporting
- Progress visualization with ETA

## Installation

```bash
cargo install --path .
```

## Usage

```bash
datacite-field-extractor --input <INPUT_DIR> --output <OUTPUT_FILE_OR_DIR> --fields <FIELDS> [OPTIONS]
```

### Options

```
-i, --input <INPUT>              Directory containing JSONL.gz files (required)
-o, --output <OUTPUT>            Output CSV file or directory [default: field_data.csv]
-l, --log-level <LEVEL>          Logging level (DEBUG, INFO, WARN, ERROR) [default: INFO]
-t, --threads <THREADS>          Number of threads to use (0 for auto) [default: 0]
-b, --batch-size <SIZE>          Number of records per batch [default: 10000]
-s, --stats-interval <INTERVAL>  Interval in seconds to log statistics [default: 60]
-g, --organize                   Organize output by provider/client
    --provider <PROVIDER_ID>     Filter by provider ID
    --client <CLIENT_ID>         Filter by client ID
    --max-open-files <MAX_FILES> Maximum open files when using --organize [default: 100]
-f, --fields <FIELDS>            Comma-separated list of fields to extract (e.g., 'creators.affiliation.name,titles') [default: creators]
```

## Output Format

CSV with the following columns:
- doi
- field_name
- subfield_path
- value
- provider_id
- client_id

## Examples

Basic usage to extract creator names:
```bash
datacite-field-extractor -i ./data -o creator_data.csv -f creators.name
```

Extract multiple fields:
```bash
datacite-field-extractor -i ./data -o metadata.csv -f "doi,creators.name,titles.title,subjects.subject"
```

Extract nested fields:
```bash
datacite-field-extractor -i ./data -o affiliations.csv -f "creators.affiliation.name,contributors.affiliation.name"
```

Organize output by provider/client:
```bash
datacite-field-extractor -i ./data -o ./output -g -f "doi,creators.name"
```

Filter by provider:
```bash
datacite-field-extractor -i ./data -o filtered_data.csv -f "doi,creators.name" --provider example-provider
```

## Field Path Specification

Fields are specified using dot notation to traverse the JSON structure. Some examples:

- `doi` - Extract the DOI
- `creators.name` - Extract names of all creators
- `creators.affiliation.name` - Extract affiliation names of all creators
- `titles.title` - Extract all titles
- `geoLocations.geoLocationPlace` - Extract geographic location places
- `rightsList.rights` - Extract rights statements

## Testing

Run the test suite:

```bash
cargo test
```

The tests verify correct extraction of various fields from a sample DataCite record containing all fields and subfields.

## Performance Tuning

- Adjust `--threads` to match your CPU cores
- Modify `--batch-size` based on available memory
- Set `--max-open-files` to control file handle usage when using `--organize`