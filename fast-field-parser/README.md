# Fast Field Parser

High-performance Rust tool for extracting and analyzing field data from DataCite snapshot files.

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
fast-field-parser --input <INPUT_DIR> --output <OUTPUT_FILE_OR_DIR> --fields <FIELDS> [OPTIONS]
```

### Options

```
-i, --input <INPUT>              Directory containing JSONL.gz files (required)
-o, --output <OUTPUT>            Output CSV file or directory [default: field_data.csv]
-l, --log-level <LEVEL>          Logging level (DEBUG, INFO, WARN, ERROR) [default: INFO]
-t, --threads <THREADS>          Number of threads to use (0 for auto) [default: 0]
-b, --batch-size <SIZE>          Number of records per batch [default: 5000]
-g, --organize                   Organize output by provider/client using an LRU cache for file handles
    --provider <PROVIDER_ID>     Filter by provider ID
    --client <CLIENT_ID>         Filter by client ID
    --resource-types <TYPES>     Comma-separated list of resource types to include (e.g., 'Dataset,Text')
    --require-all-fields         Only include records that contain all specified top-level fields
    --field-value-filter <FILTER> Filter records where a field has a specific value (e.g., 'relatedIdentifiers.relationType=IsSupplementTo'). Can be used multiple times.
    --field-does-not-exist <FIELD> Filter records where a field must NOT exist or be empty. Can be used multiple times.
    --max-open-files <MAX_FILES> Maximum number of open files when using --organize [default: 100]
-f, --fields <FIELDS>            Comma-separated list of fields to extract [default: creators.name]
```

## Output Format

CSV with the following columns:
- doi
- provider_id
- client_id
- field_name
- subfield_path
- value

## Examples

Basic usage to extract creator names:
```bash
fast-field-parser -i ./data -o creator_data.csv -f creators.name
```

Extract multiple fields:
```bash
fast-field-parser -i ./data -o metadata.csv -f "doi,creators.name,titles.title,subjects.subject"
```

Extract nested fields:
```bash
fast-field-parser -i ./data -o affiliations.csv -f "creators.affiliation.name,contributors.affiliation.name"
```

Organize output by provider/client:
```bash
fast-field-parser -i ./data -o ./output -g -f "doi,creators.name"
```

Filter by provider:
```bash
fast-field-parser -i ./data -o filtered_data.csv -f "doi,creators.name" --provider example-provider
```

Filter by resource type:
```bash
fast-field-parser -i ./data -o datasets.csv -f "doi,creators.name" --resource-types Dataset,Collection
```

Filter by field value:
```bash
fast-field-parser -i ./data -o supplements.csv -f "doi,relatedIdentifiers.relatedIdentifier" --field-value-filter "relatedIdentifiers.relationType=IsSupplementTo"
```

Filter records missing a field:
```bash
fast-field-parser -i ./data -o no_funding.csv -f "doi,creators.name" --field-does-not-exist fundingReferences
```

Require all specified fields to be present:
```bash
fast-field-parser -i ./data -o complete_records.csv -f "doi,creators.name,titles.title" --require-all-fields
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