# DataCite DOI Matcher

Rust utility for finding DOIs in the related identifier field of the DataCite data file.


## Installation

```
cargo install find-related-identifiers
```

## Usage

```
find-related-identifiers --mapping-csv INPUT_MAPPING.csv --input-dir DATACITE_FILES/ --output-csv RESULTS.csv [OPTIONS]
```

### Required Arguments

- `-m, --mapping-csv <FILE>`: Input CSV with DOIs to match
- `-i, --input-dir <DIR>`: Directory containing DataCite JSON files (*.jsonl.gz)
- `-o, --output-csv <FILE>`: Output CSV file for matched results

### Optional Arguments

- `-t, --threads <NUM>`: Number of worker threads (default: auto)
- `-b, --batch-size <NUM>`: Result batch size for writing (default: 10000)
- `-r, --relation-types <TYPES>`: Filter by relation types (comma-separated)
- `--log-level <LEVEL>`: Log level (default: INFO)
- `-h, --help`: Show help
- `-V, --version`: Show version

## Output Format

The output CSV contains the following columns:
- `input_doi`: The normalized DOI from the input mapping
- `datacite_record_doi`: The DOI of the matching DataCite record
- `matched_relation_type`: The relation type between the records
- `datacite_record_resource_type`: Resource type of the DataCite record
- `datacite_record_resource_type_general`: General resource type

## Examples

Match all DOIs with any relation type:
```
find-related-identifiers -m dois.csv -i /data/datacite/ -o matches.csv
```

Filter for specific relation types:
```
find-related-identifiers -m dois.csv -i /data/datacite/ -o matches.csv -r "IsCitedBy,References"
```

Use 8 worker threads:
```
find-related-identifiers -m dois.csv -i /data/datacite/ -o matches.csv -t 8
```