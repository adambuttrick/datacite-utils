# DOI Metadata Provenance Tracker

A utility for tracking metadata changes to DataCite DOIs using the [activities API endpoint](https://support.datacite.org/docs/tracking-provenance).

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
python get_record_activities.py -i <input_file.csv> [options]
```

### Required Arguments

- `-i, --input-file`: CSV file containing DOI records (must include doi, state, client_id, and updated columns)

### Optional Arguments

- `-o, --output-dir`: Directory for output files (default: `<input_name>_output`)
- `-t, --threads`: Number of threads for parallel processing (default: 5)
- `-l, --log-level`: Logging level (DEBUG, INFO, WARNING, ERROR) (default: INFO)
- `--overwrite`: Overwrite existing output files

## Output

The script generates two files in the output directory:
- `all_changes.csv`: Records of all metadata changes
- `unchanged_dois.csv`: List of DOIs with no detected changes

## Example

```bash
python get_record_activities.py -i dois.csv -o results -t 10 --log-level DEBUG
```

## Rate Limiting

The script respects a polite version of DataCite API rate limits (1000 calls per 5 minutes) with automatic backoff and retry logic.
