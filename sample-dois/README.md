# DOI Sampler

A utility for sampling and filtering DOIs from DataCite public data file CSVs.


## Usage

```bash
python sample_dois.py -i <input_directory> [options]
```

### Required Arguments

- `-i, --input-dir`: Directory containing DataCite public data file

### Optional Arguments

- `-o, --output-file`: Output file for sampled DOIs (default: doi_samples.csv)
- `-s, --sample-size`: Number of DOIs to sample (if not specified, returns all matching DOIs)
- `-p, --provider`: Filter by provider ID (first part of client_id)
- `-c, --client`: Filter by client ID (second part of client_id)
- `--prefix`: Filter by DOI prefix (e.g., 10.5555)
- `--start-date`: Filter by start date (YYYY-MM-DD)
- `--end-date`: Filter by end date (YYYY-MM-DD)
- `-l, --log-level`: Logging level (INFO, DEBUG, etc.) (default: INFO)
- `--seed`: Random seed for reproducible sampling

## Output

The script writes matched DOI records to an uncompressed CSV file (default: doi_samples.csv).

## Example

```bash
python sample_dois.py -i /data/datacite/archives -o custom_samples.csv -s 1000 --prefix 10.5555 --start-date 2023-01-01 --seed 42
```

This samples 1000 random DOIs with prefix 10.5555 created after January 1, 2023.
