# DataCite Relationship Finder

A tool for identifying DataCite records where a relatedIdentifier matches a DOI from an input list. The script processes DataCite data file records (in JSONL.GZ format) and finds instances where a `relatedIdentifier` in a DataCite record matches a DOI from an input CSV file. When matches are found, the script outputs the relationship details along with the original input data.

## Installation

```bash
pip install orjson pyahocorasick
```

## Usage

```bash
python datacite-relationship-finder.py -c INPUT_CSV -i INPUT_DIR [-d DOI_COLUMN] [-o OUTPUT_CSV]
```

### Arguments

- `-c, --input-csv` (required): Path to input CSV file containing DOIs to search for
- `-i, --input-dir` (required): Directory containing DataCite JSONL.GZ records
- `-d, --doi-column` (optional): Name of DOI column in the input CSV (default: "doi")
- `-o, --output-csv` (optional): Output CSV file path (default: "related_identifiers.csv")

## Example

```bash
python datacite-relationship-finder.py --input-csv my_dois.csv --input-dir /path/to/datacite/files --doi-column doi_value --output-csv results.csv
```

## Output

The script generates a CSV file containing:
- DataCite relationship details (related identifier, relation type, resource type)
- All columns from the original input CSV