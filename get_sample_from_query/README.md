# Get Sample from Query

Queries the DataCite API to collect a random sample of DOIs for a given query, with optional grouping by client, provider, or resource type.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
python get_sample_from_query.py -q QUERY -s SAMPLE_SIZE 
                               [-g {client,provider,resource-type}] [-n NUM_GROUPS]
                               [-d DELAY] [-o OUTPUT_DIR]
```

### Required Arguments
- `-q, --query`: Query string (e.g., "client-id=datacite.datacite" or "query=climate")
- `-s, --sample_size`: Number of records per group (or total if no grouping)

### Optional Arguments
- `-g, --group_by`: Group results by client, provider, or resource-type
- `-n, --num_groups`: Number of groups to sample (default: 20)
- `-d, --delay`: Delay between API requests in seconds (default: 1.0)
- `-o, --output_dir`: Output directory (default: timestamp_normalized_query)

## Examples

Get 100 random DOIs about climate:
```bash
python get_sample_from_query.py -q "query=climate" -s 100
```

Sample 50 DOIs each from 10 different providers:
```bash
python get_sample_from_query.py -q "query=physics" -s 50 -g provider -n 10
```

Get 25 DOIs from each resource type with custom output directory:
```bash
python get_sample_from_query.py -q "client-id=bl.imperial" -s 25 \
  -g resource-type -o imperial_samples
```

## Output Structure

```
output_dir/
├── csv/
│   └── timestamp_query.csv
└── json/
    └── DOI_PREFIX/
        └── DOI.json
```