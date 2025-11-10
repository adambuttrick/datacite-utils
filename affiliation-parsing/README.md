# Affiliation Parser

Aggregates and analyzes affiliation data from DataCite metadata, tracking ROR identifier assignments and normalized affiliation distributions.

## Input

Requires CSV exported from [fast-field-parser](https://github.com/adambuttrick/datacite-utils/tree/main/fast-field-parser) containing affiliation fields:

```bash
fast-field-parser -i ../datacite-snapshot -o affiliation_data.csv -f "creators.affiliation.name,creators.affiliation.affiliationIdentifier,creators.affiliation.affiliationIdentifierScheme,contributors.affiliation.name,contributors.affiliation.affiliationIdentifier,contributors.affiliation.affiliationIdentifierScheme"
```

## Outputs

Generates six JSON files:

- `affiliations_with_ror.json` - Affiliations that have ROR identifiers
- `affiliations_without_ror.json` - Affiliations missing ROR identifiers
- `affiliation_overlap.json` - Affiliations appearing both with and without ROR IDs
- `normalized_distribution.json` - Distribution of normalized affiliation strings
- `normalized_affiliation_doi_distribution.json` - DOIs associated with each normalized affiliation
- `ror_identifier_doi_distribution.json` - DOIs associated with each ROR identifier

Each record includes occurrence counts and breakdowns by DataCite provider/client.

## Usage

```bash
cargo run -- -i affiliation_data.csv -o output_dir/

# Custom output paths
cargo run -- -i affiliation_data.csv \
  --aff_with_ror with_ror.json \
  --aff_without_ror without_ror.json

# Adjust logging
cargo run -- -i affiliation_data.csv -o output_dir/ \
  --log_level DEBUG \
  --log_every 1000000
```

## Options

- `-i, --input <CSV>` - Input CSV from fast-field-parser (required)
- `-o, --output_dir <DIR>` - Directory for default output filenames
- `--aff_with_ror <PATH>` - Override output path for affiliations with ROR IDs
- `--aff_without_ror <PATH>` - Override output path for affiliations without ROR IDs
- `--overlap_output <PATH>` - Override output path for overlap stats
- `--distribution_output <PATH>` - Override output path for normalized distribution
- `--normalized_doi_output <PATH>` - Override output path for normalized affiliation DOI distribution
- `--identifier_doi_output <PATH>` - Override output path for ROR identifier DOI distribution
- `--log_level <LEVEL>` - Log level: ERROR, WARN, INFO, DEBUG, TRACE (default: INFO)
- `--log_every <N>` - Log progress every N rows (default: 5000000)
