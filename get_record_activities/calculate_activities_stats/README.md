# calculate_activities_stats.py

Calculates and visualizes statistics for research data changes.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
python calculate_activities_stats.py -c CHANGES -s SAMPLE -u UNCHANGED -o OUTPUT [-p PLOTS] [-d STATS_DIR]
```

Required arguments:
- `-c, --changes`: Path to all_changes.csv file
- `-s, --sample`: Path to sample.csv file
- `-u, --unchanged`: Path to unchanged_dois.csv file
- `-o, --output`: Path to output index file

Optional arguments:
- `-p, --plots`: Directory to save plot images (default: "charts")
- `-d, --stats-dir`: Directory to save stats files (default: "stats")

## Output

The script generates:
1. CSV statistics files in the stats directory:
   - overview.csv: General statistics
   - change_types.csv: Types of changes and frequencies
   - action_types.csv: Types of actions and frequencies
   - monthly.csv: Monthly distribution of changes
   - field_analysis.csv: Detailed field-specific changes
   - comparison.csv: Comparison between datasets
   - inconsistencies.csv: Detected inconsistencies (if any)

2. Visualization charts in the plots directory:
   - change_types_pie.png: Distribution of change types
   - action_types_bar.png: Frequency of action types
   - monthly_distribution.png: Changes over time
   - changes_per_doi_histogram.png: Distribution of changes per DOI
   - field_operations.png: Operations by field type
   - doi_comparison.png: Comparison of DOI metrics

3. An index file mapping all outputs.
