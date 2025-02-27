# Metadata Health API DataCite Data File Processor

Processes DataCite data files into JSON format used in the Metadata Health API.

## Requirements
```bash
pip install -r requirements.txt
```

## Usage
```bash
python process_data_file_for_metadata_health_api.py -i INPUT_DIR -o OUTPUT_DIR [options]
```

### Required Arguments
- `-i, --input-dir`: Directory containing DataCite .jsonl.gz files
- `-o, --output-dir`: Output directory for JSON output files

### Optional Arguments
- `-c, --cache-dir`: Cache directory for API responses
- `-l, --log-level`: Logging level (default: INFO)

## Output Files
Generates four JSON files:
- `providers_attributes.json`: Provider metadata
- `providers_stats.json`: Provider-level statistics
- `clients_attributes.json`: Client metadata
- `clients_stats.json`: Client-level statistics

## Output Schema

### Attributes Files Schema
```json
{
  "data": [
    {
      "id": "string",
      "type": "providers|clients",
      "attributes": {
        "symbol": "string",
        "name": "string",
        "displayName": "string",
        // ... other DataCite attributes
      },
      "relationships": {
        // For providers:
        "clients": ["string"], // Array of client IDs
        // For clients:
        "provider": "string"  // Provider ID
      }
    }
  ],
  "meta": {
    "total": "number",
    "timestamp": "string" // ISO 8601 format
  }
}
```

### Stats Files Schema
```json
{
  "data": [
    {
      "id": "string",
      "stats": {
        "summary": {
          "count": "number",
          "fields": {
            "identifier": {
              "count": "number",
              "instances": "number",
              "missing": "number",
              "fieldStatus": "mandatory|recommended|optional",
              "completeness": "number" // 0 to 1
            },
            // ... similar structure for all fields
          },
          "categories": {
            "mandatory": {
              "completeness": "number" // 0 to 1
            },
            "recommended": {
              "completeness": "number"
            },
            "optional": {
              "completeness": "number"
            }
          }
        },
        "byResourceType": {
          "resourceTypes": {
            "Dataset": {
              "count": "number",
              "categories": {
                // Same structure as summary.categories
              },
              "fields": {
                // Same structure as summary.fields
              }
            }
            // ... other resource types
          }
        }
      }
    }
  ],
  "meta": {
    "total": "number",
    "timestamp": "string" // ISO 8601 format
  }
}
```

### Notes

#### Mandatory Fields
- identifier
- creators
- titles
- publisher
- publicationYear
- resourceType

#### Recommended Fields
- subjects
- contributors
- date
- relatedIdentifiers
- description
- geoLocations

#### Optional Fields
- language
- alternateIdentifiers
- sizes
- formats
- version
- rights
- fundingReferences
- relatedItems

### Subfield Analysis
For fields with subfields (e.g., creators, contributors, relatedIdentifiers), additional statistics are provided about:
- Presence of identifiers (e.g., ORCID, ROR)
- Usage of controlled vocabularies
- Completeness of nested attributes
- Distribution of value types

### Aggregate Statistics
The output includes two special entries that are use to examine the quality and completeness of all DataCite metadata:
- Provider ID "aggregate": Statistics across all providers
- Client ID "aggregate.all": Statistics across all clients