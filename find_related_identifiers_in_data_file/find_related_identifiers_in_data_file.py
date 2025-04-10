import csv
import sys
import gzip
import orjson
import logging
import argparse
import ahocorasick
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("datacite-relationship-finder")


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Find DataCite records where a relatedIdentifier matches a DOI from an input CSV."
    )
    parser.add_argument(
        '-c', '--input-csv',
        required=True,
        help='Path to the input CSV file containing DOIs and other metadata.'
    )
    parser.add_argument(
        '-d', '--doi-column',
        default='doi',
        help='Name of the column in the input CSV that contains the DOI values to search for (Default: "doi").'
    )
    parser.add_argument(
        '-i', '--input-dir',
        required=True,
        help='Directory containing .jsonl.gz DataCite records.'
    )
    parser.add_argument(
        '-o', '--output-csv',
        default='related_identifiers.csv',
        help='Path to the output CSV file with match results (Default: "related_identifiers.csv").'
    )
    return parser.parse_args()


def load_input_data(csv_path, doi_column_name):
    rows = []
    fieldnames = []
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError(f"Input CSV '{csv_path}' appears to be empty or has no header.")
            fieldnames = reader.fieldnames
            if doi_column_name not in fieldnames:
                raise ValueError(
                    f"DOI column '{doi_column_name}' not found in input CSV header: {fieldnames}"
                )
            for row in reader:
                processed_row = {field: row.get(field, '') for field in fieldnames}
                rows.append(processed_row)
    except FileNotFoundError:
        logger.error(f"Input CSV file not found: {csv_path}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error reading input CSV {csv_path}: {e}")
        sys.exit(1)

    logger.info(f"Loaded {len(rows)} rows from {csv_path}.")
    return rows, fieldnames


def build_aho_corasick_automaton(input_data, doi_column_name):
    logger.info(f"Building Aho-Corasick automaton using DOIs from column '{doi_column_name}'...")
    A = ahocorasick.Automaton(ahocorasick.STORE_ANY)
    valid_dois = 0
    dois_added = set()

    for row_index, row in enumerate(input_data):
        doi_value = row.get(doi_column_name)
        if not doi_value:
            continue

        doi = str(doi_value).strip().lower()
        if not doi:
            continue

        if doi not in dois_added:
            A.add_word(doi, (doi, row))
            dois_added.add(doi)
            valid_dois += 1

    if valid_dois == 0:
        logger.error(f"No valid DOIs found in column '{doi_column_name}' in the input CSV.")
        sys.exit(1)

    A.make_automaton()
    logger.info(f"Built automaton with {valid_dois} unique, valid DOIs.")
    return A


def iter_datacite_records(jsonl_gz_path, chunk_size=2**24): # 16MB chunk size
    buffer = bytearray()
    line_count = 0
    try:
        with gzip.open(jsonl_gz_path, 'rb') as gz_file:
            while True:
                chunk = gz_file.read(chunk_size)
                if not chunk:
                    break
                buffer.extend(chunk)

                while True:
                    try:
                        split_pos = buffer.index(b'\n')
                        line = buffer[:split_pos].strip()
                        buffer = buffer[split_pos + 1:]
                    except ValueError:
                        break

                    if not line:
                        continue

                    line_count += 1
                    try:
                        yield orjson.loads(line)
                    except orjson.JSONDecodeError as ex:
                        logger.warning(f"L{line_count}: JSON decode error in {jsonl_gz_path}: {ex} - Skipping line.")
                    except Exception as e:
                         logger.warning(f"L{line_count}: Error processing line in {jsonl_gz_path}: {e} - Skipping line.")

            if buffer.strip():
                line_count += 1
                try:
                    yield orjson.loads(buffer.strip())
                except orjson.JSONDecodeError as ex:
                     logger.warning(f"L{line_count}: JSON decode error (final buffer) in {jsonl_gz_path}: {ex} - Skipping line.")
                except Exception as e:
                     logger.warning(f"L{line_count}: Error processing final buffer in {jsonl_gz_path}: {e} - Skipping line.")

    except FileNotFoundError:
        logger.error(f"DataCite file not found: {jsonl_gz_path}")
    except gzip.BadGzipFile:
         logger.error(f"Bad Gzip file: {jsonl_gz_path} - ممکن است فایل خراب یا ناقص باشد.")
    except Exception as e:
        logger.error(f"Error reading DataCite file {jsonl_gz_path}: {e}")


def find_matches_in_record(record, automaton):
    results = []
    attrs = record.get('attributes', {})

    related_identifiers = attrs.get('relatedIdentifiers') or []

    for rid_entry in related_identifiers:
        rid_value_raw = rid_entry.get('relatedIdentifier')
        relation_type = rid_entry.get('relationType', '')
        resource_type = rid_entry.get('resourceTypeGeneral', '')

        if not rid_value_raw:
            continue

        rid_value_norm = str(rid_value_raw).strip().lower()
        if not rid_value_norm:
             continue

        for end_idx, (matched_input_doi_norm, original_input_row) in automaton.iter(rid_value_norm):
            start_idx = end_idx - len(matched_input_doi_norm) + 1
            if start_idx == 0 and end_idx == len(rid_value_norm) - 1:
                match_data = {
                    "datacite_related_identifier": rid_value_raw,
                    "datacite_relation_type": relation_type,
                    "datacite_resource_type_general": resource_type
                }
                match_data.update(original_input_row)
                results.append(match_data)
                break

    return results


def main():
    args = parse_arguments()
    input_data, input_csv_headers = load_input_data(args.input_csv, args.doi_column)
    if not input_data:
        logger.info("No data loaded from input CSV. Exiting.")
        sys.exit(0)

    automaton = build_aho_corasick_automaton(input_data, args.doi_column)

    out_path = Path(args.output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    output_header = [
        'datacite_related_identifier',
        'datacite_relation_type',
        'datacite_resource_type_general'
    ] + input_csv_headers

    total_matches_found = 0
    processed_files = 0
    input_dir = Path(args.input_dir)

    # Find DataCite files
    gz_files = list(input_dir.rglob('*.jsonl.gz'))
    if not gz_files:
        logger.warning(f"No *.jsonl.gz files found in directory: {input_dir}")
        sys.exit(0)

    logger.info(f"Found {len(gz_files)} *.jsonl.gz files to process in {input_dir}")

    try:
        with open(out_path, 'w', encoding='utf-8') as out_file:
            writer = csv.writer(out_file)
            writer.writerow(output_header)

            for gzfile_path in gz_files:
                processed_files += 1
                logger.info(f"Processing file {processed_files}/{len(gz_files)}: {gzfile_path.name} ...")
                record_count_in_file = 0
                matches_in_file = 0

                for record in iter_datacite_records(gzfile_path):
                    record_count_in_file += 1
                    match_rows = find_matches_in_record(record, automaton)
                    if match_rows:
                        for row_data in match_rows:
                            output_row = [row_data.get(h, '') for h in output_header]
                            writer.writerow(output_row)
                            matches_in_file += 1

                total_matches_found += matches_in_file
                if record_count_in_file > 0:
                     logger.info(f"Finished {gzfile_path.name}. Found {matches_in_file} matches in {record_count_in_file} records.")
                else:
                     logger.info(f"Finished {gzfile_path.name}. No records successfully processed.")


    except IOError as e:
         logger.error(f"Error writing to output file {out_path}: {e}")
         sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred during processing: {e}")
        sys.exit(1)


    logger.info(f"Processing complete. Found a total of {total_matches_found} matches across {processed_files} files.")
    logger.info(f"Output written to {out_path}")

    return 0


if __name__ == '__main__':
    sys.exit(main())