use std::io::Write;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::Command;
use flate2::write::GzEncoder;
use flate2::Compression;
use tempfile::tempdir;
use assert_cmd::prelude::*;

#[cfg(test)]
mod tests {
    use super::*;
    use predicates::prelude::*;
    use csv::ReaderBuilder;
    use std::collections::HashMap;

    fn get_test_json() -> Result<String, std::io::Error> {
        let test_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
        let json_path = test_dir.join("example.json");
        fs::read_to_string(json_path)
    }

    fn create_test_jsonl_gz(dir: &Path, filename: &str, json_content: &str) -> PathBuf {
        let file_path = dir.join(filename);
        let file = File::create(&file_path).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::default());
        writeln!(encoder, "{}", json_content).unwrap();
        encoder.finish().unwrap();
        
        file_path
    }

    fn verify_field_data(csv_path: &Path, field_name: &str, expected_values: &[&str]) -> bool {
        let mut reader = ReaderBuilder::new().from_path(csv_path).unwrap();
        let mut found_values = Vec::new();
        for result in reader.records() {
            let record = result.unwrap();
            if record.get(1).unwrap() == field_name && !record.get(3).unwrap().is_empty() {
                found_values.push(record.get(3).unwrap().to_string());
            }
        }
        
        let mut all_found = true;
        for expected in expected_values {
            if !found_values.iter().any(|found| found == expected) {
                println!("Missing expected value: {} for field {}", expected, field_name);
                all_found = false;
            }
        }
        
        all_found
    }

    #[test]
    fn test_extract_doi_field() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let input_dir = temp_dir.path().join("input");
        fs::create_dir_all(&input_dir)?;
        let json_content = get_test_json()?;
        create_test_jsonl_gz(&input_dir, "example.jsonl.gz", &json_content);
        let output_file = temp_dir.path().join("output.csv");
        let status = Command::new("cargo")
            .args(&[
                "run",
                "--",
                "-i", input_dir.to_str().unwrap(),
                "-o", output_file.to_str().unwrap(),
                "-f", "doi"  // Extract just the DOI field
            ])
            .status()?;
        
        assert!(status.success());
        assert!(output_file.exists());
        
        // Verify DOI extraction
        let expected_doi = "10.82433/b09z-4k37";
        assert!(verify_field_data(&output_file, "doi", &[expected_doi]));
        
        Ok(())
    }

    #[test]
    fn test_extract_creators_field() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let input_dir = temp_dir.path().join("input");
        fs::create_dir_all(&input_dir)?;
        
        let json_content = get_test_json()?;
        create_test_jsonl_gz(&input_dir, "example.jsonl.gz", &json_content);
        
        let output_file = temp_dir.path().join("output.csv");
        
        let status = Command::new("cargo")
            .args(&[
                "run",
                "--",
                "-i", input_dir.to_str().unwrap(),
                "-o", output_file.to_str().unwrap(),
                "-f", "creators.name,creators.affiliation.name"  // Extract creator names and affiliations
            ])
            .status()?;
        
        assert!(status.success());
        assert!(output_file.exists());
        
        // Verify creator name extraction
        let expected_creator_names = [
            "ExampleFamilyName, ExampleGivenName",
            "ExampleOrganization"
        ];
        
        assert!(verify_field_data(&output_file, "creators", &expected_creator_names));
        
        // Verify creator affiliation extraction
        let expected_affiliations = ["ExampleAffiliation"];
        assert!(verify_field_data(&output_file, "creators", &expected_affiliations));
        
        Ok(())
    }

    #[test]
    fn test_extract_titles_field() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let input_dir = temp_dir.path().join("input");
        fs::create_dir_all(&input_dir)?;
        
        let json_content = get_test_json()?;
        create_test_jsonl_gz(&input_dir, "example.jsonl.gz", &json_content);
        
        let output_file = temp_dir.path().join("output.csv");
        
        let status = Command::new("cargo")
            .args(&[
                "run",
                "--",
                "-i", input_dir.to_str().unwrap(),
                "-o", output_file.to_str().unwrap(),
                "-f", "titles.title"  // Extract titles
            ])
            .status()?;
        
        assert!(status.success());
        assert!(output_file.exists());
        
        // Verify title extraction
        let expected_titles = [
            "Example Title",
            "Example Subtitle",
            "Example TranslatedTitle",
            "Example AlternativeTitle"
        ];
        
        assert!(verify_field_data(&output_file, "titles", &expected_titles));
        
        Ok(())
    }

    #[test]
    fn test_extract_subjects_field() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let input_dir = temp_dir.path().join("input");
        fs::create_dir_all(&input_dir)?;
        
        let json_content = get_test_json()?;
        create_test_jsonl_gz(&input_dir, "example.jsonl.gz", &json_content);
        
        let output_file = temp_dir.path().join("output.csv");
        
        let status = Command::new("cargo")
            .args(&[
                "run",
                "--",
                "-i", input_dir.to_str().unwrap(),
                "-o", output_file.to_str().unwrap(),
                "-f", "subjects.subject"  // Extract subjects
            ])
            .status()?;
        
        assert!(status.success());
        assert!(output_file.exists());
        
        // Verify subject extraction
        let expected_subjects = [
            "Digital curation and preservation",
            "Example Subject"
        ];
        
        assert!(verify_field_data(&output_file, "subjects", &expected_subjects));
        
        Ok(())
    }

    #[test]
    fn test_extract_contributors_field() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let input_dir = temp_dir.path().join("input");
        fs::create_dir_all(&input_dir)?;
        
        let json_content = get_test_json()?;
        create_test_jsonl_gz(&input_dir, "example.jsonl.gz", &json_content);
        
        let output_file = temp_dir.path().join("output.csv");
        
        let status = Command::new("cargo")
            .args(&[
                "run",
                "--",
                "-i", input_dir.to_str().unwrap(),
                "-o", output_file.to_str().unwrap(),
                "-f", "contributors.name,contributors.contributorType"  // Extract contributor names and types
            ])
            .status()?;
        
        assert!(status.success());
        assert!(output_file.exists());
        
        // Verify contributor name extraction (checking a few key ones)
        let expected_contributors = [
            "ExampleFamilyName, ExampleGivenName",
            "ExampleOrganization",
            "DataCite",
            "International DOI Foundation"
        ];
        
        assert!(verify_field_data(&output_file, "contributors", &expected_contributors));
        
        // Verify contributor types
        let expected_types = [
            "ContactPerson",
            "DataCollector",
            "RegistrationAgency",
            "Distributor"
        ];
        
        assert!(verify_field_data(&output_file, "contributors", &expected_types));
        
        Ok(())
    }

    #[test]
    fn test_extract_dates_field() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let input_dir = temp_dir.path().join("input");
        fs::create_dir_all(&input_dir)?;
        
        let json_content = get_test_json()?;
        create_test_jsonl_gz(&input_dir, "example.jsonl.gz", &json_content);
        
        let output_file = temp_dir.path().join("output.csv");
        
        let status = Command::new("cargo")
            .args(&[
                "run",
                "--",
                "-i", input_dir.to_str().unwrap(),
                "-o", output_file.to_str().unwrap(),
                "-f", "dates.date,dates.dateType"  // Extract dates and date types
            ])
            .status()?;
        
        assert!(status.success());
        assert!(output_file.exists());
        
        // Verify date extraction
        let expected_dates = [
            "2023-01-01",
            "2022-01-01/2022-12-31"  // Range date for Collected
        ];
        
        assert!(verify_field_data(&output_file, "dates", &expected_dates));
        
        // Verify date types
        let expected_types = [
            "Accepted",
            "Available",
            "Collected",
            "Created",
            "Issued"
        ];
        
        assert!(verify_field_data(&output_file, "dates", &expected_types));
        
        Ok(())
    }

    #[test]
    fn test_extract_related_identifiers_field() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let input_dir = temp_dir.path().join("input");
        fs::create_dir_all(&input_dir)?;
        
        let json_content = get_test_json()?;
        create_test_jsonl_gz(&input_dir, "example.jsonl.gz", &json_content);
        
        let output_file = temp_dir.path().join("output.csv");
        
        let status = Command::new("cargo")
            .args(&[
                "run",
                "--",
                "-i", input_dir.to_str().unwrap(),
                "-o", output_file.to_str().unwrap(),
                "-f", "relatedIdentifiers.relatedIdentifier,relatedIdentifiers.relationType"
            ])
            .status()?;
        
        assert!(status.success());
        assert!(output_file.exists());
        
        // Verify related identifier extraction
        let expected_identifiers = [
            "ark:/13030/tqb3kh97gh8w",
            "arXiv:0706.0001",
            "10.1016/j.epsl.2011.11.037"
        ];
        
        assert!(verify_field_data(&output_file, "relatedIdentifiers", &expected_identifiers));
        
        // Verify relation types
        let expected_types = [
            "IsCitedBy",
            "Cites",
            "IsSupplementTo"
        ];
        
        assert!(verify_field_data(&output_file, "relatedIdentifiers", &expected_types));
        
        Ok(())
    }

    #[test]
    fn test_extract_geo_locations_field() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let input_dir = temp_dir.path().join("input");
        fs::create_dir_all(&input_dir)?;
        
        let json_content = get_test_json()?;
        create_test_jsonl_gz(&input_dir, "example.jsonl.gz", &json_content);
        
        let output_file = temp_dir.path().join("output.csv");
        
        let status = Command::new("cargo")
            .args(&[
                "run",
                "--",
                "-i", input_dir.to_str().unwrap(),
                "-o", output_file.to_str().unwrap(),
                "-f", "geoLocations.geoLocationPlace,geoLocations.geoLocationPoint.pointLatitude"
            ])
            .status()?;
        
        assert!(status.success());
        assert!(output_file.exists());
        
        // Verify geoLocation place extraction
        let expected_places = [
            "Vancouver, British Columbia, Canada"
        ];
        
        assert!(verify_field_data(&output_file, "geoLocations", &expected_places));
        
        // Verify latitude extraction
        let expected_lat = ["49.2827"];  // Point latitude
        
        assert!(verify_field_data(&output_file, "geoLocations", &expected_lat));
        
        Ok(())
    }

    #[test]
    fn test_extract_all_fields() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let input_dir = temp_dir.path().join("input");
        fs::create_dir_all(&input_dir)?;
        
        let json_content = get_test_json()?;
        create_test_jsonl_gz(&input_dir, "example.jsonl.gz", &json_content);
        
        let output_file = temp_dir.path().join("output.csv");
        
        // Extract multiple fields at once
        let status = Command::new("cargo")
            .args(&[
                "run",
                "--",
                "-i", input_dir.to_str().unwrap(),
                "-o", output_file.to_str().unwrap(),
                "-f", "doi,creators.name,titles.title,contributors.name,dates.date,subjects.subject,publisher.name,version,rightsList.rights,descriptions.description,geoLocations.geoLocationPlace,fundingReferences.funderName"
            ])
            .status()?;
        
        assert!(status.success());
        assert!(output_file.exists());
        
        // Verify a sampling of fields from different parts of the schema
        assert!(verify_field_data(&output_file, "doi", &["10.82433/b09z-4k37"]));
        assert!(verify_field_data(&output_file, "creators", &["ExampleFamilyName, ExampleGivenName"]));
        assert!(verify_field_data(&output_file, "titles", &["Example Title"]));
        assert!(verify_field_data(&output_file, "publisher", &["Example Publisher"]));
        assert!(verify_field_data(&output_file, "version", &["1"]));
        assert!(verify_field_data(&output_file, "rightsList", &["Creative Commons Attribution 4.0 International"]));
        assert!(verify_field_data(&output_file, "fundingReferences", &["Example Funder"]));
        
        Ok(())
    }

    #[test]
    fn test_provider_client_filter() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let input_dir = temp_dir.path().join("input");
        fs::create_dir_all(&input_dir)?;
        
        let json_content = get_test_json()?;
        create_test_jsonl_gz(&input_dir, "example.jsonl.gz", &json_content);
        
        let output_file = temp_dir.path().join("output.csv");
        
        // Extract with provider filter
        let status = Command::new("cargo")
            .args(&[
                "run",
                "--",
                "-i", input_dir.to_str().unwrap(),
                "-o", output_file.to_str().unwrap(),
                "-f", "doi,creators.name",
                "--provider", "datacite"  // Should match our example
            ])
            .status()?;
        
        assert!(status.success());
        assert!(output_file.exists());
        assert!(verify_field_data(&output_file, "doi", &["10.82433/b09z-4k37"]));
        
        // Now try with a provider that doesn't match
        let output_file2 = temp_dir.path().join("output2.csv");
        let status = Command::new("cargo")
            .args(&[
                "run",
                "--",
                "-i", input_dir.to_str().unwrap(),
                "-o", output_file2.to_str().unwrap(),
                "-f", "doi,creators.name",
                "--provider", "wrong-provider"  // Should not match our example
            ])
            .status()?;
        
        assert!(status.success());
        assert!(output_file2.exists());
        
        // Verify file exists but has no data (just headers)
        let file_content = fs::read_to_string(&output_file2)?;
        let lines: Vec<&str> = file_content.lines().collect();
        assert_eq!(lines.len(), 1); // Only header row, no data
        
        Ok(())
    }

    #[test]
    fn test_organized_output() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let input_dir = temp_dir.path().join("input");
        fs::create_dir_all(&input_dir)?;
        
        let json_content = get_test_json()?;
        create_test_jsonl_gz(&input_dir, "example.jsonl.gz", &json_content);
        
        let output_dir = temp_dir.path().join("organized_output");
        
        // Extract with organized output
        let status = Command::new("cargo")
            .args(&[
                "run",
                "--",
                "-i", input_dir.to_str().unwrap(),
                "-o", output_dir.to_str().unwrap(),
                "-f", "doi,creators.name",
                "-g"  // Organize output by provider/client
            ])
            .status()?;
        
        assert!(status.success());
        
        // Check for organized directory structure
        let provider_dir = output_dir.join("datacite");
        let client_file = provider_dir.join("datacite.mwg.csv");
        
        assert!(provider_dir.exists(), "Provider directory wasn't created");
        assert!(client_file.exists(), "Client file wasn't created");
        
        // Verify content in organized file
        assert!(verify_field_data(&client_file, "doi", &["10.82433/b09z-4k37"]));
        
        Ok(())
    }
}