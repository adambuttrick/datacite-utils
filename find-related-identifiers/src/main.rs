use clap::Parser;
use csv::{ReaderBuilder, WriterBuilder};
use flate2::read::GzDecoder;
use indicatif::style::TemplateError;
use indicatif::{ProgressBar, ProgressStyle};
use log::{error, info, warn, LevelFilter};
use rayon::prelude::*;
use serde::Deserialize;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use thiserror::Error;
use walkdir::WalkDir;

mod memory_usage {
    #[derive(Debug)]
    pub struct MemoryStats {
        pub rss_mb: f64,
        pub vm_size_mb: f64,
        pub percent: f64,
    }

    #[cfg(target_os = "linux")]
    pub fn get_memory_usage() -> Option<MemoryStats> {
        use std::fs::read_to_string;

        let pid = std::process::id();
        let status_file = format!("/proc/{}/status", pid);
        if let Ok(content) = read_to_string(status_file) {
            let mut vm_rss = None;
            let mut vm_size = None;
            for line in content.lines() {
                if line.starts_with("VmRSS:") {
                    vm_rss = line
                        .split_whitespace()
                        .nth(1)
                        .and_then(|s| s.parse::<f64>().ok());
                } else if line.starts_with("VmSize:") {
                    vm_size = line
                        .split_whitespace()
                        .nth(1)
                        .and_then(|s| s.parse::<f64>().ok());
                }
            }
            if let Ok(meminfo) = read_to_string("/proc/meminfo") {
                for line in meminfo.lines() {
                    if line.starts_with("MemTotal:") {
                        if let Some(mem_total_kb) = line
                            .split_whitespace()
                            .nth(1)
                            .and_then(|s| s.parse::<f64>().ok())
                        {
                            if let (Some(rss), Some(size)) = (vm_rss, vm_size) {
                                let percent = if mem_total_kb > 0.0 {
                                    (rss / mem_total_kb) * 100.0
                                } else {
                                    0.0
                                };
                                return Some(MemoryStats {
                                    rss_mb: rss / 1024.0,
                                    vm_size_mb: size / 1024.0,
                                    percent,
                                });
                            }
                        }
                    }
                }
            }
        }
        None
    }

    #[cfg(target_os = "macos")]
    pub fn get_memory_usage() -> Option<MemoryStats> {
        use std::process::Command;

        let pid = std::process::id();
        let ps_output = Command::new("ps")
            .args(&["-o", "rss=", "-p", &pid.to_string()])
            .output()
            .ok()?;
        let rss_kb = String::from_utf8_lossy(&ps_output.stdout)
            .trim()
            .parse::<f64>()
            .ok()?;
        let vsz_output = Command::new("ps")
            .args(&["-o", "vsz=", "-p", &pid.to_string()])
            .output()
            .ok()?;
        let vsz_kb = String::from_utf8_lossy(&vsz_output.stdout)
            .trim()
            .parse::<f64>()
            .ok()?;
        let hw_mem_output = Command::new("sysctl")
            .args(&["-n", "hw.memsize"])
            .output()
            .ok()?;
        let total_bytes = String::from_utf8_lossy(&hw_mem_output.stdout)
            .trim()
            .parse::<f64>()
            .ok()?;
        let total_kb = total_bytes / 1024.0;
        let percent = if total_kb > 0.0 {
            (rss_kb / total_kb) * 100.0
        } else {
            0.0
        };

        Some(MemoryStats {
            rss_mb: rss_kb / 1024.0,
            vm_size_mb: vsz_kb / 1024.0,
            percent,
        })
    }

    #[cfg(target_os = "windows")]
    pub fn get_memory_usage() -> Option<MemoryStats> {
        use std::process::Command;

        let pid = std::process::id();
        let wmic_output = Command::new("wmic")
            .args(&[
                "process",
                "where",
                &format!("ProcessID={}", pid),
                "get",
                "WorkingSetSize,VirtualSize",
                "/format:csv",
            ])
            .output()
            .ok()?;
        let output_str = String::from_utf8_lossy(&wmic_output.stdout);
        let lines: Vec<&str> = output_str.lines().collect();
        if lines.len() < 2 {
            return None;
        }
        let data_parts: Vec<&str> = lines[1].split(',').collect();
        if data_parts.len() < 3 {
            return None;
        }
        let virtual_bytes = data_parts[1].trim().parse::<f64>().ok()?;
        let working_set_bytes = data_parts[2].trim().parse::<f64>().ok()?;

        let mem_output = Command::new("wmic")
            .args(&[
                "computersystem",
                "get",
                "TotalPhysicalMemory",
                "/format:value",
            ])
            .output()
            .ok()?;
        let mem_str = String::from_utf8_lossy(&mem_output.stdout);
        let total_bytes_str = mem_str.trim().strip_prefix("TotalPhysicalMemory=")?.trim();
        let total_bytes = total_bytes_str.parse::<f64>().ok()?;

        let percent = if total_bytes > 0.0 {
             (working_set_bytes / total_bytes) * 100.0
        } else {
            0.0
        };

        Some(MemoryStats {
            rss_mb: working_set_bytes / (1024.0 * 1024.0),
            vm_size_mb: virtual_bytes / (1024.0 * 1024.0),
            percent,
        })
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    pub fn get_memory_usage() -> Option<MemoryStats> {
        None
    }

    pub fn log_memory_usage(note: &str) {
        use log::info;

        if let Some(stats) = get_memory_usage() {
            info!(
                "Memory usage ({}): {:.1} MB physical (RSS), {:.1} MB virtual, {:.1}% of system memory",
                note, stats.rss_mb, stats.vm_size_mb, stats.percent
            );
        } else {
            #[cfg(target_os = "linux")]
            info!("Failed to get memory usage on Linux");
            #[cfg(target_os = "macos")]
            info!("Failed to get memory usage on macOS");
            #[cfg(target_os = "windows")]
            info!("Failed to get memory usage on Windows");
            #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
            info!("Memory usage tracking not available on this platform");
        }
    }
}

#[derive(Error, Debug)]
enum AppError {
    #[error("CSV Error: {0}")]
    Csv(#[from] csv::Error),
    #[error("IO Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON Error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Directory Traversal Error: {0}")]
    WalkDir(#[from] walkdir::Error),
    #[error("Mutex was poisoned (likely due to panic in another thread)")]
    MutexPoisoned,
    #[error("Channel send error: {0}")]
    SendError(#[from] mpsc::SendError<Option<Vec<MatchResult>>>),
    #[error("No input DOIs found in the mapping CSV")]
    NoInputDois,
    #[error("Output file path is invalid")]
    InvalidOutputPath,
    #[error("Input directory not found or is not a directory: {0}")]
    InputDirectoryNotFound(String),
    #[error("Input mapping file not found: {0}")]
    MappingFileNotFound(String),
    #[error("Processing failed for file {0}: {1}")]
    FileProcessingFailed(PathBuf, String),
    #[error("Progress bar template error: {0}")]
    Template(#[from] TemplateError),
}

impl<T> From<std::sync::PoisonError<T>> for AppError {
    fn from(_err: std::sync::PoisonError<T>) -> Self {
        AppError::MutexPoisoned
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short = 'm', long = "mapping-csv", required = true)]
    mapping_csv: PathBuf,
    #[arg(short = 'i', long = "input-dir", required = true)]
    input_dir: PathBuf,
    #[arg(short = 'o', long = "output-csv", required = true)]
    output_csv: PathBuf,
    #[arg(short = 't', long = "threads", default_value_t = 0)]
    threads: usize,
    #[arg(short = 'b', long = "batch-size", default_value_t = 10000)]
    batch_size: usize,
    #[arg(long = "log-level", default_value = "INFO", value_parser = clap::value_parser!(LevelFilter))]
    log_level: LevelFilter,
    #[arg(
        short = 'r',
        long = "relation-types",
        value_parser = clap::value_parser!(String),
        required = false,
        value_delimiter = ','
    )]
    relation_types: Option<Vec<String>>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct InputCsvRecord {
    anr_code: Option<String>,
    doi: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct DataCiteRecord {
    attributes: Attributes,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Attributes {
    doi: Option<String>,
    state: Option<String>,
    #[serde(default)]
    related_identifiers: Vec<RelatedIdentifier>,
    #[serde(default)]
    types: Option<Types>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct RelatedIdentifier {
    related_identifier: Option<String>,
    relation_type: Option<String>,
    related_identifier_type: Option<String>,
    resource_type_general: Option<String>,
    resource_type: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct Types {
    resource_type: Option<String>,
    resource_type_general: Option<String>,
}

#[derive(Debug, Clone)]
struct MatchResult {
    input_doi: String,
    matched_relation_type: Option<String>,
    datacite_record_doi: Option<String>,
    datacite_record_resource_type: Option<String>,
    datacite_record_resource_type_general: Option<String>,
}

fn normalize_doi(doi_str: &str) -> String {
    let trimmed = doi_str.trim();
    let stripped = trimmed
        .strip_prefix("https://doi.org/")
        .or_else(|| trimmed.strip_prefix("http://doi.org/"))
        .or_else(|| trimmed.strip_prefix("doi:"))
        .unwrap_or(trimmed);

    stripped.to_lowercase()
}

fn format_elapsed(elapsed: Duration) -> String {
    let seconds = elapsed.as_secs() % 60;
    let minutes = (elapsed.as_secs() / 60) % 60;
    let hours = elapsed.as_secs() / 3600;
    if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, seconds)
    } else {
        format!("{}.{:03}s", seconds, elapsed.subsec_millis())
    }
}

fn load_input_dois(csv_path: &Path) -> Result<HashSet<String>, AppError> {
    if !csv_path.exists() {
        return Err(AppError::MappingFileNotFound(csv_path.display().to_string()));
    }
    info!("Loading input DOIs from {}", csv_path.display());
    let file = File::open(csv_path)?;
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);
    let mut dois = HashSet::new();

    let headers = rdr.headers()?.clone();
    let doi_index = match headers
        .iter()
        .position(|h| h.trim().eq_ignore_ascii_case("doi"))
    {
        Some(index) => index,
        None => {
            let err_msg =
                "Could not find 'doi' column (case-insensitive) in mapping CSV header"
                    .to_string();
            return Err(AppError::Csv(csv::Error::from(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                err_msg,
            ))));
        }
    };

    let mut line_count = 0;
    for result in rdr.records() {
        line_count += 1;
        let record = match result {
            Ok(r) => r,
            Err(e) => {
                warn!("Error reading record at line {} in mapping CSV: {}", line_count + 1, e);
                continue;
            }
        };
        if let Some(doi_value) = record.get(doi_index) {
            if !doi_value.trim().is_empty() {
                let normalized = normalize_doi(doi_value);
                if !normalized.is_empty() {
                    dois.insert(normalized);
                } else {
                    warn!(
                        "DOI value '{}' (line {}) became empty after normalization.",
                        doi_value,
                        line_count + 1
                    );
                }
            }
        } else {
            warn!(
                "Record found with missing DOI field at line {}: {:?}",
                line_count + 1, record
            );
        }
    }

    if dois.is_empty() {
        Err(AppError::NoInputDois)
    } else {
        info!(
            "Loaded {} unique normalized DOIs from {} lines in {}",
            dois.len(),
            line_count,
            csv_path.display()
        );
        Ok(dois)
    }
}

fn process_datacite_file_content(
    file_path: &Path,
    input_dois: Arc<HashSet<String>>,
    relation_type_filter: Arc<Option<HashSet<String>>>,
) -> Result<Vec<MatchResult>, String>
{
    let mut file_matches = Vec::new();

    let file = File::open(file_path).map_err(|e| e.to_string())?;
    let gz = GzDecoder::new(file);
    let reader = BufReader::new(gz);

    for (line_num, line_result) in reader.lines().enumerate() {
        let line = match line_result {
            Ok(l) => l,
            Err(e) => {
                warn!(
                    "Error reading line {} in {}: {}. Skipping line.",
                    line_num + 1,
                    file_path.display(),
                    e
                );
                continue;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        let record: DataCiteRecord = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                warn!(
                    "JSON parse error on line {} in {}: {} (Line: {}...). Skipping line.",
                    line_num + 1,
                    file_path.display(),
                    e,
                    line.chars().take(100).collect::<String>()
                );
                continue;
            }
        };

        if record.attributes.state.as_deref() != Some("findable") {
            continue;
        }

        let datacite_primary_doi = record.attributes.doi.as_ref();
        let normalized_primary_doi_opt = datacite_primary_doi.map(|s| normalize_doi(s));
        let record_types = record.attributes.types.clone();

        for related_id in &record.attributes.related_identifiers {
            if let Some(related_identifier_str) = &related_id.related_identifier {
                if related_identifier_str.trim().is_empty() {
                    continue;
                }

                let normalized_related_identifier = normalize_doi(related_identifier_str);

                if !normalized_related_identifier.is_empty()
                    && input_dois.contains(&normalized_related_identifier)
                {
                     if Some(normalized_related_identifier.as_str())
                        == normalized_primary_doi_opt.as_deref()
                     {
                         continue;
                     }

                     let relation_type_matches_filter = match relation_type_filter.as_ref() {
                         Some(filter_set) => related_id.relation_type.as_ref().map_or(false, |rt| filter_set.contains(rt)),
                         None => true,
                     };

                    if relation_type_matches_filter {
                        let result = MatchResult {
                            input_doi: normalized_related_identifier.clone(),
                            matched_relation_type: related_id.relation_type.clone(),
                            datacite_record_doi: record.attributes.doi.clone(),
                            datacite_record_resource_type: record_types
                                .as_ref()
                                .and_then(|t| t.resource_type.clone()),
                            datacite_record_resource_type_general: record_types
                                .as_ref()
                                .and_then(|t| t.resource_type_general.clone()),
                        };
                        file_matches.push(result);
                    }
                }
            }
        }
    }
    Ok(file_matches)
}

fn main() -> Result<(), AppError> {
    let main_start_time = Instant::now();
    let args = Args::parse();

    env_logger::Builder::new()
        .filter_level(args.log_level)
        .format_timestamp_secs()
        .init();

    info!("Starting DataCite DOI Matcher V2.1 (Relation Type Filter)");
    memory_usage::log_memory_usage("startup");

    info!("Configuration:");
    info!("  Mapping CSV: {}", args.mapping_csv.display());
    info!("  Input Directory: {}", args.input_dir.display());
    info!("  Output CSV: {}", args.output_csv.display());
    info!("  Threads: {}", if args.threads == 0 { "Auto".to_string() } else { args.threads.to_string() });
    info!("  Batch Size: {}", args.batch_size);
    info!("  Log Level: {}", args.log_level);

    let relation_type_filter = Arc::new(args.relation_types.map(|types| {
        let filter_set: HashSet<String> = types.into_iter().collect();
        info!("  Filtering for Relation Types: {:?}", filter_set);
        filter_set
    }));
    if relation_type_filter.is_none() {
        info!("  Relation Type Filter: Not active (all types included)");
    }


    if !args.input_dir.is_dir() {
        return Err(AppError::InputDirectoryNotFound(
            args.input_dir.display().to_string(),
        ));
    }
    if let Some(parent) = args.output_csv.parent() {
        if !parent.exists() {
            info!("Creating output directory: {}", parent.display());
            std::fs::create_dir_all(parent)?;
        }
    }

    let load_start = Instant::now();
    let input_dois = Arc::new(load_input_dois(&args.mapping_csv)?);
    info!("Loaded input DOIs in {}", format_elapsed(load_start.elapsed()));

    if args.threads > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(args.threads)
            .build_global()
            .expect("Failed to build Rayon thread pool");
        info!("Using {} worker threads.", args.threads);
    } else {
        info!("Using default number of Rayon threads.");
    }

    let find_start = Instant::now();
    let files_to_process: Vec<PathBuf> = WalkDir::new(&args.input_dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "gz"))
        .filter(|e| {
            e.path()
                .file_stem()
                .map_or(false, |stem| stem.to_string_lossy().ends_with(".jsonl"))
        })
        .map(|e| e.into_path())
        .collect();

    if files_to_process.is_empty() {
        warn!("No '.jsonl.gz' files found in {}. Exiting.", args.input_dir.display());
        return Ok(());
    }
    let total_files = files_to_process.len();
    info!(
        "Found {} '.jsonl.gz' files to process in {}",
        total_files,
        format_elapsed(find_start.elapsed())
    );

    let progress_bar = ProgressBar::new(total_files as u64);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template(
                "[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}",
            )?
            .progress_chars("#>-"),
    );
    progress_bar.set_message("Starting processing...");

    let (tx, rx) = mpsc::channel::<Option<Vec<MatchResult>>>();
    let batch_collector = Arc::new(Mutex::new(Vec::with_capacity(args.batch_size)));

    let output_csv_path = args.output_csv.clone();
    let csv_writer_thread = std::thread::spawn(move || -> Result<(), AppError> {
        info!("CSV writer thread started.");
        let output_file = BufWriter::new(File::create(&output_csv_path)?);
        let mut writer = WriterBuilder::new()
            .quote_style(csv::QuoteStyle::Necessary)
            .from_writer(output_file);

        writer.write_record(&[
            "input_doi",
            "datacite_record_doi",
            "matched_relation_type",
            "datacite_record_resource_type",
            "datacite_record_resource_type_general",
        ])?;
        writer.flush()?;

        while let Ok(batch_option) = rx.recv() {
            match batch_option {
                Some(batch) => {
                    if batch.is_empty() { continue; }
                    for result in batch {
                        writer.write_record(&[
                            &result.input_doi,
                            result.datacite_record_doi.as_deref().unwrap_or(""),
                            result.matched_relation_type.as_deref().unwrap_or(""),
                            result
                                .datacite_record_resource_type
                                .as_deref()
                                .unwrap_or(""),
                            result
                                .datacite_record_resource_type_general
                                .as_deref()
                                .unwrap_or(""),
                        ])?;
                    }
                    writer.flush()?;
                }
                None => {
                    info!("CSV writer received None (termination signal). Flushing and exiting.");
                    break;
                }
            }
        }
        writer.flush()?;
        info!("CSV writer thread finished.");
        Ok(())
    });

    let process_start_time = Instant::now();
    info!("Starting parallel file processing...");

    let processing_results: Vec<Result<usize, AppError>> = files_to_process
        .par_iter()
        .map(|file_path| {
            let file_name_for_msg = file_path.file_name().map_or_else(|| file_path.to_string_lossy(), |n| n.to_string_lossy());
            progress_bar.set_message(format!("Processing: {}", file_name_for_msg));

            let input_dois_clone = Arc::clone(&input_dois);
            let relation_filter_clone = Arc::clone(&relation_type_filter);

            match process_datacite_file_content(file_path, input_dois_clone, relation_filter_clone) {
                Ok(file_matches) => {
                    let matches_count = file_matches.len();

                    if !file_matches.is_empty() {
                        let mut batch_guard = batch_collector.lock()?;
                        batch_guard.extend(file_matches);

                        if batch_guard.len() >= args.batch_size {
                            let batch_to_send =
                                std::mem::replace(&mut *batch_guard, Vec::with_capacity(args.batch_size));
                            drop(batch_guard);

                            tx.send(Some(batch_to_send)).map_err(|e| {
                                error!("Fatal: Failed to send batch to CSV writer: {}", e);
                                AppError::SendError(e)
                            })?;
                        }
                    }
                    progress_bar.inc(1);
                    Ok(matches_count)
                }
                Err(e) => {
                    error!(
                        "Error processing file {}: {}",
                        file_path.display(),
                        e
                    );
                    progress_bar.inc(1);
                    Err(AppError::FileProcessingFailed(file_path.clone(), e))
                }
            }
        })
        .collect();

    info!("Parallel processing loop finished in {}.", format_elapsed(process_start_time.elapsed()));

    info!("Sending final batch if any...");
    let final_batch = {
        let mut batch_guard = batch_collector.lock()?;
        std::mem::replace(&mut *batch_guard, Vec::new())
    };

    if !final_batch.is_empty() {
        info!("Sending final batch of size {}.", final_batch.len());
         tx.send(Some(final_batch)).map_err(|e| {
            error!("Fatal: Failed to send final batch to CSV writer: {}", e);
            AppError::SendError(e)
        })?;
    }

    info!("Signaling CSV writer thread to terminate...");
     tx.send(None).map_err(|e| {
        error!("Fatal: Failed to send termination signal to CSV writer: {}", e);
        AppError::SendError(e)
    })?;

    info!("Waiting for CSV writer thread to join...");
    match csv_writer_thread.join() {
        Ok(Ok(())) => info!("CSV writer thread joined successfully."),
        Ok(Err(e)) => {
            error!("CSV writer thread returned an error: {}", e);
             return Err(e);
        }
        Err(e) => {
            error!("Failed to join CSV writer thread (panic): {:?}", e);
             return Err(AppError::MutexPoisoned);
        }
    }

    progress_bar.finish_with_message(format!(
        "Processing finished in {}",
        format_elapsed(main_start_time.elapsed())
    ));

    let mut total_matches_found = 0;
    let mut file_errors = 0;
    let mut files_processed_ok = 0;
    for result in processing_results {
        match result {
            Ok(count) => {
                total_matches_found += count as u64;
                files_processed_ok += 1;
            }
            Err(_) => {
                 file_errors += 1;
            }
        }
    }

    info!("--- Completion Summary ---");
    info!("  Processed {} / {} input files successfully.", files_processed_ok, total_files);
    info!("  Total matches found: {}", total_matches_found);
    info!("  Total execution time: {}", format_elapsed(main_start_time.elapsed()));

    if file_errors > 0 {
        error!(
            "Completed with {} file processing errors. Check logs above.",
            file_errors
        );
    } else {
        info!("Completed successfully with no file processing errors.");
    }

    memory_usage::log_memory_usage("completion");

    info!(
        "Total execution time: {}",
        format_elapsed(main_start_time.elapsed())
    );

    Ok(())
}