use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use anyhow::{Context, Result};
use clap::{App, Arg};
use csv::Writer;
use flate2::read::GzDecoder;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use log::{error, info, warn, LevelFilter};
use rayon::prelude::*;
use serde_json::Value;
use simple_logger::SimpleLogger;

#[cfg(target_os = "linux")]
use std::fs::read_to_string;
#[cfg(target_os = "macos")]
use std::process::Command;
#[cfg(target_os = "windows")]
use std::process::Command as WinCommand;

#[derive(Debug, Clone)]
struct AffiliationData {
    doi: String,
    name: String,
    category: String,
    role: String,
    affiliation_name: String,
    affiliation_id: String,
    affiliation_scheme: String,
    provider_id: String,
    client_id: String,
}

struct IncrementalStats {
    unique_records: HashMap<String, bool>,
    unique_persons: HashMap<String, bool>,
    unique_affiliations: HashMap<String, bool>,
    total_affiliation_records: usize,
    processed_files: usize,
    providers: HashMap<String, usize>,
    clients: HashMap<String, usize>,
}

impl IncrementalStats {
    fn new() -> Self {
        Self {
            unique_records: HashMap::new(),
            unique_persons: HashMap::new(),
            unique_affiliations: HashMap::new(),
            total_affiliation_records: 0,
            processed_files: 0,
            providers: HashMap::new(),
            clients: HashMap::new(),
        }
    }

    fn update(&mut self, affiliations: &[AffiliationData]) {
        self.total_affiliation_records += affiliations.len();
        self.processed_files += 1;
        for affiliation in affiliations {
            self.unique_records.insert(affiliation.doi.clone(), true);
            self.unique_persons.insert(affiliation.name.clone(), true);
            self.unique_affiliations.insert(affiliation.affiliation_name.clone(), true);
            
            // Track provider and client counts
            *self.providers.entry(affiliation.provider_id.clone()).or_insert(0) += 1;
            *self.clients.entry(affiliation.client_id.clone()).or_insert(0) += 1;
        }
    }

    fn log_current_stats(&self) {
        info!("Current Statistics:");
        info!("  Files processed: {}", self.processed_files);
        info!("  Total affiliation records: {}", self.total_affiliation_records);
        info!("  Unique DOIs/records: {}", self.unique_records.len());
        info!("  Unique persons: {}", self.unique_persons.len());
        info!("  Unique affiliations: {}", self.unique_affiliations.len());
        info!("  Unique providers: {}", self.providers.len());
        info!("  Unique clients: {}", self.clients.len());
    }
}

fn find_jsonl_gz_files<P: AsRef<Path>>(directory: P) -> Result<Vec<PathBuf>> {
    let pattern = directory.as_ref().join("**/*.jsonl.gz");
    let pattern_str = pattern.to_string_lossy();
    info!("Searching for files matching pattern: {}", pattern_str);
    let paths: Vec<PathBuf> = glob(&pattern_str)?.filter_map(Result::ok).collect();
    Ok(paths)
}

fn process_jsonl_file<P: AsRef<Path>>(
    filepath: P, 
    filter_provider: Option<&str>,
    filter_client: Option<&str>
) -> Result<Vec<AffiliationData>> {
    let filepath = filepath.as_ref();
    let file = File::open(filepath).with_context(|| format!("Failed to open file: {}", filepath.display()))?;
    let decoder = GzDecoder::new(file);
    let reader = BufReader::new(decoder);
    let mut affiliation_data = Vec::new();
    for (line_num, line) in reader.lines().enumerate() {
        match line {
            Ok(line_str) => {
                if line_str.trim().is_empty() { continue; }
                match serde_json::from_str::<Value>(&line_str) {
                    Ok(record) => {
                        // Extract provider and client IDs
                        let provider_id = match extract_provider_id(&record) {
                            Some(id) => id,
                            None => {
                                warn!("No provider ID found in record at {}:{}", filepath.display(), line_num + 1);
                                continue;
                            }
                        };
                        
                        let client_id = match extract_client_id(&record) {
                            Some(id) => id,
                            None => {
                                warn!("No client ID found in record at {}:{}", filepath.display(), line_num + 1);
                                continue;
                            }
                        };
                        
                        // Apply provider/client filters if specified
                        if let Some(filter_prov) = filter_provider {
                            if filter_prov != provider_id {
                                continue;
                            }
                        }
                        
                        if let Some(filter_cli) = filter_client {
                            if filter_cli != client_id {
                                continue;
                            }
                        }
                        
                        let doi = match extract_doi(&record) {
                            Some(id) => id,
                            None => {
                                warn!("No DOI found in record at {}:{}", filepath.display(), line_num + 1);
                                continue;
                            }
                        };
                        
                        if let Some(creators) = record.pointer("/attributes/creators") {
                            if let Some(creators_array) = creators.as_array() {
                                for creator in creators_array {
                                    extract_affiliations(creator, &doi, "creator", &provider_id, &client_id, &mut affiliation_data);
                                }
                            }
                        }
                        if let Some(contributors) = record.pointer("/attributes/contributors") {
                            if let Some(contributors_array) = contributors.as_array() {
                                for contributor in contributors_array {
                                    extract_affiliations(contributor, &doi, "contributor", &provider_id, &client_id, &mut affiliation_data);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Error parsing JSON from {}:{}: {}", filepath.display(), line_num + 1, e);
                    }
                }
            }
            Err(e) => {
                error!("Error reading from {}: {}", filepath.display(), e);
                break;
            }
        }
    }
    Ok(affiliation_data)
}

fn extract_doi(record: &Value) -> Option<String> {
    if let Some(Value::String(id)) = record.get("id") {
        return Some(id.clone());
    }
    if let Some(Value::String(doi)) = record.pointer("/attributes/doi") {
        return Some(doi.clone());
    }
    None
}

fn extract_provider_id(record: &Value) -> Option<String> {
    record
        .pointer("/relationships/provider/data/id")
        .and_then(|id| id.as_str())
        .map(|s| s.to_string())
}

fn extract_client_id(record: &Value) -> Option<String> {
    record
        .pointer("/relationships/client/data/id")
        .and_then(|id| id.as_str())
        .map(|s| s.to_string())
}

fn extract_affiliations(
    person: &Value, 
    doi: &str, 
    category: &str, 
    provider_id: &str,
    client_id: &str,
    affiliation_data: &mut Vec<AffiliationData>
) {
    let name = person.get("name").and_then(|n| n.as_str()).unwrap_or("").to_string();
    let role = if category == "contributor" {
        person.get("contributorType").and_then(|r| r.as_str()).unwrap_or("").to_string()
    } else {
        "Author".to_string()
    };
    if let Some(affiliations) = person.get("affiliation") {
        if let Some(affiliations_array) = affiliations.as_array() {
            for affiliation in affiliations_array {
                let affiliation_name = affiliation.get("name").and_then(|n| n.as_str()).unwrap_or("").to_string();
                let affiliation_id = affiliation.get("affiliationIdentifier").and_then(|id| id.as_str()).unwrap_or("").to_string();
                let affiliation_scheme = affiliation.get("affiliationIdentifierScheme").and_then(|s| s.as_str()).unwrap_or("").to_string();
                affiliation_data.push(AffiliationData {
                    doi: doi.to_string(),
                    name: name.clone(),
                    category: category.to_string(),
                    role: role.clone(),
                    affiliation_name,
                    affiliation_id,
                    affiliation_scheme,
                    provider_id: provider_id.to_string(),
                    client_id: client_id.to_string(),
                });
            }
        }
    }
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

#[cfg(target_os = "linux")]
fn get_memory_usage() -> Option<(f64, f64, f64)> {
    let pid = std::process::id();
    let status_file = format!("/proc/{}/status", pid);
    if let Ok(content) = read_to_string(status_file) {
        let mut vm_rss = None;
        let mut vm_size = None;
        for line in content.lines() {
            if line.starts_with("VmRSS:") {
                vm_rss = line.split_whitespace().nth(1).and_then(|s| s.parse::<f64>().ok());
            } else if line.starts_with("VmSize:") {
                vm_size = line.split_whitespace().nth(1).and_then(|s| s.parse::<f64>().ok());
            }
        }
        if let Ok(meminfo) = read_to_string("/proc/meminfo") {
            for line in meminfo.lines() {
                if line.starts_with("MemTotal:") {
                    if let Some(mem_total_kb) = line.split_whitespace().nth(1).and_then(|s| s.parse::<f64>().ok()) {
                        if let (Some(rss), Some(size)) = (vm_rss, vm_size) {
                            return Some((rss / 1024.0, size / 1024.0, rss / mem_total_kb * 100.0));
                        }
                    }
                }
            }
        }
    }
    None
}

#[cfg(target_os = "macos")]
fn get_memory_usage() -> Option<(f64, f64, f64)> {
    let pid = std::process::id();
    let ps_output = Command::new("ps").args(&["-o", "rss=", "-p", &pid.to_string()]).output().ok()?;
    let rss_kb = String::from_utf8_lossy(&ps_output.stdout).trim().parse::<f64>().ok()?;
    let vsz_output = Command::new("ps").args(&["-o", "vsz=", "-p", &pid.to_string()]).output().ok()?;
    let vsz_kb = String::from_utf8_lossy(&vsz_output.stdout).trim().parse::<f64>().ok()?;
    let hw_mem_output = Command::new("sysctl").args(&["-n", "hw.memsize"]).output().ok()?;
    let total_bytes = String::from_utf8_lossy(&hw_mem_output.stdout).trim().parse::<f64>().ok()?;
    let total_kb = total_bytes / 1024.0;
    let percent = (rss_kb / total_kb) * 100.0;
    Some((rss_kb / 1024.0, vsz_kb / 1024.0, percent))
}

#[cfg(target_os = "windows")]
fn get_memory_usage() -> Option<(f64, f64, f64)> {
    let pid = std::process::id();
    let wmic_output = WinCommand::new("wmic").args(&["process", "where", &format!("ProcessID={}", pid), "get", "WorkingSetSize,VirtualSize", "/format:csv"]).output().ok()?;
    let output_str = String::from_utf8_lossy(&wmic_output.stdout);
    let lines: Vec<&str> = output_str.lines().collect();
    if lines.len() < 2 { return None; }
    let data_parts: Vec<&str> = lines[1].split(',').collect();
    if data_parts.len() < 3 { return None; }
    let working_set_bytes = data_parts[1].parse::<f64>().ok()?;
    let virtual_bytes = data_parts[2].parse::<f64>().ok()?;
    let mem_output = WinCommand::new("wmic").args(&["computersystem", "get", "TotalPhysicalMemory", "/format:value"]).output().ok()?;
    let mem_str = String::from_utf8_lossy(&mem_output.stdout);
    let total_bytes_str = mem_str.trim().strip_prefix("TotalPhysicalMemory=")?.trim();
    let total_bytes = total_bytes_str.parse::<f64>().ok()?;
    let percent = (working_set_bytes / total_bytes) * 100.0;
    Some((working_set_bytes / (1024.0 * 1024.0), virtual_bytes / (1024.0 * 1024.0), percent))
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn get_memory_usage() -> Option<(f64, f64, f64)> {
    None
}

fn log_memory_usage(note: &str) {
    if let Some((rss_mb, vm_size_mb, percent)) = get_memory_usage() {
        info!("Memory usage ({}): {:.1} MB physical, {:.1} MB virtual, {:.1}% of system memory", note, rss_mb, vm_size_mb, percent);
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

struct CsvWriterManager {
    base_output_dir: PathBuf,
    organize_by_provider: bool,
    default_writer: Option<Writer<File>>,
    // Using LRU cache pattern for file handles
    current_writers: HashMap<(String, String), Writer<File>>,
    // Track which files we've created already to ensure headers are written once
    created_files: HashSet<PathBuf>,
    max_open_files: usize,
    headers: Vec<String>,
}

impl CsvWriterManager {
    fn new<P: AsRef<Path>>(output_path: P, organize_by_provider: bool, max_open_files: usize) -> Result<Self> {
        let path = output_path.as_ref();
        let headers = vec![
            "doi".to_string(),
            "name".to_string(),
            "category".to_string(),
            "role".to_string(),
            "affiliation_name".to_string(),
            "affiliation_id".to_string(),
            "affiliation_scheme".to_string(),
            "provider_id".to_string(),
            "client_id".to_string(),
        ];
        
        if organize_by_provider {
            fs::create_dir_all(path)?;
            info!("Created output directory: {}", path.display());
            info!("Using a maximum of {} open files at once", max_open_files);
            
            Ok(Self {
                base_output_dir: path.to_path_buf(),
                organize_by_provider,
                default_writer: None,
                current_writers: HashMap::new(),
                created_files: HashSet::new(),
                max_open_files,
                headers,
            })
        } else {
            let mut writer = Writer::from_path(path)?;
            writer.write_record(&headers)?;
            writer.flush()?;
            
            Ok(Self {
                base_output_dir: path.parent().unwrap_or(Path::new(".")).to_path_buf(),
                organize_by_provider,
                default_writer: Some(writer),
                current_writers: HashMap::new(),
                created_files: HashSet::new(),
                max_open_files,
                headers,
            })
        }
    }
    
    fn get_writer(&mut self, provider_id: &str, client_id: &str) -> Result<&mut Writer<File>> {
        if !self.organize_by_provider {
            return Ok(self.default_writer.as_mut().unwrap());
        }
        
        let key = (provider_id.to_string(), client_id.to_string());
        if !self.current_writers.contains_key(&key) {
            if self.current_writers.len() >= self.max_open_files {
                let keys_to_remove: Vec<(String, String)> = self.current_writers.keys()
                    .take(self.max_open_files / 2)
                    .cloned()
                    .collect();
                
                info!("Reached {} open files limit, closing {} files", self.max_open_files, keys_to_remove.len());
                
                for k in keys_to_remove {
                    if let Some(mut writer) = self.current_writers.remove(&k) {
                        let _ = writer.flush();
                    }
                }
            }
            
            let provider_dir = self.base_output_dir.join(provider_id);
            fs::create_dir_all(&provider_dir)?;
            
            let client_file = provider_dir.join(format!("{}.csv", client_id));
            
            let need_header = !self.created_files.contains(&client_file);
            
            let file_exists = client_file.exists();
            
            let mut csv_writer = if file_exists {
                let file = OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(&client_file)?;
                Writer::from_writer(file)
            } else {
                let writer = Writer::from_path(&client_file)?;
                writer
            };
            
            if !file_exists {
                csv_writer.write_record(&self.headers)?;
                csv_writer.flush()?;
                self.created_files.insert(client_file.clone());
                info!("Created new file with header: {}", client_file.display());
            } else if need_header {
                self.created_files.insert(client_file.clone());
                info!("Opened existing file: {}", client_file.display());
            }
            
            self.current_writers.insert(key.clone(), csv_writer);
        }
        
        Ok(self.current_writers.get_mut(&key).unwrap())
    }
    
    fn write_batch(&mut self, batch: &[AffiliationData]) -> Result<()> {
        let mut grouped_records: HashMap<(String, String), Vec<&AffiliationData>> = HashMap::new();
        
        for affiliation in batch {
            let key = (affiliation.provider_id.clone(), affiliation.client_id.clone());
            grouped_records.entry(key).or_insert_with(Vec::new).push(affiliation);
        }
        
        for ((provider_id, client_id), records) in grouped_records {
            let writer = self.get_writer(&provider_id, &client_id)?;
            
            for affiliation in records {
                writer.write_record(&[
                    &affiliation.doi,
                    &affiliation.name,
                    &affiliation.category,
                    &affiliation.role,
                    &affiliation.affiliation_name,
                    &affiliation.affiliation_id,
                    &affiliation.affiliation_scheme,
                    &affiliation.provider_id,
                    &affiliation.client_id,
                ])?;
            }
            writer.flush()?;
        }
        
        Ok(())
    }
    
    fn flush_all(&mut self) -> Result<()> {
        if let Some(writer) = self.default_writer.as_mut() {
            writer.flush()?;
        }
        
        for (_, writer) in self.current_writers.iter_mut() {
            writer.flush()?;
        }
        
        if self.organize_by_provider {
            info!("Flushing {} open CSV files", self.current_writers.len());
            info!("Total unique files created/opened: {}", self.created_files.len());
        }
        
        Ok(())
    }
}

fn main() -> Result<()> {
    let start_time = Instant::now();
    let matches = App::new("Affiliation Metadata Extractor")
        .version("1.2")
        .about("Extracts affiliation metadata from compressed JSONL files")
        .arg(Arg::with_name("input").short('i').long("input").value_name("INPUT").help("Directory containing JSONL.gz files").required(true))
        .arg(Arg::with_name("output").short('o').long("output").value_name("OUTPUT").help("Output CSV file or directory").default_value("affiliation_metadata.csv"))
        .arg(Arg::with_name("log-level").short('l').long("log-level").value_name("LEVEL").help("Logging level (DEBUG, INFO, WARN, ERROR)").default_value("INFO"))
        .arg(Arg::with_name("threads").short('t').long("threads").value_name("THREADS").help("Number of threads to use (0 for auto)").default_value("0"))
        .arg(Arg::with_name("batch-size").short('b').long("batch-size").value_name("SIZE").help("Number of records to process in a batch before writing to CSV").default_value("10000"))
        .arg(Arg::with_name("stats-interval").short('s').long("stats-interval").value_name("INTERVAL").help("Interval in seconds to log statistics").default_value("60"))
        .arg(Arg::with_name("organize").short('g').long("organize").help("Organize output by provider/client").takes_value(false))
        .arg(Arg::with_name("provider").long("provider").value_name("PROVIDER_ID").help("Filter by provider ID"))
        .arg(Arg::with_name("client").long("client").value_name("CLIENT_ID").help("Filter by client ID"))
        .arg(Arg::with_name("max-open-files").long("max-open-files").value_name("MAX_FILES").help("Maximum number of open files when using --organize (default: 100)").default_value("100"))
        .get_matches();
    
    let log_level = match matches.value_of("log-level").unwrap() {
        "DEBUG" => LevelFilter::Debug,
        "INFO" => LevelFilter::Info,
        "WARN" => LevelFilter::Warn,
        "ERROR" => LevelFilter::Error,
        _ => LevelFilter::Info,
    };
    
    SimpleLogger::new().with_level(log_level).init()?;
    let input_dir = matches.value_of("input").unwrap();
    let output_path = matches.value_of("output").unwrap();
    let batch_size = matches.value_of("batch-size").unwrap().parse::<usize>().unwrap_or(10000);
    let stats_interval = matches.value_of("stats-interval").unwrap().parse::<u64>().unwrap_or(60);
    let organize_by_provider = matches.is_present("organize");
    let filter_provider = matches.value_of("provider");
    let filter_client = matches.value_of("client");
    let max_open_files = matches.value_of("max-open-files").unwrap().parse::<usize>().unwrap_or(100);
    
    info!("Using batch size of {} records", batch_size);
    info!("Statistics will be logged every {} seconds", stats_interval);
    
    if let Some(provider) = filter_provider {
        info!("Filtering by provider ID: {}", provider);
    }
    
    if let Some(client) = filter_client {
        info!("Filtering by client ID: {}", client);
    }
    
    if organize_by_provider {
        info!("Output will be organized by provider/client in directory: {}", output_path);
    } else {
        info!("Output will be written to single file: {}", output_path);
    }
    
    if let Some(threads_str) = matches.value_of("threads") {
        if let Ok(threads) = threads_str.parse::<usize>() {
            if threads > 0 {
                rayon::ThreadPoolBuilder::new().num_threads(threads).build_global()?;
                info!("Using {} threads", threads);
            }
        }
    }
    
    log_memory_usage("startup");
    info!("Finding files in {}...", input_dir);
    let files = find_jsonl_gz_files(input_dir)?;
    info!("Found {} files to process", files.len());
    
    if files.is_empty() {
        warn!("No files found in {}. Exiting.", input_dir);
        return Ok(());
    }
    
    let progress_bar = ProgressBar::new(files.len() as u64);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
            .unwrap()
            .progress_chars("#>-")
    );
    progress_bar.set_message("Processing files...");
    
    let mut csv_writer_manager = CsvWriterManager::new(output_path, organize_by_provider, max_open_files)?;
    
    let (tx, rx) = std::sync::mpsc::channel::<Option<Vec<AffiliationData>>>();
    let csv_writer_mutex = Arc::new(Mutex::new(csv_writer_manager));
    let stats = Arc::new(Mutex::new(IncrementalStats::new()));
    let stats_clone = Arc::clone(&stats);
    let stats_thread_running = Arc::new(Mutex::new(true));
    let stats_thread_running_clone = Arc::clone(&stats_thread_running);
    
    let stats_thread = std::thread::spawn(move || {
        let mut last_log_time = Instant::now();
        loop {
            std::thread::sleep(Duration::from_secs(1));
            if last_log_time.elapsed().as_secs() >= stats_interval {
                log_memory_usage("periodic check");
                if let Ok(stats) = stats_clone.lock() {
                    stats.log_current_stats();
                }
                last_log_time = Instant::now();
            }
            if let Ok(running) = stats_thread_running_clone.lock() {
                if !*running {
                    break;
                }
            }
        }
    });
    
    let csv_writer_thread = std::thread::spawn(move || {
        let mut writer_manager = csv_writer_mutex.lock().unwrap();
        while let Ok(batch_option) = rx.recv() {
            match batch_option {
                Some(batch) => {
                    if let Err(e) = writer_manager.write_batch(&batch) {
                        error!("Error writing batch to CSV: {}", e);
                    }
                }
                None => {
                    break;
                }
            }
        }
        if let Err(e) = writer_manager.flush_all() {
            error!("Error flushing CSV writers: {}", e);
        }
    });
    
    let batch_collector = Arc::new(Mutex::new(Vec::with_capacity(batch_size)));
    
    files.par_iter().for_each(|filepath| {
        match process_jsonl_file(filepath, filter_provider, filter_client) {
            Ok(file_affiliations) => {
                let file_name = filepath.file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| filepath.to_string_lossy().to_string());
                
                progress_bar.set_message(format!("Processed: {}", file_name));
                
                if !file_affiliations.is_empty() {
                    let mut stats_guard = stats.lock().unwrap();
                    stats_guard.update(&file_affiliations);
                    drop(stats_guard);
                    
                    let mut batch_guard = batch_collector.lock().unwrap();
                    batch_guard.extend(file_affiliations);
                    
                    if batch_guard.len() >= batch_size {
                        let batch_to_send = std::mem::replace(&mut *batch_guard, Vec::with_capacity(batch_size));
                        drop(batch_guard);
                        
                        if let Err(e) = tx.send(Some(batch_to_send)) {
                            error!("Error sending batch to CSV writer: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Error processing {}: {}", filepath.display(), e);
            }
        }
        progress_bar.inc(1);
    });
    
    let remaining_batch = {
        let mut batch_guard = batch_collector.lock().unwrap();
        std::mem::replace(&mut *batch_guard, Vec::new())
    };
    
    if !remaining_batch.is_empty() {
        if let Err(e) = tx.send(Some(remaining_batch)) {
            error!("Error sending final batch to CSV writer: {}", e);
        }
    }
    
    if let Err(e) = tx.send(None) {
        error!("Error sending end signal to CSV writer: {}", e);
    }
    
    if let Err(e) = csv_writer_thread.join() {
        error!("Error joining CSV writer thread: {:?}", e);
    }
    
    progress_bar.finish_with_message(format!("Completed in {}", format_elapsed(start_time.elapsed())));
    
    {
        let stats_guard = stats.lock().unwrap();
        info!("Final Statistics:");
        info!("  Files processed: {}", stats_guard.processed_files);
        info!("  Total affiliation records: {}", stats_guard.total_affiliation_records);
        info!("  Unique DOIs/records: {}", stats_guard.unique_records.len());
        info!("  Unique persons: {}", stats_guard.unique_persons.len());
        info!("  Unique affiliations: {}", stats_guard.unique_affiliations.len());
        info!("  Unique providers: {}", stats_guard.providers.len());
        info!("  Unique clients: {}", stats_guard.clients.len());
        
        info!("Provider statistics:");
        for (provider, count) in stats_guard.providers.iter() {
            info!("  Provider {}: {} records", provider, count);
        }
        
        info!("Client statistics:");
        for (client, count) in stats_guard.clients.iter() {
            info!("  Client {}: {} records", client, count);
        }
    }
    
    log_memory_usage("completion");
    
    {
        let mut running = stats_thread_running.lock().unwrap();
        *running = false;
    }
    
    if let Err(e) = stats_thread.join() {
        error!("Error joining stats thread: {:?}", e);
    }
    
    let total_runtime = start_time.elapsed();
    info!("Total execution time: {}", format_elapsed(total_runtime));
    
    Ok(())
}