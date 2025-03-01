use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use anyhow::{Result};
use clap::Parser;
use csv::Writer;
use flate2::read::GzDecoder;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use log::{error, info, warn, LevelFilter};
use rayon::prelude::*;
use serde_json::Value;
use simple_logger::SimpleLogger;
use lazy_static::lazy_static;

#[cfg(target_os = "linux")]
use std::fs::read_to_string;
#[cfg(target_os = "windows")]
use std::process::Command as WinCommand;

// Command-line arguments using clap's derive API
#[derive(Parser)]
#[command(name = "DataCite Field Extractor")]
#[command(about = "Efficiently extracts any field data from DataCite metadata in compressed JSONL files")]
#[command(version = "3.0")]
struct Cli {
    #[arg(short, long, help = "Directory containing JSONL.gz files", required = true)]
    input: String,
    
    #[arg(short, long, default_value = "field_data.csv", help = "Output CSV file or directory")]
    output: String,
    
    #[arg(short, long, default_value = "INFO", help = "Logging level (DEBUG, INFO, WARN, ERROR)")]
    log_level: String,
    
    #[arg(short, long, default_value = "0", help = "Number of threads to use (0 for auto)")]
    threads: usize,
    
    #[arg(short, long, default_value = "10000", help = "Number of records to process in a batch before writing to CSV")]
    batch_size: usize,
    
    #[arg(short, long, default_value = "60", help = "Interval in seconds to log statistics")]
    stats_interval: u64,
    
    #[arg(short = 'g', long, help = "Organize output by provider/client")]
    organize: bool,
    
    #[arg(long, help = "Filter by provider ID")]
    provider: Option<String>,
    
    #[arg(long, help = "Filter by client ID")]
    client: Option<String>,
    
    #[arg(long, default_value = "100", help = "Maximum number of open files when using --organize")]
    max_open_files: usize,
    
    #[arg(short, long, default_value = "creators", help = "Comma-separated list of fields to extract (e.g., 'creators.affiliation.name,titles')")]
    fields: String,
}

// Newtype pattern for IDs
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Doi(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ProviderId(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ClientId(String);

// Decomposed FieldData with newtype IDs
#[derive(Debug, Clone)]
struct FieldData {
    doi: Doi,
    field_name: String,
    subfield_path: String,
    value: String,
    provider_id: ProviderId,
    client_id: ClientId,
}

impl Default for FieldData {
    fn default() -> Self {
        Self {
            doi: Doi(String::new()),
            field_name: String::new(),
            subfield_path: String::new(),
            value: String::new(),
            provider_id: ProviderId(String::new()),
            client_id: ClientId(String::new()),
        }
    }
}

// Struct decomposition for independent borrowing
struct RecordStats {
    unique_records: HashMap<String, bool>,
    total_field_records: usize,
}

impl Default for RecordStats {
    fn default() -> Self {
        Self {
            unique_records: HashMap::new(),
            total_field_records: 0,
        }
    }
}

struct ProviderStats {
    providers: HashMap<ProviderId, usize>,
    clients: HashMap<ClientId, usize>,
}

impl Default for ProviderStats {
    fn default() -> Self {
        Self {
            providers: HashMap::new(),
            clients: HashMap::new(),
        }
    }
}

struct FieldStats {
    unique_fields: HashMap<String, usize>,
}

impl Default for FieldStats {
    fn default() -> Self {
        Self {
            unique_fields: HashMap::new(),
        }
    }
}

struct IncrementalStats {
    record_stats: RecordStats,
    provider_stats: ProviderStats,
    field_stats: FieldStats,
    processed_files: usize,
}

impl IncrementalStats {
    fn new() -> Self {
        Self {
            record_stats: RecordStats::default(),
            provider_stats: ProviderStats::default(),
            field_stats: FieldStats::default(),
            processed_files: 0,
        }
    }

    fn update(&mut self, field_data: &[FieldData]) {
        self.record_stats.total_field_records += field_data.len();
        self.processed_files += 1;
        
        for data in field_data {
            self.record_stats.unique_records.insert(data.doi.0.clone(), true);
            *self.field_stats.unique_fields.entry(data.field_name.clone()).or_insert(0) += 1;
            
            *self.provider_stats.providers.entry(data.provider_id.clone()).or_insert(0) += 1;
            *self.provider_stats.clients.entry(data.client_id.clone()).or_insert(0) += 1;
        }
    }

    fn log_current_stats(&self) {
        info!("Current Statistics:");
        info!("  Files processed: {}", self.processed_files);
        info!("  Total field records: {}", self.record_stats.total_field_records);
        info!("  Unique DOIs/records: {}", self.record_stats.unique_records.len());
        info!("  Unique fields: {}", self.field_stats.unique_fields.len());
        
        info!("  Field breakdown:");
        for (field, count) in &self.field_stats.unique_fields {
            info!("    {}: {} records", field, count);
        }
        
        info!("  Unique providers: {}", self.provider_stats.providers.len());
        info!("  Unique clients: {}", self.provider_stats.clients.len());
    }
}

// Define field types to track arrays vs objects vs values
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
enum FieldType {
    Array,
    Object,
    Value,
}

// Pre-defined schema structure to optimize path traversal
lazy_static! {
    static ref SCHEMA_STRUCTURE: HashMap<String, FieldType> = {
        let mut schema = HashMap::new();
        
        // Define top-level structure based on DataCite schema
        schema.insert("attributes.identifiers".to_string(), FieldType::Array);
        schema.insert("attributes.alternateIdentifiers".to_string(), FieldType::Array);
        
        schema.insert("attributes.creators".to_string(), FieldType::Array);
        schema.insert("attributes.creators.affiliation".to_string(), FieldType::Array);
        schema.insert("attributes.creators.nameIdentifiers".to_string(), FieldType::Array);
        
        schema.insert("attributes.titles".to_string(), FieldType::Array);
        
        schema.insert("attributes.subjects".to_string(), FieldType::Array);
        
        schema.insert("attributes.contributors".to_string(), FieldType::Array);
        schema.insert("attributes.contributors.affiliation".to_string(), FieldType::Array);
        schema.insert("attributes.contributors.nameIdentifiers".to_string(), FieldType::Array);
        
        schema.insert("attributes.dates".to_string(), FieldType::Array);
        
        schema.insert("attributes.relatedIdentifiers".to_string(), FieldType::Array);
        
        schema.insert("attributes.relatedItems".to_string(), FieldType::Array);
        schema.insert("attributes.relatedItems.titles".to_string(), FieldType::Array);
        schema.insert("attributes.relatedItems.creators".to_string(), FieldType::Array);
        schema.insert("attributes.relatedItems.contributors".to_string(), FieldType::Array);
        
        schema.insert("attributes.sizes".to_string(), FieldType::Array);
        schema.insert("attributes.formats".to_string(), FieldType::Array);
        
        schema.insert("attributes.rightsList".to_string(), FieldType::Array);
        
        schema.insert("attributes.descriptions".to_string(), FieldType::Array);
        
        schema.insert("attributes.geoLocations".to_string(), FieldType::Array);
        schema.insert("attributes.geoLocations.geoLocationPolygon".to_string(), FieldType::Array);
        
        schema.insert("attributes.fundingReferences".to_string(), FieldType::Array);
        
        schema
    };
}

// Path pattern to efficiently traverse JSON structure
#[derive(Debug, Clone)]
struct PathPattern {
    parts: Vec<PathPatternPart>,
    field_name: String,
}

impl Default for PathPattern {
    fn default() -> Self {
        Self {
            parts: Vec::new(),
            field_name: String::new(),
        }
    }
}

#[derive(Debug, Clone)]
enum PathPatternPart {
    Field(String),
    ArrayWildcard,
}

impl PathPattern {
    // Create a new PathPattern from a field specification
    fn new(field_path: &[String]) -> Self {
        let mut parts = Vec::new();
        let mut current_schema_path = "attributes".to_string();
        
        // Always start with attributes
        parts.push(PathPatternPart::Field("attributes".to_string()));
        
        // Generate pattern parts based on schema structure
        for part in field_path {
            // Check if this part is an array at current level
            if !current_schema_path.is_empty() {
                current_schema_path.push_str(".");
            }
            current_schema_path.push_str(part);
            
            parts.push(PathPatternPart::Field(part.clone()));
            
            // If this path is known to be an array in our schema, add wildcard
            if SCHEMA_STRUCTURE.get(&current_schema_path) == Some(&FieldType::Array) {
                parts.push(PathPatternPart::ArrayWildcard);
            }
        }
        
        // Use first non-attributes part as field name, or default to first part
        let field_name = if field_path.is_empty() {
            "unknown".to_string()
        } else {
            field_path[0].clone()
        };
        
        PathPattern { parts, field_name }
    }
    
    // Apply this pattern to a JSON Value
    fn apply(&self, json: &Value) -> Vec<(String, Value)> {
        let mut results = Vec::new();
        let mut to_process = vec![(json, String::new(), 0)]; // (node, path, parts_idx)
        
        while let Some((node, current_path, parts_idx)) = to_process.pop() {
            if parts_idx >= self.parts.len() {
                // Reached the end of the pattern, capture the value
                match node {
                    Value::String(s) => {
                        results.push((current_path, Value::String(s.clone())));
                    },
                    Value::Number(n) => {
                        results.push((current_path, Value::Number(n.clone())));
                    },
                    Value::Bool(b) => {
                        results.push((current_path, Value::Bool(*b)));
                    },
                    Value::Null => {
                        results.push((current_path, Value::Null));
                    },
                    Value::Object(_) | Value::Array(_) => {
                        // For complex values, serialize to JSON
                        if let Ok(json_str) = serde_json::to_string(node) {
                            results.push((current_path, Value::String(json_str)));
                        }
                    }
                }
                continue;
            }
            
            match &self.parts[parts_idx] {
                PathPatternPart::Field(field_name) => {
                    if let Some(value) = node.get(field_name) {
                        let new_path = if current_path.is_empty() {
                            field_name.clone()
                        } else {
                            format!("{}.{}", current_path, field_name)
                        };
                        to_process.push((value, new_path, parts_idx + 1));
                    }
                },
                PathPatternPart::ArrayWildcard => {
                    if let Some(array) = node.as_array() {
                        for (idx, item) in array.iter().enumerate() {
                            let array_path = format!("{}[{}]", current_path, idx);
                            to_process.push((item, array_path, parts_idx + 1));
                        }
                    }
                }
            }
        }
        
        results
    }
}

// Parse field specifications from comma-separated string
fn parse_field_specifications(field_specs: &str) -> Vec<Vec<String>> {
    field_specs
        .split(',')
        .map(|spec| {
            spec.trim()
                .split('.')
                .map(|part| part.trim().to_string())
                .collect()
        })
        .collect()
}

// Pre-compile field path patterns at startup
fn initialize_path_patterns(field_paths: &[Vec<String>]) -> HashMap<String, PathPattern> {
    let mut patterns = HashMap::new();
    
    for field_path in field_paths {
        let field_key = field_path.join(".");
        let pattern = PathPattern::new(field_path);
        patterns.insert(field_key, pattern);
    }
    
    patterns
}

// Find .jsonl.gz files in directory (including subdirectories)
fn find_jsonl_gz_files<P: AsRef<Path>>(directory: P) -> Result<Vec<PathBuf>> {
    let pattern = directory.as_ref().join("**/*.jsonl.gz");
    let pattern_str = pattern.to_string_lossy();
    info!("Searching for files matching pattern: {}", pattern_str);
    let paths: Vec<PathBuf> = glob(&pattern_str)?.filter_map(Result::ok).collect();
    Ok(paths)
}

// Command pattern for file processing
trait FileProcessor {
    fn process(&self, filepath: &Path) -> Result<Vec<FieldData>, (PathBuf, anyhow::Error)>;
}

struct JsonlProcessor {
    patterns: Arc<HashMap<String, PathPattern>>,
    field_paths: Vec<Vec<String>>,
    filter_provider: Option<String>,
    filter_client: Option<String>
}

impl FileProcessor for JsonlProcessor {
    fn process(&self, filepath: &Path) -> Result<Vec<FieldData>, (PathBuf, anyhow::Error)> {
        let file = match File::open(filepath) {
            Ok(f) => f,
            Err(e) => return Err((filepath.to_path_buf(), anyhow::Error::new(e).context("Failed to open file"))),
        };
        
        let decoder = GzDecoder::new(file);
        let reader = BufReader::new(decoder);
        let mut all_field_data = Vec::new();
        
        for (line_num, line) in reader.lines().enumerate() {
            match line {
                Ok(line_str) => {
                    if line_str.trim().is_empty() { continue; }
                    match serde_json::from_str::<Value>(&line_str) {
                        Ok(record) => {
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
                            if self.filter_provider.iter().any(|p| *p != provider_id.0) {
                                continue;
                            }
                            
                            if self.filter_client.iter().any(|c| *c != client_id.0) {
                                continue;
                            }
                            
                            let doi = match extract_doi(&record) {
                                Some(id) => id,
                                None => {
                                    warn!("No DOI found in record at {}:{}", filepath.display(), line_num + 1);
                                    continue;
                                }
                            };
                            
                            // Process each field specification using pre-compiled patterns
                            for field_path in &self.field_paths {
                                let field_key = field_path.join(".");
                                
                                if let Some(pattern) = self.patterns.get(&field_key) {
                                    let field_results = pattern.apply(&record);
                                    
                                    for (path, value) in field_results {
                                        let value_str = match value {
                                            Value::String(s) => s,
                                            Value::Number(n) => n.to_string(),
                                            Value::Bool(b) => b.to_string(),
                                            Value::Null => "".to_string(),
                                            _ => "[complex value]".to_string(),
                                        };
                                        
                                        all_field_data.push(FieldData {
                                            doi: doi.clone(),
                                            field_name: pattern.field_name.clone(),
                                            subfield_path: path,
                                            value: value_str,
                                            provider_id: provider_id.clone(),
                                            client_id: client_id.clone(),
                                        });
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
                    let error = anyhow::Error::new(e).context(format!("Error reading from {}", filepath.display()));
                    return Err((filepath.to_path_buf(), error));
                }
            }
        }
        
        Ok(all_field_data)
    }
}

// Helper functions to extract key identifiers
fn extract_doi(record: &Value) -> Option<Doi> {
    if let Some(Value::String(id)) = record.get("id") {
        return Some(Doi(id.clone()));
    }
    if let Some(Value::String(doi)) = record.pointer("/attributes/doi") {
        return Some(Doi(doi.clone()));
    }
    None
}

fn extract_provider_id(record: &Value) -> Option<ProviderId> {
    record
        .pointer("/relationships/provider/data/id")
        .and_then(|id| id.as_str())
        .map(|s| ProviderId(s.to_string()))
}

fn extract_client_id(record: &Value) -> Option<ClientId> {
    record
        .pointer("/relationships/client/data/id")
        .and_then(|id| id.as_str())
        .map(|s| ClientId(s.to_string()))
}

// Memory usage monitoring - encapsulated in its own module
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
                                return Some(MemoryStats {
                                    rss_mb: rss / 1024.0,
                                    vm_size_mb: size / 1024.0,
                                    percent: rss / mem_total_kb * 100.0
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
        let ps_output = Command::new("ps").args(&["-o", "rss=", "-p", &pid.to_string()]).output().ok()?;
        let rss_kb = String::from_utf8_lossy(&ps_output.stdout).trim().parse::<f64>().ok()?;
        let vsz_output = Command::new("ps").args(&["-o", "vsz=", "-p", &pid.to_string()]).output().ok()?;
        let vsz_kb = String::from_utf8_lossy(&vsz_output.stdout).trim().parse::<f64>().ok()?;
        let hw_mem_output = Command::new("sysctl").args(&["-n", "hw.memsize"]).output().ok()?;
        let total_bytes = String::from_utf8_lossy(&hw_mem_output.stdout).trim().parse::<f64>().ok()?;
        let total_kb = total_bytes / 1024.0;
        let percent = (rss_kb / total_kb) * 100.0;
        
        Some(MemoryStats {
            rss_mb: rss_kb / 1024.0,
            vm_size_mb: vsz_kb / 1024.0,
            percent
        })
    }

    #[cfg(target_os = "windows")]
    pub fn get_memory_usage() -> Option<MemoryStats> {
        use std::process::Command;
        
        let pid = std::process::id();
        let wmic_output = Command::new("wmic")
            .args(&["process", "where", &format!("ProcessID={}", pid), "get", "WorkingSetSize,VirtualSize", "/format:csv"])
            .output().ok()?;
        let output_str = String::from_utf8_lossy(&wmic_output.stdout);
        let lines: Vec<&str> = output_str.lines().collect();
        if lines.len() < 2 { return None; }
        let data_parts: Vec<&str> = lines[1].split(',').collect();
        if data_parts.len() < 3 { return None; }
        let working_set_bytes = data_parts[1].parse::<f64>().ok()?;
        let virtual_bytes = data_parts[2].parse::<f64>().ok()?;
        let mem_output = Command::new("wmic")
            .args(&["computersystem", "get", "TotalPhysicalMemory", "/format:value"])
            .output().ok()?;
        let mem_str = String::from_utf8_lossy(&mem_output.stdout);
        let total_bytes_str = mem_str.trim().strip_prefix("TotalPhysicalMemory=")?.trim();
        let total_bytes = total_bytes_str.parse::<f64>().ok()?;
        let percent = (working_set_bytes / total_bytes) * 100.0;
        
        Some(MemoryStats {
            rss_mb: working_set_bytes / (1024.0 * 1024.0),
            vm_size_mb: virtual_bytes / (1024.0 * 1024.0),
            percent
        })
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    pub fn get_memory_usage() -> Option<MemoryStats> {
        None
    }

    pub fn log_memory_usage(note: &str) {
        use log::info;
        
        if let Some(stats) = get_memory_usage() {
            info!("Memory usage ({}): {:.1} MB physical, {:.1} MB virtual, {:.1}% of system memory", 
                  note, stats.rss_mb, stats.vm_size_mb, stats.percent);
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

// Strategy pattern for output handling
trait OutputStrategy: Send {
    fn write_batch(&mut self, batch: &[FieldData]) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}

#[allow(dead_code)]
struct SingleFileOutput {
    writer: Writer<File>,
    headers: Vec<String>,
}

impl SingleFileOutput {
    fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let headers = vec![
            "doi".to_string(),
            "field_name".to_string(),
            "subfield_path".to_string(),
            "value".to_string(),
            "provider_id".to_string(),
            "client_id".to_string(),
        ];
        
        let mut writer = Writer::from_path(path)?;
        writer.write_record(&headers)?;
        writer.flush()?;
        
        Ok(Self {
            writer,
            headers,
        })
    }
}

impl OutputStrategy for SingleFileOutput {
    fn write_batch(&mut self, batch: &[FieldData]) -> Result<()> {
        for field_data in batch {
            self.writer.write_record(&[
                &field_data.doi.0,
                &field_data.field_name,
                &field_data.subfield_path,
                &field_data.value,
                &field_data.provider_id.0,
                &field_data.client_id.0,
            ])?;
        }
        self.writer.flush()?;
        Ok(())
    }
    
    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

// Organized output strategy (by provider/client)
struct OrganizedOutput {
    base_output_dir: PathBuf,
    current_writers: HashMap<(ProviderId, ClientId), Writer<File>>,
    created_files: HashSet<PathBuf>,
    max_open_files: usize,
    headers: Vec<String>,
}

impl OrganizedOutput {
    fn new<P: AsRef<Path>>(output_path: P, max_open_files: usize) -> Result<Self> {
        let path = output_path.as_ref();
        fs::create_dir_all(path)?;
        info!("Created output directory: {}", path.display());
        info!("Using a maximum of {} open files at once", max_open_files);
        
        let headers = vec![
            "doi".to_string(),
            "field_name".to_string(),
            "subfield_path".to_string(),
            "value".to_string(),
            "provider_id".to_string(),
            "client_id".to_string(),
        ];
        
        Ok(Self {
            base_output_dir: path.to_path_buf(),
            current_writers: HashMap::new(),
            created_files: HashSet::new(),
            max_open_files,
            headers,
        })
    }
    
    fn get_writer(&mut self, provider_id: &ProviderId, client_id: &ClientId) -> Result<&mut Writer<File>> {
        let key = (provider_id.clone(), client_id.clone());
        if !self.current_writers.contains_key(&key) {
            if self.current_writers.len() >= self.max_open_files {
                let keys_to_remove: Vec<(ProviderId, ClientId)> = self.current_writers.keys()
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
            
            let provider_dir = self.base_output_dir.join(&provider_id.0);
            fs::create_dir_all(&provider_dir)?;
            
            let client_file = provider_dir.join(format!("{}.csv", client_id.0));
            
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
}

impl OutputStrategy for OrganizedOutput {
    fn write_batch(&mut self, batch: &[FieldData]) -> Result<()> {
        let mut grouped_records: HashMap<(ProviderId, ClientId), Vec<&FieldData>> = HashMap::new();
        
        for field_data in batch {
            let key = (field_data.provider_id.clone(), field_data.client_id.clone());
            grouped_records.entry(key).or_insert_with(Vec::new).push(field_data);
        }
        
        for ((provider_id, client_id), records) in grouped_records {
            let writer = self.get_writer(&provider_id, &client_id)?;
            
            for field_data in records {
                writer.write_record(&[
                    &field_data.doi.0,
                    &field_data.field_name,
                    &field_data.subfield_path,
                    &field_data.value,
                    &field_data.provider_id.0,
                    &field_data.client_id.0,
                ])?;
            }
            writer.flush()?;
        }
        
        Ok(())
    }
    
    fn flush(&mut self) -> Result<()> {
        for (_, writer) in self.current_writers.iter_mut() {
            writer.flush()?;
        }
        
        info!("Flushing {} open CSV files", self.current_writers.len());
        info!("Total unique files created/opened: {}", self.created_files.len());
        
        Ok(())
    }
}

// RAII-based CsvWriterManager using the strategy pattern
struct CsvWriterManager {
    output_strategy: Box<dyn OutputStrategy>,
}

impl CsvWriterManager {
    fn new<P: AsRef<Path>>(output_path: P, organize_by_provider: bool, max_open_files: usize) -> Result<Self> {
        let strategy: Box<dyn OutputStrategy> = if organize_by_provider {
            Box::new(OrganizedOutput::new(output_path, max_open_files)?)
        } else {
            Box::new(SingleFileOutput::new(output_path)?)
        };
        
        Ok(Self {
            output_strategy: strategy,
        })
    }
    
    fn write_batch(&mut self, batch: &[FieldData]) -> Result<()> {
        self.output_strategy.write_batch(batch)
    }
    
    fn flush_all(&mut self) -> Result<()> {
        self.output_strategy.flush()
    }
}

// Implement Drop for RAII pattern
impl Drop for CsvWriterManager {
    fn drop(&mut self) {
        if let Err(e) = self.flush_all() {
            error!("Error flushing CSV writers during cleanup: {}", e);
        }
    }
}

fn main() -> Result<()> {
    let start_time = Instant::now();
    
    // Parse command line arguments
    let cli = Cli::parse();
    
    // Setup logging
    let log_level = match cli.log_level.as_str() {
        "DEBUG" => LevelFilter::Debug,
        "INFO" => LevelFilter::Info,
        "WARN" => LevelFilter::Warn,
        "ERROR" => LevelFilter::Error,
        _ => LevelFilter::Info,
    };
    
    SimpleLogger::new().with_level(log_level).init()?;
    
    // Configure thread pool
    if cli.threads > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(cli.threads)
            .build_global()?;
        info!("Using {} threads", cli.threads);
    }
    
    let field_extractions = parse_field_specifications(&cli.fields);
    
    info!("Fields to extract: {:?}", field_extractions);
    info!("Using batch size of {} records", cli.batch_size);
    info!("Statistics will be logged every {} seconds", cli.stats_interval);
    
    if let Some(provider) = &cli.provider {
        info!("Filtering by provider ID: {}", provider);
    }
    
    if let Some(client) = &cli.client {
        info!("Filtering by client ID: {}", client);
    }
    
    if cli.organize {
        info!("Output will be organized by provider/client in directory: {}", cli.output);
    } else {
        info!("Output will be written to single file: {}", cli.output);
    }
    
    memory_usage::log_memory_usage("startup");
    
    // Pre-compile path patterns for all requested fields
    info!("Pre-compiling field path patterns...");
    let path_patterns = initialize_path_patterns(&field_extractions);
    info!("Generated {} path patterns", path_patterns.len());
    
    info!("Finding files in {}...", cli.input);
    let files = find_jsonl_gz_files(&cli.input)?;
    info!("Found {} files to process", files.len());
    
    if files.is_empty() {
        warn!("No files found in {}. Exiting.", cli.input);
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
    
    let csv_writer_manager = CsvWriterManager::new(&cli.output, cli.organize, cli.max_open_files)?;
    
    let (tx, rx) = std::sync::mpsc::channel::<Option<Vec<FieldData>>>();
    let csv_writer_mutex = Arc::new(Mutex::new(csv_writer_manager));
    let stats = Arc::new(Mutex::new(IncrementalStats::new()));
    let stats_clone = Arc::clone(&stats);
    let stats_thread_running = Arc::new(Mutex::new(true));
    let stats_thread_running_clone = Arc::clone(&stats_thread_running);
    
    let stats_thread = std::thread::spawn(move || {
        let mut last_log_time = Instant::now();
        loop {
            std::thread::sleep(Duration::from_secs(1));
            if last_log_time.elapsed().as_secs() >= cli.stats_interval {
                memory_usage::log_memory_usage("periodic check");
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
    
    let batch_collector = Arc::new(Mutex::new(Vec::with_capacity(cli.batch_size)));
    
    // Use thread-safe shared reference to patterns
    let patterns = Arc::new(path_patterns);
    let field_extractions_clone = field_extractions.clone();
    
    // Create file processor using Command pattern
    let processor = Arc::new(JsonlProcessor {
        patterns: Arc::clone(&patterns),
        field_paths: field_extractions_clone,
        filter_provider: cli.provider,
        filter_client: cli.client,
    });
    
    files.par_iter().for_each(|filepath| {
        let processor_ref = Arc::clone(&processor);
        
        match processor_ref.process(filepath) {
            Ok(file_field_data) => {
                let file_name = filepath.file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| filepath.to_string_lossy().to_string());
                
                progress_bar.set_message(format!("Processed: {}", file_name));
                
                if !file_field_data.is_empty() {
                    let mut stats_guard = stats.lock().unwrap();
                    stats_guard.update(&file_field_data);
                    drop(stats_guard);
                    
                    let mut batch_guard = batch_collector.lock().unwrap();
                    batch_guard.extend(file_field_data);
                    
                    if batch_guard.len() >= cli.batch_size {
                        let batch_to_send = std::mem::replace(&mut *batch_guard, Vec::with_capacity(cli.batch_size));
                        drop(batch_guard);
                        
                        if let Err(e) = tx.send(Some(batch_to_send)) {
                            error!("Error sending batch to CSV writer: {}", e);
                        }
                    }
                }
            }
            Err((path, e)) => {
                error!("Error processing {}: {}", path.display(), e);
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
        info!("  Total field records: {}", stats_guard.record_stats.total_field_records);
        info!("  Unique DOIs/records: {}", stats_guard.record_stats.unique_records.len());
        
        info!("  Field breakdown:");
        for (field, count) in &stats_guard.field_stats.unique_fields {
            info!("    {}: {} records", field, count);
        }
        
        info!("  Unique providers: {}", stats_guard.provider_stats.providers.len());
        info!("  Unique clients: {}", stats_guard.provider_stats.clients.len());
        
        info!("Provider statistics:");
        for (provider, count) in stats_guard.provider_stats.providers.iter() {
            info!("  Provider {}: {} records", provider.0, count);
        }
        
        info!("Client statistics:");
        for (client, count) in stats_guard.provider_stats.clients.iter() {
            info!("  Client {}: {} records", client.0, count);
        }
    }
    
    memory_usage::log_memory_usage("completion");
    
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