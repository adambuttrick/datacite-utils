use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use anyhow::{anyhow, Result};
use clap::Parser;
use csv::Writer;
use flate2::read::GzDecoder;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use lazy_static::lazy_static;
use log::{error, info, LevelFilter};
use num_cpus;
use rayon::prelude::*;
use serde_json::Value;
use simple_logger::SimpleLogger;
use crossbeam_channel::{bounded, Sender};


#[derive(Parser)]
#[command(name = "DataCite Field Extractor")]
#[command(about = "Efficiently extracts any field data from DataCite metadata in compressed JSONL files using a PatternTrie")]
#[command(version = "1.1")]
struct Cli {
    #[arg(short, long, help = "Directory containing JSONL.gz files", required = true)]
    input: String,

    #[arg(short, long, default_value = "field_data.csv", help = "Output CSV file or directory")]
    output: String,

    #[arg(short, long, default_value = "INFO", help = "Logging level (DEBUG, INFO, WARN, ERROR)")]
    log_level: String,

    #[arg(short, long, default_value = "0", help = "Number of threads to use (0 for auto)")]
    threads: usize,

    #[arg(short, long, default_value = "5000", help = "Number of records to batch before sending to the writer thread")]
    batch_size: usize,

    #[arg(short = 'g', long, help = "Organize output by provider/client using an LRU cache for file handles")]
    organize: bool,

    #[arg(long, help = "Filter by provider ID")]
    provider: Option<String>,

    #[arg(long, help = "Filter by client ID")]
    client: Option<String>,
    
    #[arg(long, help = "Comma-separated list of resource types to include (e.g., 'Dataset,Text')")]
    resource_types: Option<String>,

    #[arg(long, help = "Only include records that contain all specified top-level fields")]
    require_all_fields: bool,
    
    #[arg(
        long = "field-value-filter",
        action = clap::ArgAction::Append,
        help = "Filter records where a field has a specific value (e.g., 'relatedIdentifiers.relationType=IsSupplementTo'). Can be used multiple times."
    )]
    field_value_filters: Vec<String>,
    
    #[arg(
        long = "field-does-not-exist",
        action = clap::ArgAction::Append,
        help = "Filter records where a field must NOT exist or be empty (null, [], {}). Can be used multiple times."
    )]
    field_does_not_exist: Vec<String>,

    #[arg(long, default_value = "100", help = "Maximum number of open files when using --organize")]
    max_open_files: usize,

    #[arg(short = 'f', long, default_value = "creators.name", help = "Comma-separated list of fields to extract")]
    fields: String,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Doi(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ProviderId(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ClientId(String);

#[derive(Debug, Clone)]
struct FieldData {
    doi: Doi,
    provider_id: ProviderId,
    client_id: ClientId,
    field_name: String,
    subfield_path: String,
    value: String,
}

const ARRAY_TRAVERSAL_KEY: &str = "[]";

lazy_static! {
    static ref SCHEMA_STRUCTURE: HashMap<String, bool> = {
        let mut schema = HashMap::new();
        let array_fields = [
            "identifiers", "alternateIdentifiers", "creators",
            "creators.affiliation", "creators.nameIdentifiers",
            "titles", "subjects", "contributors",
            "contributors.affiliation", "contributors.nameIdentifiers",
            "dates", "relatedIdentifiers", "relatedItems",
            "relatedItems.titles", "relatedItems.creators",
            "relatedItems.contributors", "sizes", "formats",
            "rightsList", "descriptions", "geoLocations",
            "geoLocations.geoLocationPolygon", "fundingReferences",
        ];
        for field in array_fields {
            schema.insert(field.to_string(), true);
        }
        schema
    };
}

#[derive(Default, Debug)]
struct TrieNode {
    children: HashMap<String, TrieNode>,
    terminating_pattern: Option<String>,
}

#[derive(Debug)]
struct PatternTrie {
    root: TrieNode,
}

impl PatternTrie {
     fn new(field_specs: &[Vec<String>]) -> Self {
        let mut root = TrieNode::default();
        for spec in field_specs {
            let mut current_node = &mut root;
            let full_path = spec;
            let mut current_path_parts: Vec<&str> = Vec::new();

            for part in full_path {
                current_path_parts.push(part);
                current_node = current_node.children.entry(part.clone()).or_default();
                let current_schema_path = current_path_parts.join(".");
                if SCHEMA_STRUCTURE.get(&current_schema_path).is_some() {
                    current_node = current_node.children.entry(ARRAY_TRAVERSAL_KEY.to_string()).or_default();
                }
            }
            current_node.terminating_pattern = Some(spec[0].clone());
        }
        Self { root }
    }

    fn extract(&self, json_attributes: &Value, doi: Doi, provider_id: ProviderId, client_id: ClientId) -> Vec<FieldData> {
        let mut results = Vec::new();
        self.traverse(&self.root, json_attributes, "", &mut results, &doi, &provider_id, &client_id);
        results
    }

    fn traverse<'a>( &self, node: &'a TrieNode, json_value: &'a Value, current_path: &str, results: &mut Vec<FieldData>, doi: &Doi, provider_id: &ProviderId, client_id: &ClientId) {
        if let Some(field_name) = &node.terminating_pattern {
            let value_str = match json_value {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            results.push(FieldData {
                doi: doi.clone(), provider_id: provider_id.clone(), client_id: client_id.clone(),
                field_name: field_name.clone(), subfield_path: current_path.to_string(), value: value_str,
            });
        }
        for (key, child_node) in &node.children {
            if key == ARRAY_TRAVERSAL_KEY {
                if let Some(array) = json_value.as_array() {
                    for (i, item) in array.iter().enumerate() {
                        let new_path = format!("{}[{}]", current_path, i);
                        self.traverse(child_node, item, &new_path, results, doi, provider_id, client_id);
                    }
                }
            } else if let Some(next_value) = json_value.get(key) {
                let new_path = if current_path.is_empty() { key.clone() } else { format!("{}.{}", current_path, key) };
                self.traverse(child_node, next_value, &new_path, results, doi, provider_id, client_id);
            }
        }
    }
}


fn parse_field_specifications(field_specs: &str) -> Vec<Vec<String>> {
    field_specs.split(',').map(|spec| spec.trim().split('.').map(|part| part.trim().to_string()).collect()).collect()
}

fn validate_field_value(attributes_val: &Value, path_parts: &[String], required_value: &str) -> bool {
    let mut current_nodes = vec![attributes_val];

    for (i, part) in path_parts.iter().enumerate() {
        if current_nodes.is_empty() { return false; }
        
        let mut next_nodes = Vec::new();
        let is_last_part = i == path_parts.len() - 1;
        
        for node in &current_nodes {
            if let Some(value) = node.get(part) {
                if is_last_part {
                    if let Some(array) = value.as_array() {
                        if array.iter().any(|v| v.as_str() == Some(required_value)) {
                            return true;
                        }
                    } else if value.as_str() == Some(required_value) {
                        return true;
                    }
                } else {
                    if let Some(array) = value.as_array() {
                        next_nodes.extend(array.iter());
                    } else {
                        next_nodes.push(value);
                    }
                }
            }
        }
        
        if is_last_part { return false; }
        current_nodes = next_nodes;
    }
    
    false
}


fn path_exists(attributes_val: &Value, path_parts: &[String]) -> bool {
    let mut current_node = attributes_val;
    for part in path_parts {
        if let Some(next_node) = current_node.get(part) {
            current_node = next_node;
        } else {
            return false;
        }
    }

    !match current_node {
        Value::Null => true,
        Value::Array(arr) => arr.is_empty(),
        Value::Object(obj) => obj.is_empty(),
        _ => false,
    }
}


fn find_jsonl_gz_files<P: AsRef<Path>>(directory: P) -> Result<Vec<PathBuf>> {
    let pattern = directory.as_ref().join("**/*.jsonl.gz");
    info!("Searching for files matching pattern: {}", pattern.to_string_lossy());
    Ok(glob(&pattern.to_string_lossy())?.filter_map(Result::ok).collect())
}

trait FileProcessor {
    fn process(&self, filepath: &Path, tx: Sender<Vec<FieldData>>) -> Result<(), (PathBuf, anyhow::Error)>;
}

struct JsonlProcessor {
    trie: Arc<PatternTrie>,
    filter_provider: Option<String>,
    filter_client: Option<String>,
    filter_resource_types: Option<HashSet<String>>,
    required_fields: Option<HashSet<String>>,
    field_value_filters: Vec<(Vec<String>, String)>,
    exclusion_filters: Vec<Vec<String>>,
    batch_size: usize,
}

impl FileProcessor for JsonlProcessor {
    fn process(&self, filepath: &Path, tx: Sender<Vec<FieldData>>) -> Result<(), (PathBuf, anyhow::Error)> {
        let file = File::open(filepath).map_err(|e| (filepath.to_path_buf(), anyhow::Error::new(e).context("Failed to open file")))?;
        let decoder = GzDecoder::new(file);
        let reader = BufReader::new(decoder);
        
        let mut batch = Vec::with_capacity(self.batch_size);

        for line in reader.lines() {
            let line_str = line.map_err(|e| (filepath.to_path_buf(), anyhow::Error::new(e).context("Failed to read line")))?;
            if line_str.trim().is_empty() { continue; }

            if let Ok(record) = serde_json::from_str::<Value>(&line_str) {
                let attributes = &record["attributes"];
                if attributes.is_null() { continue; }

                if let Some(allowed_types) = &self.filter_resource_types {
                    let resource_type_general = attributes.pointer("/types/resourceTypeGeneral").and_then(Value::as_str);
                    if resource_type_general.map_or(true, |rt| !allowed_types.contains(rt)) {
                        continue;
                    }
                }

                if !self.field_value_filters.is_empty() {
                    if !self.field_value_filters.iter().all(|(path, val)| validate_field_value(attributes, path, val)) {
                        continue;
                    }
                }

                if !self.exclusion_filters.is_empty() {
                    if self.exclusion_filters.iter().any(|path| path_exists(attributes, path)) {
                        continue;
                    }
                }
                
                let (Some(provider_id), Some(client_id), Some(doi)) = (extract_provider_id(&record), extract_client_id(&record), extract_doi(&record)) else { continue; };
                if self.filter_provider.as_ref().is_some_and(|p| *p != provider_id.0) { continue; }
                if self.filter_client.as_ref().is_some_and(|c| *c != client_id.0) { continue; }
                
                let mut extracted_data = self.trie.extract(attributes, doi, provider_id, client_id);

                if let Some(required) = &self.required_fields {
                    if !extracted_data.is_empty() {
                        let found_fields: HashSet<String> = extracted_data.iter().map(|data| data.field_name.clone()).collect();
                        if found_fields.len() < required.len() {
                            extracted_data.clear();
                        }
                    } else { continue; }
                }

                if extracted_data.is_empty() { continue; }
                
                batch.append(&mut extracted_data);

                if batch.len() >= self.batch_size {
                    if tx.send(std::mem::take(&mut batch)).is_err() {
                        error!("Writer thread disconnected. Aborting processing for {}", filepath.display());
                        return Ok(());
                    }
                }
            }
        }

        if !batch.is_empty() {
            if tx.send(batch).is_err() {
                 error!("Writer thread disconnected. Could not send final batch for {}", filepath.display());
            }
        }
        
        Ok(())
    }
}

fn extract_doi(record: &Value) -> Option<Doi> {
    record.get("id").and_then(Value::as_str).map(|s| Doi(s.to_string())).or_else(|| record.pointer("/attributes/doi").and_then(Value::as_str).map(|s| Doi(s.to_string())))
}
fn extract_provider_id(record: &Value) -> Option<ProviderId> {
    record.pointer("/relationships/provider/data/id").and_then(Value::as_str).map(|s| ProviderId(s.to_string()))
}
fn extract_client_id(record: &Value) -> Option<ClientId> {
    record.pointer("/relationships/client/data/id").and_then(Value::as_str).map(|s| ClientId(s.to_string()))
}


trait OutputStrategy: Send {
    fn write_batch(&mut self, batch: &[FieldData]) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}

struct SingleFileOutput { writer: Writer<File> }
impl SingleFileOutput {
    fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut writer = Writer::from_path(path)?;
        writer.write_record(&["doi", "provider_id", "client_id", "field_name", "subfield_path", "value"])?;
        writer.flush()?;
        Ok(Self { writer })
    }
}
impl OutputStrategy for SingleFileOutput {
    fn write_batch(&mut self, batch: &[FieldData]) -> Result<()> {
        for data in batch {
            self.writer.write_record(&[&data.doi.0, &data.provider_id.0, &data.client_id.0, &data.field_name, &data.subfield_path, &data.value])?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<()> { Ok(self.writer.flush()?) }
}

struct OrganizedOutput {
    base_output_dir: PathBuf,
    current_writers: HashMap<(ProviderId, ClientId), Writer<File>>,
    access_order: VecDeque<(ProviderId, ClientId)>,
    created_files: HashSet<PathBuf>,
    max_open_files: usize,
}
impl OrganizedOutput {
    fn new<P: AsRef<Path>>(output_path: P, max_open_files: usize) -> Result<Self> {
        let path = output_path.as_ref();
        fs::create_dir_all(path)?;
        info!("Created output directory: {}. Using LRU cache for up to {} open files.", path.display(), max_open_files);
        Ok(Self { base_output_dir: path.to_path_buf(), current_writers: HashMap::new(), access_order: VecDeque::new(), created_files: HashSet::new(), max_open_files })
    }
    fn get_writer(&mut self, provider_id: &ProviderId, client_id: &ClientId) -> Result<&mut Writer<File>> {
        let key = (provider_id.clone(), client_id.clone());
        if let Some(pos) = self.access_order.iter().position(|k| k == &key) {
            self.access_order.remove(pos);
            self.access_order.push_front(key.clone());
        } else {
            if self.current_writers.len() >= self.max_open_files {
                if let Some(lru_key) = self.access_order.pop_back() {
                    if let Some(mut writer) = self.current_writers.remove(&lru_key) { writer.flush()?; }
                }
            }
            let provider_dir = self.base_output_dir.join(&provider_id.0);
            fs::create_dir_all(&provider_dir)?;
            let client_file = provider_dir.join(format!("{}.csv", client_id.0));
            let write_header = !self.created_files.contains(&client_file);
            let file = OpenOptions::new().create(true).write(true).append(true).open(&client_file)?;
            let mut writer = Writer::from_writer(file);
            if write_header {
                writer.write_record(&["doi", "provider_id", "client_id", "field_name", "subfield_path", "value"])?;
                writer.flush()?;
                self.created_files.insert(client_file);
            }
            self.current_writers.insert(key.clone(), writer);
            self.access_order.push_front(key.clone());
        }
        Ok(self.current_writers.get_mut(&key).unwrap())
    }
}
impl OutputStrategy for OrganizedOutput {
    fn write_batch(&mut self, batch: &[FieldData]) -> Result<()> {
        let mut grouped_records: HashMap<(ProviderId, ClientId), Vec<&FieldData>> = HashMap::new();
        for data in batch {
            grouped_records.entry((data.provider_id.clone(), data.client_id.clone())).or_default().push(data);
        }
        for ((provider_id, client_id), records) in grouped_records {
            let writer = self.get_writer(&provider_id, &client_id)?;
            for data in records {
                 writer.write_record(&[&data.doi.0, &data.provider_id.0, &data.client_id.0, &data.field_name, &data.subfield_path, &data.value])?;
            }
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        for (_, writer) in self.current_writers.iter_mut() { writer.flush()?; }
        Ok(())
    }
}
struct CsvWriterManager { output_strategy: Box<dyn OutputStrategy> }
impl CsvWriterManager {
    fn new<P: AsRef<Path>>(output_path: P, organize: bool, max_open_files: usize) -> Result<Self> {
        let strategy: Box<dyn OutputStrategy> = if organize { Box::new(OrganizedOutput::new(output_path, max_open_files)?) } else { Box::new(SingleFileOutput::new(output_path)?) };
        Ok(Self { output_strategy: strategy })
    }
    fn write_batch(&mut self, batch: &[FieldData]) -> Result<()> { self.output_strategy.write_batch(batch) }
    fn flush_all(&mut self) -> Result<()> { self.output_strategy.flush() }
}
impl Drop for CsvWriterManager {
    fn drop(&mut self) {
        if let Err(e) = self.flush_all() { error!("Error flushing CSV writers during cleanup: {}", e); }
    }
}


fn main() -> Result<()> {
    let start_time = Instant::now();
    let cli = Cli::parse();
    
    SimpleLogger::new().with_level(match cli.log_level.to_uppercase().as_str() {
        "DEBUG" => LevelFilter::Debug, "INFO" => LevelFilter::Info,
        "WARN" => LevelFilter::Warn, "ERROR" => LevelFilter::Error,
        _ => LevelFilter::Info,
    }).init()?;
    
    let num_threads = if cli.threads > 0 { cli.threads } else { num_cpus::get() };
    rayon::ThreadPoolBuilder::new().num_threads(num_threads).build_global()?;
    info!("Using {} threads for file processing.", num_threads);

    let resource_types_filter: Option<HashSet<String>> = cli.resource_types.map(|s| {
        s.split(',').map(|item| item.trim().to_string()).collect()
    });
    if let Some(types) = &resource_types_filter {
        info!("Filtering for resource types: {:?}", types);
    }
    
    let field_value_filters = cli.field_value_filters
        .iter()
        .map(|s| {
            s.split_once('=').map(|(path, val)| {
                (path.split('.').map(str::to_string).collect(), val.to_string())
            })
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| anyhow!("Invalid field-value filter format. Expected 'path.to.field=value'"))?;

    if !field_value_filters.is_empty() {
        info!("Applying field-value filters: {:?}", cli.field_value_filters);
    }

    let exclusion_filters: Vec<Vec<String>> = cli.field_does_not_exist
        .iter()
        .map(|s| s.split('.').map(str::to_string).collect())
        .collect();
    
    if !exclusion_filters.is_empty() {
        info!("Applying field exclusion filters for: {:?}", cli.field_does_not_exist);
    }

    let field_extractions = parse_field_specifications(&cli.fields);

    let required_fields_set: Option<HashSet<String>> = if cli.require_all_fields {
        let set: HashSet<String> = field_extractions.iter().map(|spec| spec[0].clone()).collect();
        if !set.is_empty() { Some(set) } else { None }
    } else {
        None
    };
    if let Some(fields) = &required_fields_set {
        info!("Requiring all top-level fields to be present: {:?}", fields);
    }

    info!("Building PatternTrie for fields: {}", &cli.fields);
    let trie = Arc::new(PatternTrie::new(&field_extractions));
    
    info!("Finding files in {}...", cli.input);
    let files = find_jsonl_gz_files(&cli.input)?;
    let total_files = files.len();
    info!("Found {} files to process.", total_files);
    if files.is_empty() { return Ok(()); }

    let progress_bar = ProgressBar::new(total_files as u64);
    progress_bar.set_style(ProgressStyle::default_bar().template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}").unwrap().progress_chars("#>-"));
    
    let channel_capacity = num_threads * 4;
    info!("Using a bounded channel with capacity: {}", channel_capacity);
    let (tx, rx) = bounded::<Vec<FieldData>>(channel_capacity);
    
    let csv_writer_manager = CsvWriterManager::new(&cli.output, cli.organize, cli.max_open_files)?;
    let writer_mutex = Arc::new(Mutex::new(csv_writer_manager));
    
    let writer_thread = {
        let writer_mutex = Arc::clone(&writer_mutex);
        std::thread::spawn(move || {
            let mut writer_manager = writer_mutex.lock().unwrap();
            let mut batches_processed: u32 = 0;
            const FLUSH_INTERVAL: u32 = 100;

            while let Ok(batch) = rx.recv() {
                if let Err(e) = writer_manager.write_batch(&batch) {
                    error!("Error writing batch to CSV: {}", e);
                    continue;
                }
                
                batches_processed += 1;

                if batches_processed % FLUSH_INTERVAL == 0 {
                    if let Err(e) = writer_manager.flush_all() {
                        error!("Error flushing CSV buffer: {}", e);
                    }
                }
            }
            
            if let Err(e) = writer_manager.flush_all() {
                error!("Error on final flush: {}", e);
            }
        })
    };

    let processor = Arc::new(JsonlProcessor {
        trie: Arc::clone(&trie),
        filter_provider: cli.provider,
        filter_client: cli.client,
        filter_resource_types: resource_types_filter,
        required_fields: required_fields_set,
        field_value_filters,
        exclusion_filters,
        batch_size: cli.batch_size,
    });

    files.par_iter().for_each_with(tx.clone(), |tx_clone, filepath| {
        let file_name_short = filepath.file_name().unwrap_or_default().to_string_lossy();
        progress_bar.set_message(format!("Processing: {}", file_name_short));
        
        if let Err((path, e)) = processor.process(filepath, tx_clone.clone()) {
             error!("Error processing {}: {}", path.display(), e)
        }
        progress_bar.inc(1);
    });
    
    drop(tx); 
    writer_thread.join().expect("CSV writer thread panicked");
    progress_bar.finish_with_message("Processing complete.");
    
    info!("\n--- Final Report ---");
    info!("Processed {} files.", total_files);
    info!("Total execution time: {}", format_elapsed(start_time.elapsed()));
    
    Ok(())
}

fn format_elapsed(elapsed: Duration) -> String {
    let secs = elapsed.as_secs();
    if secs >= 3600 {
        format!("{}h {}m {}s", secs / 3600, (secs % 3600) / 60, secs % 60)
    } else if secs >= 60 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}.{:03}s", secs, elapsed.subsec_millis())
    }
}