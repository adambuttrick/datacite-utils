use anyhow::{Context, Result, anyhow};
use clap::Parser;
use csv::{ReaderBuilder, StringRecord};
use deunicode::deunicode;
use lazy_static::lazy_static;
use log::{LevelFilter, info};
use regex::Regex;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::Serialize;
use simple_logger::SimpleLogger;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

const DEFAULT_OUTPUT_FILENAMES: [(&str, &str); 6] = [
    ("with_ror", "affiliations_with_ror.json"),
    ("without_ror", "affiliations_without_ror.json"),
    ("overlap", "affiliation_overlap.json"),
    ("distribution", "normalized_distribution.json"),
    (
        "normalized_affiliation_dois",
        "normalized_affiliation_doi_distribution.json",
    ),
    ("identifier_dois", "ror_identifier_doi_distribution.json"),
];

#[derive(Parser, Debug)]
#[command(
    name = "affiliation-aggregator",
    about = "Stream affiliation stats from flattened CSV exports."
)]
struct Cli {
    #[arg(
        short,
        long,
        help = "Input CSV exported from fast-field-parser",
        value_name = "CSV"
    )]
    input: PathBuf,

    #[arg(
        short,
        long,
        help = "Directory for default output filenames",
        value_name = "DIR"
    )]
    output_dir: Option<PathBuf>,

    #[arg(long, help = "Override output path for affiliations with ROR IDs")]
    aff_with_ror: Option<PathBuf>,

    #[arg(long, help = "Override output path for affiliations without ROR IDs")]
    aff_without_ror: Option<PathBuf>,

    #[arg(long, help = "Override output path for affiliation overlap stats")]
    overlap_output: Option<PathBuf>,

    #[arg(long, help = "Override output path for normalized distribution stats")]
    distribution_output: Option<PathBuf>,

    #[arg(
        long,
        help = "Override output path for normalized affiliation DOI distribution"
    )]
    normalized_doi_output: Option<PathBuf>,

    #[arg(
        long,
        help = "Override output path for ROR/identifier DOI distribution"
    )]
    identifier_doi_output: Option<PathBuf>,

    #[arg(
        long,
        default_value_t = LevelFilter::Info,
        value_parser = parse_level,
        help = "Log level (ERROR, WARN, INFO, DEBUG, TRACE)"
    )]
    log_level: LevelFilter,

    #[arg(
        long,
        default_value_t = 5_000_000,
        help = "Log progress every N rows processed"
    )]
    log_every: u64,
}

fn parse_level(input: &str) -> std::result::Result<LevelFilter, String> {
    match input.to_ascii_uppercase().as_str() {
        "ERROR" => Ok(LevelFilter::Error),
        "WARN" => Ok(LevelFilter::Warn),
        "INFO" => Ok(LevelFilter::Info),
        "DEBUG" => Ok(LevelFilter::Debug),
        "TRACE" => Ok(LevelFilter::Trace),
        other => Err(format!("Invalid log level: {}", other)),
    }
}

#[derive(Default)]
struct PendingAffiliation {
    name: Option<String>,
    identifier: Option<String>,
    scheme: Option<String>,
}

#[derive(Default)]
struct AffiliationCounts {
    with_ror: u64,
    without_ror: u64,
    ror_counts: FxHashMap<String, u64>,
    provider_counts: FxHashMap<String, u64>,
    client_counts: FxHashMap<String, u64>,
}

#[derive(Default)]
struct NormalizedCounts {
    total: u64,
    affiliations: FxHashMap<String, u64>,
    dois: FxHashSet<String>,
    provider_counts: FxHashMap<String, u64>,
    client_counts: FxHashMap<String, u64>,
}

#[derive(Default)]
struct IdentifierCounts {
    total: u64,
    dois: FxHashSet<String>,
    provider_counts: FxHashMap<String, u64>,
    client_counts: FxHashMap<String, u64>,
}

#[derive(Default)]
struct Aggregator {
    affiliations: FxHashMap<String, AffiliationCounts>,
    normalized: FxHashMap<String, NormalizedCounts>,
    identifiers: FxHashMap<String, IdentifierCounts>,
}

impl Aggregator {
    fn add_entry(
        &mut self,
        doi: &str,
        affiliation: &str,
        normalized: Option<&str>,
        ror_id: Option<&str>,
        provider_id: Option<&str>,
        client_id: Option<&str>,
    ) {
        let entry = self
            .affiliations
            .entry(affiliation.to_string())
            .or_default();
        if let Some(ror) = ror_id {
            entry.with_ror += 1;
            *entry.ror_counts.entry(ror.to_string()).or_insert(0) += 1;
            let identifier_entry = self.identifiers.entry(ror.to_string()).or_default();
            identifier_entry.total += 1;
            identifier_entry.dois.insert(doi.to_string());
            if let Some(pid) = provider_id.and_then(non_empty_str) {
                *identifier_entry
                    .provider_counts
                    .entry(pid.to_string())
                    .or_insert(0) += 1;
            }
            if let Some(cid) = client_id.and_then(non_empty_str) {
                *identifier_entry
                    .client_counts
                    .entry(cid.to_string())
                    .or_insert(0) += 1;
            }
        } else {
            entry.without_ror += 1;
        }
        if let Some(pid) = provider_id.and_then(non_empty_str) {
            *entry.provider_counts.entry(pid.to_string()).or_insert(0) += 1;
        }
        if let Some(cid) = client_id.and_then(non_empty_str) {
            *entry.client_counts.entry(cid.to_string()).or_insert(0) += 1;
        }
        if let Some(norm) = normalized {
            let normalized_entry = self.normalized.entry(norm.to_string()).or_default();
            normalized_entry.total += 1;
            *normalized_entry
                .affiliations
                .entry(affiliation.to_string())
                .or_insert(0) += 1;
            normalized_entry.dois.insert(doi.to_string());
            if let Some(pid) = provider_id.and_then(non_empty_str) {
                *normalized_entry
                    .provider_counts
                    .entry(pid.to_string())
                    .or_insert(0) += 1;
            }
            if let Some(cid) = client_id.and_then(non_empty_str) {
                *normalized_entry
                    .client_counts
                    .entry(cid.to_string())
                    .or_insert(0) += 1;
            }
        }
    }

    fn with_ror_records(&self) -> Vec<AffiliationWithRorRecord> {
        let mut items: Vec<_> = self
            .affiliations
            .iter()
            .filter(|(_, stats)| stats.with_ror > 0)
            .map(|(aff, stats)| AffiliationWithRorRecord {
                affiliation: aff.clone(),
                occurrences: stats.with_ror,
                providers: entity_breakdown(&stats.provider_counts),
                clients: entity_breakdown(&stats.client_counts),
                ror_assignments: to_btree(&stats.ror_counts),
            })
            .collect();
        items.sort_by(|a, b| {
            b.occurrences
                .cmp(&a.occurrences)
                .then_with(|| a.affiliation.cmp(&b.affiliation))
        });
        items
    }

    fn without_ror_records(&self) -> Vec<AffiliationWithoutRorRecord> {
        let mut items: Vec<_> = self
            .affiliations
            .iter()
            .filter(|(_, stats)| stats.with_ror == 0)
            .map(|(aff, stats)| AffiliationWithoutRorRecord {
                affiliation: aff.clone(),
                occurrences: stats.without_ror,
                providers: entity_breakdown(&stats.provider_counts),
                clients: entity_breakdown(&stats.client_counts),
            })
            .collect();
        items.sort_by(|a, b| {
            b.occurrences
                .cmp(&a.occurrences)
                .then_with(|| a.affiliation.cmp(&b.affiliation))
        });
        items
    }

    fn overlap_records(&self) -> Vec<AffiliationOverlapRecord> {
        let mut items: Vec<_> = self
            .affiliations
            .iter()
            .filter(|(_, stats)| stats.with_ror > 0 && stats.without_ror > 0)
            .map(|(aff, stats)| AffiliationOverlapRecord {
                affiliation: aff.clone(),
                unassigned_occurrences: stats.without_ror,
                assigned_occurrences: stats.with_ror,
                identifier_occurrences: to_btree(&stats.ror_counts),
                providers: entity_breakdown(&stats.provider_counts),
                clients: entity_breakdown(&stats.client_counts),
            })
            .collect();
        items.sort_by(|a, b| {
            (b.assigned_occurrences + b.unassigned_occurrences)
                .cmp(&(a.assigned_occurrences + a.unassigned_occurrences))
                .then_with(|| a.affiliation.cmp(&b.affiliation))
        });
        items
    }

    fn distribution_records(&self) -> Vec<NormalizedDistributionRecord> {
        let mut items: Vec<_> = self
            .normalized
            .iter()
            .map(|(norm, stats)| NormalizedDistributionRecord {
                normalized: norm.clone(),
                total_count: stats.total,
                affiliations: affiliation_counts_vec(&stats.affiliations),
                providers: entity_breakdown(&stats.provider_counts),
                clients: entity_breakdown(&stats.client_counts),
            })
            .collect();
        items.sort_by(|a, b| {
            b.total_count
                .cmp(&a.total_count)
                .then_with(|| a.normalized.cmp(&b.normalized))
        });
        items
    }

    fn normalized_affiliation_doi_records(&self) -> Vec<NormalizedAffiliationDoisRecord> {
        let mut items: Vec<_> = self
            .normalized
            .iter()
            .map(|(norm, stats)| {
                let mut dois: Vec<_> = stats.dois.iter().cloned().collect();
                dois.sort();
                NormalizedAffiliationDoisRecord {
                    normalized: norm.clone(),
                    occurrences: stats.total,
                    dois,
                    providers: entity_breakdown(&stats.provider_counts),
                    clients: entity_breakdown(&stats.client_counts),
                }
            })
            .collect();
        items.sort_by(|a, b| {
            b.occurrences
                .cmp(&a.occurrences)
                .then_with(|| a.normalized.cmp(&b.normalized))
        });
        items
    }

    fn identifier_doi_records(&self) -> Vec<IdentifierDoisRecord> {
        let mut items: Vec<_> = self
            .identifiers
            .iter()
            .map(|(identifier, stats)| {
                let mut dois: Vec<_> = stats.dois.iter().cloned().collect();
                dois.sort();
                IdentifierDoisRecord {
                    identifier: identifier.clone(),
                    occurrences: stats.total,
                    dois,
                    providers: entity_breakdown(&stats.provider_counts),
                    clients: entity_breakdown(&stats.client_counts),
                }
            })
            .collect();
        items.sort_by(|a, b| {
            b.occurrences
                .cmp(&a.occurrences)
                .then_with(|| a.identifier.cmp(&b.identifier))
        });
        items
    }
}

#[derive(Serialize)]
struct AffiliationWithRorRecord {
    affiliation: String,
    occurrences: u64,
    providers: EntityBreakdown,
    clients: EntityBreakdown,
    ror_assignments: BTreeMap<String, u64>,
}

#[derive(Serialize)]
struct AffiliationWithoutRorRecord {
    affiliation: String,
    occurrences: u64,
    providers: EntityBreakdown,
    clients: EntityBreakdown,
}

#[derive(Serialize)]
struct AffiliationOverlapRecord {
    affiliation: String,
    unassigned_occurrences: u64,
    assigned_occurrences: u64,
    identifier_occurrences: BTreeMap<String, u64>,
    providers: EntityBreakdown,
    clients: EntityBreakdown,
}

#[derive(Serialize)]
struct NormalizedDistributionRecord {
    normalized: String,
    total_count: u64,
    affiliations: Vec<AffiliationCount>,
    providers: EntityBreakdown,
    clients: EntityBreakdown,
}

#[derive(Serialize)]
struct AffiliationCount {
    affiliation: String,
    occurrences: u64,
}

#[derive(Serialize)]
struct NormalizedAffiliationDoisRecord {
    normalized: String,
    occurrences: u64,
    dois: Vec<String>,
    providers: EntityBreakdown,
    clients: EntityBreakdown,
}

#[derive(Serialize)]
struct IdentifierDoisRecord {
    identifier: String,
    occurrences: u64,
    dois: Vec<String>,
    providers: EntityBreakdown,
    clients: EntityBreakdown,
}

#[derive(Serialize)]
struct EntityBreakdown {
    unique_total: u64,
    counts: BTreeMap<String, u64>,
}

fn to_btree(map: &FxHashMap<String, u64>) -> BTreeMap<String, u64> {
    map.iter().map(|(k, v)| (k.clone(), *v)).collect()
}

fn entity_breakdown(map: &FxHashMap<String, u64>) -> EntityBreakdown {
    EntityBreakdown {
        unique_total: map.len() as u64,
        counts: to_btree(map),
    }
}

fn affiliation_counts_vec(map: &FxHashMap<String, u64>) -> Vec<AffiliationCount> {
    let mut items: Vec<_> = map
        .iter()
        .map(|(affiliation, occurrences)| AffiliationCount {
            affiliation: affiliation.clone(),
            occurrences: *occurrences,
        })
        .collect();
    items.sort_by(|a, b| {
        b.occurrences
            .cmp(&a.occurrences)
            .then_with(|| a.affiliation.cmp(&b.affiliation))
    });
    items
}

#[derive(Default)]
struct ColumnIndices {
    doi: usize,
    provider_id: usize,
    client_id: usize,
    field_name: usize,
    subfield_path: usize,
    value: usize,
}

impl ColumnIndices {
    fn from_headers(headers: &StringRecord) -> Result<Self> {
        let mut indices = ColumnIndices::default();
        indices.doi = headers
            .iter()
            .position(|h| h == "doi")
            .ok_or_else(|| anyhow!("Missing 'doi' column"))?;
        indices.provider_id = headers
            .iter()
            .position(|h| h == "provider_id")
            .ok_or_else(|| anyhow!("Missing 'provider_id' column"))?;
        indices.client_id = headers
            .iter()
            .position(|h| h == "client_id")
            .ok_or_else(|| anyhow!("Missing 'client_id' column"))?;
        indices.field_name = headers
            .iter()
            .position(|h| h == "field_name")
            .ok_or_else(|| anyhow!("Missing 'field_name' column"))?;
        indices.subfield_path = headers
            .iter()
            .position(|h| h == "subfield_path")
            .ok_or_else(|| anyhow!("Missing 'subfield_path' column"))?;
        indices.value = headers
            .iter()
            .position(|h| h == "value")
            .ok_or_else(|| anyhow!("Missing 'value' column"))?;
        Ok(indices)
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    SimpleLogger::new()
        .with_level(cli.log_level)
        .init()
        .context("Initialize logger")?;
    run(cli)
}

fn run(cli: Cli) -> Result<()> {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_path(&cli.input)
        .with_context(|| format!("Failed to open {}", cli.input.display()))?;
    let headers = reader
        .headers()
        .context("Unable to read CSV headers")?
        .clone();
    let indices = ColumnIndices::from_headers(&headers)?;

    let mut aggregator = Aggregator::default();
    let mut current_doi: Option<String> = None;
    let mut current_provider: Option<String> = None;
    let mut current_client: Option<String> = None;
    let mut pending: FxHashMap<(String, String), PendingAffiliation> = FxHashMap::default();
    let mut processed_rows: u64 = 0;

    for record in reader.records() {
        let record = record?;
        processed_rows += 1;
        if cli.log_every > 0 && processed_rows % cli.log_every == 0 {
            info!("Processed {} rows", processed_rows);
        }
        let doi = record.get(indices.doi).unwrap_or("").trim();
        if doi.is_empty() {
            continue;
        }
        let provider_value = record.get(indices.provider_id).unwrap_or("").trim();
        let client_value = record.get(indices.client_id).unwrap_or("").trim();
        if current_doi.as_deref() != Some(doi) {
            if let Some(prev) = &current_doi {
                flush_pending(
                    prev,
                    current_provider.as_deref(),
                    current_client.as_deref(),
                    &mut pending,
                    &mut aggregator,
                );
            }
            current_doi = Some(doi.to_string());
            current_provider = None;
            current_client = None;
        }
        if !provider_value.is_empty() {
            current_provider = Some(provider_value.to_string());
        }
        if !client_value.is_empty() {
            current_client = Some(client_value.to_string());
        }
        handle_record(&record, &indices, &mut pending);
    }
    if let Some(last_doi) = &current_doi {
        flush_pending(
            last_doi,
            current_provider.as_deref(),
            current_client.as_deref(),
            &mut pending,
            &mut aggregator,
        );
    }

    let outputs = determine_output_paths(&cli);
    for path in outputs.values() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Create dir {}", parent.display()))?;
        }
    }

    let with_ror = aggregator.with_ror_records();
    let without_ror = aggregator.without_ror_records();
    let overlap = aggregator.overlap_records();
    let distribution = aggregator.distribution_records();
    let normalized_affiliation_dois = aggregator.normalized_affiliation_doi_records();
    let identifier_dois = aggregator.identifier_doi_records();

    write_json_records(
        outputs
            .get("with_ror")
            .expect("missing with_ror output path"),
        &with_ror,
    )?;
    write_json_records(
        outputs
            .get("without_ror")
            .expect("missing without_ror output path"),
        &without_ror,
    )?;
    write_json_records(
        outputs.get("overlap").expect("missing overlap output path"),
        &overlap,
    )?;
    write_json_records(
        outputs
            .get("distribution")
            .expect("missing distribution output path"),
        &distribution,
    )?;
    write_json_records(
        outputs
            .get("normalized_affiliation_dois")
            .expect("missing normalized affiliation doi output"),
        &normalized_affiliation_dois,
    )?;
    write_json_records(
        outputs
            .get("identifier_dois")
            .expect("missing identifier doi output"),
        &identifier_dois,
    )?;

    info!("Finished. Processed {} rows", processed_rows);
    Ok(())
}

fn determine_output_paths(cli: &Cli) -> FxHashMap<String, PathBuf> {
    let base_dir = cli
        .output_dir
        .clone()
        .or_else(|| cli.input.parent().map(|p| p.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."));
    let mut map = FxHashMap::default();
    for (key, filename) in DEFAULT_OUTPUT_FILENAMES {
        let custom = match key {
            "with_ror" => cli.aff_with_ror.clone(),
            "without_ror" => cli.aff_without_ror.clone(),
            "overlap" => cli.overlap_output.clone(),
            "distribution" => cli.distribution_output.clone(),
            "normalized_affiliation_dois" => cli.normalized_doi_output.clone(),
            "identifier_dois" => cli.identifier_doi_output.clone(),
            _ => None,
        };
        let path = custom.unwrap_or_else(|| base_dir.join(filename));
        map.insert(key.to_string(), path);
    }
    map
}

fn write_json_records<T: Serialize>(path: &Path, records: &[T]) -> Result<()> {
    let file = File::create(path).with_context(|| format!("Open {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, records)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

fn handle_record(
    record: &StringRecord,
    idx: &ColumnIndices,
    pending: &mut FxHashMap<(String, String), PendingAffiliation>,
) {
    let subfield = record.get(idx.subfield_path).unwrap_or("");
    if !subfield.contains("affiliation") {
        return;
    }
    let (base, attribute) = match subfield.rsplit_once('.') {
        Some(parts) => parts,
        None => return,
    };
    if !base.contains("affiliation") {
        return;
    }
    let field_name = record.get(idx.field_name).unwrap_or("");
    let key = (field_name.to_string(), base.to_string());
    let entry = pending.entry(key).or_default();
    let value = record.get(idx.value).unwrap_or("").to_string();
    match attribute {
        "name" => entry.name = Some(value),
        "affiliationIdentifier" => entry.identifier = Some(value),
        "affiliationIdentifierScheme" => entry.scheme = Some(value),
        _ => {}
    }
}

fn flush_pending(
    doi: &str,
    provider_id: Option<&str>,
    client_id: Option<&str>,
    pending: &mut FxHashMap<(String, String), PendingAffiliation>,
    aggregator: &mut Aggregator,
) {
    for pending_aff in pending.values() {
        if let Some(name) = pending_aff
            .name
            .as_deref()
            .and_then(sanitize_affiliation_value)
        {
            let normalized_owned = normalize_text(name);
            let normalized = normalized_owned.as_deref();
            let ror_owned = normalize_ror_identifier(
                pending_aff.identifier.as_deref(),
                pending_aff.scheme.as_deref(),
            );
            let ror = ror_owned.as_deref();
            aggregator.add_entry(doi, name, normalized, ror, provider_id, client_id);
        }
    }
    pending.clear();
}

fn sanitize_affiliation_value(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn non_empty_str(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn normalize_text(text: &str) -> Option<String> {
    if text.is_empty() {
        return None;
    }
    let ascii = if is_latin_char_text(text) {
        deunicode(text)
    } else {
        text.to_string()
    };
    let mut cleaned = String::with_capacity(ascii.len());
    for ch in ascii.to_lowercase().chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch.is_whitespace() {
            cleaned.push(ch);
        }
    }
    let normalized = cleaned.trim().to_string();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn is_latin_char_text(text: &str) -> bool {
    text.chars().any(|c| ('\u{0000}'..='\u{024F}').contains(&c))
}

lazy_static! {
    static ref ROR_REGEX: Regex =
        Regex::new(r"(?i)(?:https?://)?(?:www\.)?ror\.org/([0-9a-z]{9})").unwrap();
}

fn normalize_ror_identifier(identifier: Option<&str>, scheme: Option<&str>) -> Option<String> {
    let raw = identifier?.trim();
    if raw.is_empty() {
        return None;
    }
    if let Some(captures) = ROR_REGEX.captures(raw) {
        return Some(format!(
            "https://ror.org/{}",
            captures[1].to_ascii_lowercase()
        ));
    }
    if let Some(scheme_value) = scheme {
        if scheme_value.trim().eq_ignore_ascii_case("ror")
            && raw.len() == 9
            && raw.chars().all(|c| c.is_ascii_alphanumeric())
        {
            return Some(format!("https://ror.org/{}", raw.to_ascii_lowercase()));
        }
    }
    None
}
