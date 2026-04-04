//! Benchmark support for driving `PezhaiServer` through loopback TCP transport.
//!
//! This module intentionally keeps workload assembly and statistics logic
//! transport-agnostic so tests can validate behavior deterministically without
//! sockets. The bench binary supplies one transport-specific client factory.

use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::pezhai::types::Bound;
use crate::sevai::tcp::{TcpRpcClient, TcpTransportConfig, spawn_loopback_transport};
use crate::sevai::{
    ExternalMethod, ExternalRequest, ExternalResponse, ExternalResponsePayload, PezhaiServer,
    ServerBootstrapArgs,
};

/// Default scan page record limit used by benchmark scan operations.
pub const DEFAULT_SCAN_MAX_RECORDS_PER_PAGE: u32 = 128;
/// Default scan page byte limit used by benchmark scan operations.
pub const DEFAULT_SCAN_MAX_BYTES_PER_PAGE: u32 = 256 * 1024;
/// Maximum rows consumed by one logical benchmark scan operation.
pub const DEFAULT_SCAN_MAX_TOTAL_ROWS: usize = 1000;
/// Default operation preset used when neither percentages nor a preset are set.
pub const DEFAULT_PRESET_NAME: &str = "balanced";

const DEFAULT_CLIENTS: usize = 4;
const DEFAULT_POPULATE_CLIENTS: usize = 4;
const DEFAULT_POPULATE_KEYS: usize = 10_000;
const DEFAULT_DURATION_SECS: u64 = 30;
const DEFAULT_VALUE_BYTES_MIN: usize = 64;
const DEFAULT_VALUE_BYTES_MAX: usize = 1024;

const DEFAULT_WAL_SEGMENT_BYTES: u64 = 1_073_741_824;
const DEFAULT_GROUP_COMMIT_BYTES: u64 = 65_536;
const DEFAULT_GROUP_COMMIT_MAX_DELAY_MS: u64 = 50;
const DEFAULT_MEMTABLE_FLUSH_BYTES: u64 = 67_108_864;
const DEFAULT_BASE_LEVEL_BYTES: u64 = 268_435_456;
const DEFAULT_LEVEL_FANOUT: u32 = 10;
const DEFAULT_L0_FILE_THRESHOLD: u32 = 8;
const DEFAULT_MAX_LEVELS: u32 = 7;
const DEFAULT_MAX_EXTERNAL_REQUESTS: usize = 64;
const DEFAULT_MAX_SCAN_FETCH_QUEUE: usize = 8;
const DEFAULT_MAX_SCAN_SESSIONS: usize = 32;
const DEFAULT_MAX_WORKER_TASKS: usize = 64;
const DEFAULT_MAX_WAITING_WAL_SYNCS: usize = 64;
const DEFAULT_SCAN_EXPIRY_MS: u64 = 30_000;
const DEFAULT_RETRY_DELAY_MS: u64 = 25;
const DEFAULT_WORKER_THREADS: usize = 2;
const DEFAULT_GC_INTERVAL_SECS: u64 = 300;
const DEFAULT_CHECKPOINT_INTERVAL_SECS: u64 = 300;
const DEFAULT_PAGE_SIZE_BYTES: usize = 4096;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// One benchmark failure with a stable printable message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BenchmarkError {
    message: String,
}

impl BenchmarkError {
    /// Builds one benchmark error from a human-readable message.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for BenchmarkError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for BenchmarkError {}

impl From<BenchmarkError> for Error {
    fn from(value: BenchmarkError) -> Self {
        Error::invalid_argument(value.message)
    }
}

/// One operation-mix shape used by workload selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationMix {
    /// Percentage of `Get` operations.
    pub get_percent: u8,
    /// Percentage of `Put` operations.
    pub put_percent: u8,
    /// Percentage of `Delete` operations.
    pub delete_percent: u8,
    /// Percentage of `Scan` operations.
    pub scan_percent: u8,
}

impl OperationMix {
    /// Validates that each field is in `0..=100` and total is exactly 100.
    pub fn validate(&self) -> std::result::Result<(), BenchmarkError> {
        let total = u16::from(self.get_percent)
            + u16::from(self.put_percent)
            + u16::from(self.delete_percent)
            + u16::from(self.scan_percent);
        if total != 100 {
            return Err(BenchmarkError::new(format!(
                "operation percentages must sum to 100, got {total}"
            )));
        }
        Ok(())
    }
}

/// CLI input after parsing but before semantic validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BenchmarkCli {
    /// Optional path to an existing `store/config.toml`.
    pub config: Option<PathBuf>,
    /// Number of benchmark client connections.
    pub clients: usize,
    /// Number of concurrent client connections used during pre-population.
    pub populate_clients: usize,
    /// Number of keys pre-populated before measurement.
    pub populate_keys: usize,
    /// Timed measurement duration in seconds.
    pub duration_secs: u64,
    /// Optional operation mix preset.
    pub preset: Option<String>,
    /// Optional explicit operation percentages.
    pub explicit_mix: Option<OperationMix>,
    /// Inclusive minimum value size in bytes.
    pub value_bytes_min: usize,
    /// Inclusive maximum value size in bytes.
    pub value_bytes_max: usize,
}

impl Default for BenchmarkCli {
    fn default() -> Self {
        Self {
            config: None,
            clients: DEFAULT_CLIENTS,
            populate_clients: DEFAULT_POPULATE_CLIENTS,
            populate_keys: DEFAULT_POPULATE_KEYS,
            duration_secs: DEFAULT_DURATION_SECS,
            preset: None,
            explicit_mix: None,
            value_bytes_min: DEFAULT_VALUE_BYTES_MIN,
            value_bytes_max: DEFAULT_VALUE_BYTES_MAX,
        }
    }
}

/// Resolved benchmark settings after applying presets and validation rules.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BenchmarkSettings {
    /// Optional path to an existing `store/config.toml`.
    pub config: Option<PathBuf>,
    /// Number of benchmark client connections.
    pub clients: usize,
    /// Number of concurrent client connections used during pre-population.
    pub populate_clients: usize,
    /// Number of keys pre-populated before measurement.
    pub populate_keys: usize,
    /// Timed measurement duration in seconds.
    pub duration_secs: u64,
    /// Final operation mix used during measurement.
    pub mix: OperationMix,
    /// Inclusive minimum value size in bytes.
    pub value_bytes_min: usize,
    /// Inclusive maximum value size in bytes.
    pub value_bytes_max: usize,
}

impl BenchmarkSettings {
    /// Returns the timed benchmark duration.
    #[must_use]
    pub fn duration(&self) -> Duration {
        Duration::from_secs(self.duration_secs)
    }

    /// Validates non-mix benchmark limits and ranges.
    pub fn validate(&self) -> std::result::Result<(), BenchmarkError> {
        if self.clients == 0 {
            return Err(BenchmarkError::new("--clients must be greater than zero"));
        }
        if self.populate_clients == 0 {
            return Err(BenchmarkError::new(
                "--populate-clients must be greater than zero",
            ));
        }
        if self.populate_keys == 0 {
            return Err(BenchmarkError::new(
                "--populate-keys must be greater than zero",
            ));
        }
        if self.duration_secs == 0 {
            return Err(BenchmarkError::new("--duration must be greater than zero"));
        }
        if self.value_bytes_min > self.value_bytes_max {
            return Err(BenchmarkError::new(
                "--value-bytes-min must be <= --value-bytes-max",
            ));
        }
        self.mix.validate()
    }
}

/// One resolved benchmark config path and optional temporary-store cleanup guard.
#[derive(Debug)]
pub struct BenchmarkConfigPath {
    /// Path to `store/config.toml` used by the benchmark server bootstrap.
    pub config_path: PathBuf,
    _temp_store_guard: Option<TempStoreGuard>,
}

/// One operation kind measured by the benchmark.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum BenchOperation {
    /// `Get` operation.
    Get,
    /// `Put` operation.
    Put,
    /// `Delete` operation.
    Delete,
    /// Full logical scan operation.
    Scan,
}

impl BenchOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Put => "put",
            Self::Delete => "delete",
            Self::Scan => "scan",
        }
    }
}

/// One weighted selector used to sample operation kinds from percentages.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationSelector {
    cut_get: u16,
    cut_put: u16,
    cut_delete: u16,
}

impl OperationSelector {
    /// Builds one selector from a validated operation mix.
    #[must_use]
    pub fn from_mix(mix: OperationMix) -> Self {
        let cut_get = u16::from(mix.get_percent);
        let cut_put = cut_get + u16::from(mix.put_percent);
        let cut_delete = cut_put + u16::from(mix.delete_percent);
        Self {
            cut_get,
            cut_put,
            cut_delete,
        }
    }

    /// Chooses one operation for a random number in `[0, 99]`.
    #[must_use]
    pub fn choose(&self, sample_0_to_99: u16) -> BenchOperation {
        if sample_0_to_99 < self.cut_get {
            BenchOperation::Get
        } else if sample_0_to_99 < self.cut_put {
            BenchOperation::Put
        } else if sample_0_to_99 < self.cut_delete {
            BenchOperation::Delete
        } else {
            BenchOperation::Scan
        }
    }
}

/// One benchmark scan operation shape that remains deterministic for tests.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanWorkload {
    /// Inclusive finite lower bound for this scan operation.
    pub start_key: Vec<u8>,
    /// Page record limit for each `ScanFetchNext`.
    pub max_records_per_page: u32,
    /// Page byte limit for each `ScanFetchNext`.
    pub max_bytes_per_page: u32,
    /// Total row limit across all fetched pages in one logical scan op.
    pub max_total_rows: usize,
}

/// One immutable key universe shared by populate and timed measurement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyUniverse {
    keys: Vec<Vec<u8>>,
}

impl KeyUniverse {
    /// Builds a deterministic key universe with exactly `count` keys.
    #[must_use]
    pub fn new(count: usize) -> Self {
        let mut keys = Vec::with_capacity(count);
        for index in 0..count {
            keys.push(format!("bench-key-{index:016}").into_bytes());
        }
        Self { keys }
    }

    /// Returns the total key count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Returns whether the key universe is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Returns one key by index.
    #[must_use]
    pub fn key(&self, index: usize) -> &[u8] {
        &self.keys[index]
    }
}

/// One client-visible percentile summary computed from sorted nanosecond samples.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PercentileSummary {
    /// Smallest sample.
    pub min: u64,
    /// 50th percentile.
    pub p50: u64,
    /// 95th percentile.
    pub p95: u64,
    /// 99th percentile.
    pub p99: u64,
    /// 99.9th percentile.
    pub p999: u64,
    /// Largest sample.
    pub max: u64,
}

/// One benchmark report row for either one operation type or aggregate results.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReportRow {
    /// Row label (`get`, `put`, `delete`, `scan`, or `aggregate`).
    pub label: String,
    /// Number of completed operations in this row.
    pub count: usize,
    /// Throughput for this row in operations per second.
    pub tps: u64,
    /// Percentile summary for this row.
    pub latency_ns: PercentileSummary,
}

/// Full benchmark report with per-operation and aggregate rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BenchmarkReport {
    /// Elapsed timed window in nanoseconds.
    pub elapsed_ns: u64,
    /// Per-operation rows keyed by operation name.
    pub per_operation: BTreeMap<BenchOperation, ReportRow>,
    /// Aggregate row across all operations.
    pub aggregate: ReportRow,
}

/// Minimal async transport abstraction required by benchmark execution.
pub trait BenchmarkClient: Send {
    /// Sends one request and returns one terminal response.
    fn call<'a>(&'a mut self, request: ExternalRequest) -> BoxFuture<'a, Result<ExternalResponse>>;
}

/// Factory that creates one benchmark client connection per logical benchmark client.
pub trait BenchmarkClientFactory: Send + Sync {
    /// Connects one client using the stable benchmark client id.
    fn connect<'a>(&'a self, client_id: String) -> BoxFuture<'a, Result<Box<dyn BenchmarkClient>>>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TcpBenchmarkClientFactory {
    addr: SocketAddr,
    config: TcpTransportConfig,
}

impl BenchmarkClientFactory for TcpBenchmarkClientFactory {
    fn connect<'a>(
        &'a self,
        _client_id: String,
    ) -> BoxFuture<'a, Result<Box<dyn BenchmarkClient>>> {
        Box::pin(async move {
            let client = TcpRpcClient::connect(self.addr, self.config)
                .await
                .map_err(Error::from)?;
            Ok(Box::new(TcpBenchmarkClient { inner: client }) as Box<dyn BenchmarkClient>)
        })
    }
}

#[derive(Debug)]
struct TcpBenchmarkClient {
    inner: TcpRpcClient,
}

impl BenchmarkClient for TcpBenchmarkClient {
    fn call<'a>(&'a mut self, request: ExternalRequest) -> BoxFuture<'a, Result<ExternalResponse>> {
        Box::pin(async move { self.inner.call(request).await.map_err(Error::from) })
    }
}

/// Runs the loopback TCP benchmark from CLI args.
///
/// This entrypoint starts one in-process `PezhaiServer`, one loopback TCP
/// transport adapter, and the configured number of benchmark clients in the
/// same process.
pub fn run_sevai_tcp_benchmark<I, S>(args: I) -> Result<BenchmarkReport>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let parsed = parse_cli_args(args).map_err(Error::from)?;
    let settings = resolve_settings(parsed).map_err(Error::from)?;
    let config = resolve_config_path(&settings)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(|error| Error::io(error.to_string()))?;

    runtime.block_on(async move {
        let server = PezhaiServer::start(ServerBootstrapArgs {
            config_path: config.config_path.clone(),
        })
        .await
        .map_err(Error::from)?;

        let transport = spawn_loopback_transport(server.clone(), TcpTransportConfig::default())
            .await
            .map_err(Error::from)?;

        let factory = Arc::new(TcpBenchmarkClientFactory {
            addr: transport.local_addr(),
            config: TcpTransportConfig::default(),
        });
        let benchmark_result = run_benchmark(&settings, factory).await;

        let transport_shutdown = transport.shutdown().await.map_err(Error::from);
        let server_shutdown = server.shutdown().await.map_err(Error::from);
        let server_wait = server.wait_stopped().await.map_err(Error::from);

        transport_shutdown?;
        server_shutdown?;
        server_wait?;

        let report = benchmark_result?;
        print_report(&report);
        Ok(report)
    })
}

/// Parses benchmark CLI arguments into a `BenchmarkCli` shape.
pub fn parse_cli_args<I, S>(args: I) -> std::result::Result<BenchmarkCli, BenchmarkError>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut cli = BenchmarkCli::default();
    let mut parsed = ParsedCliPercentages::default();
    let mut args = args.into_iter().map(Into::into);

    while let Some(flag) = args.next() {
        match flag.as_str() {
            // `cargo bench` appends this libtest-style flag even when the
            // custom benchmark target disables the default harness.
            "--bench" => {}
            "--config" => cli.config = Some(PathBuf::from(next_value(&mut args, "--config")?)),
            "--clients" => {
                cli.clients = parse_usize(next_value(&mut args, "--clients")?, "--clients")?
            }
            "--populate-clients" => {
                cli.populate_clients = parse_usize(
                    next_value(&mut args, "--populate-clients")?,
                    "--populate-clients",
                )?
            }
            "--populate-keys" => {
                cli.populate_keys =
                    parse_usize(next_value(&mut args, "--populate-keys")?, "--populate-keys")?
            }
            "--duration" => {
                cli.duration_secs = parse_u64(next_value(&mut args, "--duration")?, "--duration")?
            }
            "--preset" => cli.preset = Some(next_value(&mut args, "--preset")?),
            "--get-percent" => {
                parsed.get_percent = Some(parse_u8(
                    next_value(&mut args, "--get-percent")?,
                    "--get-percent",
                )?)
            }
            "--put-percent" => {
                parsed.put_percent = Some(parse_u8(
                    next_value(&mut args, "--put-percent")?,
                    "--put-percent",
                )?)
            }
            "--delete-percent" => {
                parsed.delete_percent = Some(parse_u8(
                    next_value(&mut args, "--delete-percent")?,
                    "--delete-percent",
                )?)
            }
            "--scan-percent" => {
                parsed.scan_percent = Some(parse_u8(
                    next_value(&mut args, "--scan-percent")?,
                    "--scan-percent",
                )?)
            }
            "--value-bytes-min" => {
                cli.value_bytes_min = parse_usize(
                    next_value(&mut args, "--value-bytes-min")?,
                    "--value-bytes-min",
                )?
            }
            "--value-bytes-max" => {
                cli.value_bytes_max = parse_usize(
                    next_value(&mut args, "--value-bytes-max")?,
                    "--value-bytes-max",
                )?
            }
            "--help" | "-h" => {
                return Err(BenchmarkError::new(help_text()));
            }
            _ => {
                return Err(BenchmarkError::new(format!(
                    "unknown benchmark flag `{flag}`\n{}",
                    help_text()
                )));
            }
        }
    }

    cli.explicit_mix = parsed.into_mix()?;
    Ok(cli)
}

/// Resolves parsed CLI values into validated benchmark settings.
pub fn resolve_settings(
    cli: BenchmarkCli,
) -> std::result::Result<BenchmarkSettings, BenchmarkError> {
    let mix = match cli.explicit_mix {
        Some(explicit) => explicit,
        None => {
            let preset_name = cli.preset.as_deref().unwrap_or(DEFAULT_PRESET_NAME);
            preset_mix(preset_name)?
        }
    };
    let settings = BenchmarkSettings {
        config: cli.config,
        clients: cli.clients,
        populate_clients: cli.populate_clients,
        populate_keys: cli.populate_keys,
        duration_secs: cli.duration_secs,
        mix,
        value_bytes_min: cli.value_bytes_min,
        value_bytes_max: cli.value_bytes_max,
    };
    settings.validate()?;
    Ok(settings)
}

/// Resolves the config path, creating a default temporary store when omitted.
pub fn resolve_config_path(settings: &BenchmarkSettings) -> Result<BenchmarkConfigPath> {
    if let Some(path) = &settings.config {
        return Ok(BenchmarkConfigPath {
            config_path: path.clone(),
            _temp_store_guard: None,
        });
    }

    let temp = TempStoreGuard::new()?;
    let store_dir = temp.path.join("store");
    std::fs::create_dir_all(store_dir.join("wal"))?;
    std::fs::create_dir_all(store_dir.join("meta"))?;
    std::fs::create_dir_all(store_dir.join("data"))?;
    let config_path = store_dir.join("config.toml");
    std::fs::write(&config_path, default_config_toml())?;
    Ok(BenchmarkConfigPath {
        config_path,
        _temp_store_guard: Some(temp),
    })
}

/// Builds one scan operation from a deterministic key index.
#[must_use]
pub fn build_scan_workload(key_universe: &KeyUniverse, start_key_index: usize) -> ScanWorkload {
    let bounded_index = start_key_index % key_universe.len();
    ScanWorkload {
        start_key: key_universe.key(bounded_index).to_vec(),
        max_records_per_page: DEFAULT_SCAN_MAX_RECORDS_PER_PAGE,
        max_bytes_per_page: DEFAULT_SCAN_MAX_BYTES_PER_PAGE,
        max_total_rows: DEFAULT_SCAN_MAX_TOTAL_ROWS,
    }
}

/// Computes one percentile summary from sorted nanosecond samples.
pub fn summarize_sorted_samples(
    sorted_samples_ns: &[u64],
) -> std::result::Result<PercentileSummary, BenchmarkError> {
    if sorted_samples_ns.is_empty() {
        return Err(BenchmarkError::new(
            "cannot summarize empty latency samples",
        ));
    }
    Ok(PercentileSummary {
        min: sorted_samples_ns[0],
        p50: percentile_value(sorted_samples_ns, 50, 100),
        p95: percentile_value(sorted_samples_ns, 95, 100),
        p99: percentile_value(sorted_samples_ns, 99, 100),
        p999: percentile_value(sorted_samples_ns, 999, 1000),
        max: sorted_samples_ns[sorted_samples_ns.len() - 1],
    })
}

/// Runs one full benchmark through one client factory.
pub async fn run_benchmark(
    settings: &BenchmarkSettings,
    client_factory: Arc<dyn BenchmarkClientFactory>,
) -> Result<BenchmarkReport> {
    settings.validate().map_err(Error::from)?;
    let key_universe = KeyUniverse::new(settings.populate_keys);
    let selector = OperationSelector::from_mix(settings.mix);

    populate_and_sync(settings, &key_universe, Arc::clone(&client_factory)).await?;

    let started = Instant::now();
    let deadline = started + settings.duration();
    let mut joins = Vec::with_capacity(settings.clients);
    for client_index in 0..settings.clients {
        let client_factory = Arc::clone(&client_factory);
        let key_universe = key_universe.clone();
        let selector = selector.clone();
        let settings = settings.clone();
        joins.push(tokio::spawn(async move {
            run_client_loop(
                client_index,
                settings,
                key_universe,
                selector,
                client_factory,
                deadline,
            )
            .await
        }));
    }

    let mut aggregate_samples = Vec::new();
    let mut per_operation_samples: BTreeMap<BenchOperation, Vec<u64>> = BTreeMap::new();
    for join in joins {
        let client_samples = join.await.map_err(|error| Error::io(error.to_string()))??;
        for (operation, mut latencies) in client_samples {
            aggregate_samples.extend(latencies.iter().copied());
            per_operation_samples
                .entry(operation)
                .or_default()
                .append(&mut latencies);
        }
    }
    let elapsed_ns = started.elapsed().as_nanos() as u64;

    let mut per_operation = BTreeMap::new();
    for (operation, mut samples) in per_operation_samples {
        if samples.is_empty() {
            continue;
        }
        samples.sort_unstable();
        let summary = summarize_sorted_samples(&samples).map_err(Error::from)?;
        let tps = throughput_tps(samples.len(), elapsed_ns);
        per_operation.insert(
            operation,
            ReportRow {
                label: operation.as_str().to_string(),
                count: samples.len(),
                tps,
                latency_ns: summary,
            },
        );
    }

    aggregate_samples.sort_unstable();
    let aggregate_summary = summarize_sorted_samples(&aggregate_samples).map_err(Error::from)?;
    let aggregate_count = aggregate_samples.len();
    let aggregate = ReportRow {
        label: "aggregate".to_string(),
        count: aggregate_count,
        tps: throughput_tps(aggregate_count, elapsed_ns),
        latency_ns: aggregate_summary,
    };
    Ok(BenchmarkReport {
        elapsed_ns,
        per_operation,
        aggregate,
    })
}

/// Prints benchmark report rows to stdout.
pub fn print_report(report: &BenchmarkReport) {
    println!(
        "aggregate tps={} elapsed_ns={}",
        report.aggregate.tps, report.elapsed_ns
    );
    print_row(&report.aggregate);
    for operation in [
        BenchOperation::Get,
        BenchOperation::Put,
        BenchOperation::Delete,
        BenchOperation::Scan,
    ] {
        if let Some(row) = report.per_operation.get(&operation) {
            print_row(row);
        }
    }
}

fn print_row(row: &ReportRow) {
    println!(
        "op={} count={} tps={} min={} p50={} p95={} p99={} p99.9={} max={}",
        row.label,
        row.count,
        row.tps,
        row.latency_ns.min,
        row.latency_ns.p50,
        row.latency_ns.p95,
        row.latency_ns.p99,
        row.latency_ns.p999,
        row.latency_ns.max
    );
}

async fn populate_and_sync(
    settings: &BenchmarkSettings,
    key_universe: &KeyUniverse,
    client_factory: Arc<dyn BenchmarkClientFactory>,
) -> Result<()> {
    let populate_clients = settings.populate_clients.min(key_universe.len());
    let mut joins = Vec::with_capacity(populate_clients);
    for client_index in 0..populate_clients {
        let client_factory = Arc::clone(&client_factory);
        let key_universe = key_universe.clone();
        let settings = settings.clone();
        joins.push(tokio::spawn(async move {
            run_populate_client(
                client_index,
                populate_clients,
                &settings,
                &key_universe,
                client_factory,
            )
            .await
        }));
    }
    for join in joins {
        join.await.map_err(|error| Error::io(error.to_string()))??;
    }

    let mut sync_client = client_factory
        .connect("bench-populate-sync".to_string())
        .await?;
    let sync_response = sync_client
        .call(ExternalRequest {
            client_id: "bench-populate-sync".to_string(),
            request_id: 1,
            method: ExternalMethod::Sync,
            cancel_token: None,
        })
        .await?;
    require_ok_response("populate sync", &sync_response)?;
    Ok(())
}

// Populate writes are partitioned round-robin across client ids so the setup
// phase can overlap durability waits while keeping key ownership stable.
async fn run_populate_client(
    client_index: usize,
    populate_clients: usize,
    settings: &BenchmarkSettings,
    key_universe: &KeyUniverse,
    client_factory: Arc<dyn BenchmarkClientFactory>,
) -> Result<()> {
    let client_id = format!("bench-populate-{client_index:04}");
    let mut client = client_factory.connect(client_id.clone()).await?;
    let mut request_id = 1_u64;
    for key_index in (client_index..key_universe.len()).step_by(populate_clients) {
        let response = client
            .call(ExternalRequest {
                client_id: client_id.clone(),
                request_id,
                method: ExternalMethod::Put {
                    key: key_universe.key(key_index).to_vec(),
                    value: populate_value_for_key(
                        key_index,
                        settings.value_bytes_min,
                        settings.value_bytes_max,
                    ),
                },
                cancel_token: None,
            })
            .await?;
        require_ok_response("populate put", &response)?;
        request_id += 1;
    }
    Ok(())
}

async fn run_client_loop(
    client_index: usize,
    settings: BenchmarkSettings,
    key_universe: KeyUniverse,
    selector: OperationSelector,
    client_factory: Arc<dyn BenchmarkClientFactory>,
    deadline: Instant,
) -> Result<BTreeMap<BenchOperation, Vec<u64>>> {
    let client_id = format!("bench-client-{client_index:04}");
    let mut request_id = 1_u64;
    let mut client = client_factory.connect(client_id.clone()).await?;
    let mut rng = SplitMix64::new(0x6A09_E667_F3BC_C909 ^ client_index as u64);
    let mut latencies: BTreeMap<BenchOperation, Vec<u64>> = BTreeMap::new();

    while Instant::now() < deadline {
        let operation = selector.choose((rng.next_u64() % 100) as u16);
        let started = Instant::now();
        match operation {
            BenchOperation::Get => {
                let key = key_universe.key((rng.next_u64() as usize) % key_universe.len());
                let response = client
                    .call(ExternalRequest {
                        client_id: client_id.clone(),
                        request_id: next_request_id(&mut request_id),
                        method: ExternalMethod::Get { key: key.to_vec() },
                        cancel_token: None,
                    })
                    .await?;
                require_ok_response("get", &response)?;
                if !matches!(response.payload, Some(ExternalResponsePayload::Get(_))) {
                    return Err(Error::corruption(
                        "get response did not include a Get payload",
                    ));
                }
            }
            BenchOperation::Put => {
                let key = key_universe.key((rng.next_u64() as usize) % key_universe.len());
                let value =
                    random_value(&mut rng, settings.value_bytes_min, settings.value_bytes_max);
                let response = client
                    .call(ExternalRequest {
                        client_id: client_id.clone(),
                        request_id: next_request_id(&mut request_id),
                        method: ExternalMethod::Put {
                            key: key.to_vec(),
                            value,
                        },
                        cancel_token: None,
                    })
                    .await?;
                require_ok_response("put", &response)?;
                if !matches!(response.payload, Some(ExternalResponsePayload::Put)) {
                    return Err(Error::corruption(
                        "put response did not include a Put payload",
                    ));
                }
            }
            BenchOperation::Delete => {
                let key = key_universe.key((rng.next_u64() as usize) % key_universe.len());
                let response = client
                    .call(ExternalRequest {
                        client_id: client_id.clone(),
                        request_id: next_request_id(&mut request_id),
                        method: ExternalMethod::Delete { key: key.to_vec() },
                        cancel_token: None,
                    })
                    .await?;
                require_ok_response("delete", &response)?;
                if !matches!(response.payload, Some(ExternalResponsePayload::Delete)) {
                    return Err(Error::corruption(
                        "delete response did not include a Delete payload",
                    ));
                }
            }
            BenchOperation::Scan => {
                let scan = build_scan_workload(
                    &key_universe,
                    (rng.next_u64() as usize) % key_universe.len(),
                );
                run_scan_operation(&mut *client, &client_id, &mut request_id, &scan).await?;
            }
        }
        let elapsed_ns = started.elapsed().as_nanos() as u64;
        latencies.entry(operation).or_default().push(elapsed_ns);
    }

    Ok(latencies)
}

async fn run_scan_operation(
    client: &mut dyn BenchmarkClient,
    client_id: &str,
    request_id: &mut u64,
    scan: &ScanWorkload,
) -> Result<()> {
    let start = client
        .call(ExternalRequest {
            client_id: client_id.to_string(),
            request_id: next_request_id(request_id),
            method: ExternalMethod::ScanStart {
                start_bound: Bound::Finite(scan.start_key.clone()),
                end_bound: Bound::PosInf,
                max_records_per_page: scan.max_records_per_page,
                max_bytes_per_page: scan.max_bytes_per_page,
            },
            cancel_token: None,
        })
        .await?;
    require_ok_response("scan start", &start)?;
    let scan_id = match start.payload {
        Some(ExternalResponsePayload::ScanStart(payload)) => payload.scan_id,
        _ => {
            return Err(Error::corruption(
                "scan start response did not include a ScanStart payload",
            ));
        }
    };

    let mut rows_read = 0usize;
    loop {
        let fetch = client
            .call(ExternalRequest {
                client_id: client_id.to_string(),
                request_id: next_request_id(request_id),
                method: ExternalMethod::ScanFetchNext { scan_id },
                cancel_token: None,
            })
            .await?;
        require_ok_response("scan fetch next", &fetch)?;
        let payload = match fetch.payload {
            Some(ExternalResponsePayload::ScanFetchNext(payload)) => payload,
            _ => {
                return Err(Error::corruption(
                    "scan fetch response did not include a ScanFetchNext payload",
                ));
            }
        };
        rows_read = rows_read.saturating_add(payload.rows.len());
        if payload.eof || rows_read >= scan.max_total_rows {
            break;
        }
    }
    Ok(())
}

fn require_ok_response(context: &str, response: &ExternalResponse) -> Result<()> {
    if response.status.code != crate::sevai::StatusCode::Ok {
        return Err(Error::io(format!(
            "{context} returned non-ok status {:?}: {:?}",
            response.status.code, response.status.message
        )));
    }
    Ok(())
}

fn percentile_value(sorted_samples: &[u64], numerator: usize, denominator: usize) -> u64 {
    let last_index = sorted_samples.len() - 1;
    let scaled = last_index.saturating_mul(numerator);
    let index = scaled.div_ceil(denominator);
    sorted_samples[index]
}

fn throughput_tps(count: usize, elapsed_ns: u64) -> u64 {
    if elapsed_ns == 0 {
        return count as u64;
    }
    ((count as u128 * 1_000_000_000_u128) / elapsed_ns as u128) as u64
}

// Populate values are derived from the key index so setup stays repeatable
// even when the populate work is spread across a different number of clients.
fn populate_value_for_key(key_index: usize, min_bytes: usize, max_bytes: usize) -> Vec<u8> {
    let mut rng = SplitMix64::new(0x0014_0A1E_D00D_BEEF ^ key_index as u64);
    random_value(&mut rng, min_bytes, max_bytes)
}

fn next_request_id(next: &mut u64) -> u64 {
    let current = *next;
    *next = next.saturating_add(1);
    current
}

fn parse_u8(raw: String, flag: &str) -> std::result::Result<u8, BenchmarkError> {
    raw.parse::<u8>()
        .map_err(|_| BenchmarkError::new(format!("{flag} must be an integer in 0..=255")))
}

fn parse_usize(raw: String, flag: &str) -> std::result::Result<usize, BenchmarkError> {
    raw.parse::<usize>()
        .map_err(|_| BenchmarkError::new(format!("{flag} must be a non-negative integer")))
}

fn parse_u64(raw: String, flag: &str) -> std::result::Result<u64, BenchmarkError> {
    raw.parse::<u64>()
        .map_err(|_| BenchmarkError::new(format!("{flag} must be a non-negative integer")))
}

fn next_value<I>(args: &mut I, flag: &str) -> std::result::Result<String, BenchmarkError>
where
    I: Iterator<Item = String>,
{
    args.next()
        .ok_or_else(|| BenchmarkError::new(format!("missing value for {flag}")))
}

fn preset_mix(name: &str) -> std::result::Result<OperationMix, BenchmarkError> {
    let mix = match name {
        "balanced" => OperationMix {
            get_percent: 60,
            put_percent: 25,
            delete_percent: 10,
            scan_percent: 5,
        },
        "read-heavy" => OperationMix {
            get_percent: 85,
            put_percent: 10,
            delete_percent: 3,
            scan_percent: 2,
        },
        "write-heavy" => OperationMix {
            get_percent: 35,
            put_percent: 50,
            delete_percent: 10,
            scan_percent: 5,
        },
        _ => {
            return Err(BenchmarkError::new(format!(
                "unknown preset `{name}`. expected one of: balanced, read-heavy, write-heavy"
            )));
        }
    };
    mix.validate()?;
    Ok(mix)
}

fn help_text() -> &'static str {
    "usage:
  --config <path> (optional)
  --clients <n>
  --populate-clients <n>
  --populate-keys <n>
  --duration <secs>
  --preset <name> (optional)
  --get-percent <n>
  --put-percent <n>
  --delete-percent <n>
  --scan-percent <n>
  --value-bytes-min <n>
  --value-bytes-max <n>"
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ParsedCliPercentages {
    get_percent: Option<u8>,
    put_percent: Option<u8>,
    delete_percent: Option<u8>,
    scan_percent: Option<u8>,
}

impl ParsedCliPercentages {
    fn into_mix(self) -> std::result::Result<Option<OperationMix>, BenchmarkError> {
        let any_set = self.get_percent.is_some()
            || self.put_percent.is_some()
            || self.delete_percent.is_some()
            || self.scan_percent.is_some();
        if !any_set {
            return Ok(None);
        }
        let mix = OperationMix {
            get_percent: self.get_percent.ok_or_else(|| {
                BenchmarkError::new("--get-percent is required when setting explicit percentages")
            })?,
            put_percent: self.put_percent.ok_or_else(|| {
                BenchmarkError::new("--put-percent is required when setting explicit percentages")
            })?,
            delete_percent: self.delete_percent.ok_or_else(|| {
                BenchmarkError::new(
                    "--delete-percent is required when setting explicit percentages",
                )
            })?,
            scan_percent: self.scan_percent.ok_or_else(|| {
                BenchmarkError::new("--scan-percent is required when setting explicit percentages")
            })?,
        };
        mix.validate()?;
        Ok(Some(mix))
    }
}

#[derive(Debug)]
struct TempStoreGuard {
    path: PathBuf,
}

impl TempStoreGuard {
    fn new() -> Result<Self> {
        let mut path = std::env::temp_dir();
        let mut rng = SplitMix64::new(0xA409_3822_299F_31D0 ^ now_nanos());
        let suffix = rng.next_u64();
        path.push(format!("kalanjiyam-bench-{suffix:016x}"));
        std::fs::create_dir_all(&path)?;
        Ok(Self { path })
    }
}

impl Drop for TempStoreGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn default_config_toml() -> String {
    format!(
        "[engine]
sync_mode = \"per_write\"
page_size_bytes = {DEFAULT_PAGE_SIZE_BYTES}

[wal]
segment_bytes = {DEFAULT_WAL_SEGMENT_BYTES}
group_commit_bytes = {DEFAULT_GROUP_COMMIT_BYTES}
group_commit_max_delay_ms = {DEFAULT_GROUP_COMMIT_MAX_DELAY_MS}

[lsm]
memtable_flush_bytes = {DEFAULT_MEMTABLE_FLUSH_BYTES}
base_level_bytes = {DEFAULT_BASE_LEVEL_BYTES}
level_fanout = {DEFAULT_LEVEL_FANOUT}
l0_file_threshold = {DEFAULT_L0_FILE_THRESHOLD}
max_levels = {DEFAULT_MAX_LEVELS}

[maintenance]
max_external_requests = {DEFAULT_MAX_EXTERNAL_REQUESTS}
max_scan_fetch_queue = {DEFAULT_MAX_SCAN_FETCH_QUEUE}
max_scan_sessions = {DEFAULT_MAX_SCAN_SESSIONS}
max_worker_tasks = {DEFAULT_MAX_WORKER_TASKS}
max_waiting_wal_syncs = {DEFAULT_MAX_WAITING_WAL_SYNCS}
scan_expiry_ms = {DEFAULT_SCAN_EXPIRY_MS}
retry_delay_ms = {DEFAULT_RETRY_DELAY_MS}
worker_threads = {DEFAULT_WORKER_THREADS}
gc_interval_secs = {DEFAULT_GC_INTERVAL_SECS}
checkpoint_interval_secs = {DEFAULT_CHECKPOINT_INTERVAL_SECS}
"
    )
}

fn random_value(rng: &mut SplitMix64, min_bytes: usize, max_bytes: usize) -> Vec<u8> {
    let len = if min_bytes == max_bytes {
        min_bytes
    } else {
        let span = max_bytes - min_bytes + 1;
        min_bytes + (rng.next_u64() as usize % span)
    };
    let mut value = vec![0_u8; len];
    for byte in &mut value {
        *byte = (rng.next_u64() & 0xFF) as u8;
    }
    value
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos() as u64)
}

/// Small deterministic RNG for repeatable benchmark behavior and tests.
#[derive(Clone, Debug, Eq, PartialEq)]
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// Runs benchmark setup, workload execution, and report printing from process args.
pub async fn run_from_cli_args<I, S>(
    args: I,
    client_factory: Arc<dyn BenchmarkClientFactory>,
) -> Result<BenchmarkReport>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let parsed = parse_cli_args(args).map_err(Error::from)?;
    let settings = resolve_settings(parsed).map_err(Error::from)?;
    let _config = resolve_config_path(&settings)?;
    let report = run_benchmark(&settings, client_factory).await?;
    print_report(&report);
    Ok(report)
}

/// Returns whether one config path exists and appears to be a TOML file path.
#[must_use]
pub fn looks_like_config_path(path: &Path) -> bool {
    path.extension().is_some_and(|ext| ext == "toml")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use crate::pezhai::types::SyncResponse;

    fn run_live_benchmark(args: &[&str]) -> BenchmarkReport {
        run_sevai_tcp_benchmark(args.iter().copied())
            .expect("live benchmark smoke test should succeed")
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum RecordedMethod {
        Put { key: Vec<u8>, value: Vec<u8> },
        Sync,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct RecordedRequest {
        client_id: String,
        request_id: u64,
        method: RecordedMethod,
    }

    #[derive(Clone, Debug, Default)]
    struct RecordingFactory {
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
    }

    impl RecordingFactory {
        fn recorded_requests(&self) -> Vec<RecordedRequest> {
            self.requests
                .lock()
                .expect("recording mutex should not poison")
                .clone()
        }
    }

    impl BenchmarkClientFactory for RecordingFactory {
        fn connect<'a>(
            &'a self,
            _client_id: String,
        ) -> BoxFuture<'a, Result<Box<dyn BenchmarkClient>>> {
            let requests = Arc::clone(&self.requests);
            Box::pin(async move {
                Ok(Box::new(RecordingClient { requests }) as Box<dyn BenchmarkClient>)
            })
        }
    }

    #[derive(Debug)]
    struct RecordingClient {
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
    }

    impl BenchmarkClient for RecordingClient {
        fn call<'a>(
            &'a mut self,
            request: ExternalRequest,
        ) -> BoxFuture<'a, Result<ExternalResponse>> {
            let recorded = match &request.method {
                ExternalMethod::Put { key, value } => RecordedRequest {
                    client_id: request.client_id.clone(),
                    request_id: request.request_id,
                    method: RecordedMethod::Put {
                        key: key.clone(),
                        value: value.clone(),
                    },
                },
                ExternalMethod::Sync => RecordedRequest {
                    client_id: request.client_id.clone(),
                    request_id: request.request_id,
                    method: RecordedMethod::Sync,
                },
                method => panic!("unexpected method in populate test: {method:?}"),
            };
            self.requests
                .lock()
                .expect("recording mutex should not poison")
                .push(recorded);

            let payload = match request.method {
                ExternalMethod::Put { .. } => Some(ExternalResponsePayload::Put),
                ExternalMethod::Sync => {
                    Some(ExternalResponsePayload::Sync(SyncResponse::default()))
                }
                _ => None,
            };
            Box::pin(async move {
                Ok(ExternalResponse {
                    client_id: request.client_id,
                    request_id: request.request_id,
                    status: crate::sevai::Status::ok(),
                    payload,
                })
            })
        }
    }

    #[test]
    fn cli_parsing_collects_all_flags() {
        let parsed = parse_cli_args([
            "--bench",
            "--config",
            "/tmp/store/config.toml",
            "--clients",
            "8",
            "--populate-clients",
            "3",
            "--populate-keys",
            "5000",
            "--duration",
            "17",
            "--preset",
            "read-heavy",
            "--value-bytes-min",
            "32",
            "--value-bytes-max",
            "4096",
        ])
        .expect("cli should parse benchmark flags");

        assert_eq!(parsed.config, Some(PathBuf::from("/tmp/store/config.toml")));
        assert_eq!(parsed.clients, 8);
        assert_eq!(parsed.populate_clients, 3);
        assert_eq!(parsed.populate_keys, 5000);
        assert_eq!(parsed.duration_secs, 17);
        assert_eq!(parsed.preset.as_deref(), Some("read-heavy"));
        assert_eq!(parsed.value_bytes_min, 32);
        assert_eq!(parsed.value_bytes_max, 4096);
        assert_eq!(parsed.explicit_mix, None);
    }

    #[test]
    fn cargo_bench_passthrough_flag_is_ignored() {
        let parsed = parse_cli_args([
            "--clients",
            "2",
            "--populate-keys",
            "10",
            "--duration",
            "1",
            "--preset",
            "balanced",
            "--bench",
        ])
        .expect("cargo bench passthrough flag should be ignored");
        assert_eq!(parsed.clients, 2);
        assert_eq!(parsed.populate_clients, DEFAULT_POPULATE_CLIENTS);
        assert_eq!(parsed.populate_keys, 10);
        assert_eq!(parsed.duration_secs, 1);
        assert_eq!(parsed.preset.as_deref(), Some("balanced"));
    }

    #[test]
    fn help_text_lists_populate_clients_flag() {
        let error = parse_cli_args(["--help"]).expect_err("help should short-circuit with usage");
        assert!(error.to_string().contains("--populate-clients"));
    }

    #[test]
    fn explicit_percentages_override_preset() {
        let parsed = parse_cli_args([
            "--preset",
            "read-heavy",
            "--get-percent",
            "10",
            "--put-percent",
            "20",
            "--delete-percent",
            "30",
            "--scan-percent",
            "40",
        ])
        .expect("cli should parse explicit percentages");
        let settings =
            resolve_settings(parsed).expect("explicit percentages should override preset");
        assert_eq!(
            settings.mix,
            OperationMix {
                get_percent: 10,
                put_percent: 20,
                delete_percent: 30,
                scan_percent: 40,
            }
        );
    }

    #[test]
    fn partial_explicit_percentages_are_rejected() {
        let result = parse_cli_args(["--get-percent", "70", "--put-percent", "20"]);
        assert!(result.is_err());
    }

    #[test]
    fn default_preset_is_balanced_when_mix_is_unspecified() {
        let settings = resolve_settings(BenchmarkCli::default())
            .expect("default cli should resolve with balanced mix");
        assert_eq!(
            settings.mix,
            OperationMix {
                get_percent: 60,
                put_percent: 25,
                delete_percent: 10,
                scan_percent: 5,
            }
        );
    }

    #[test]
    fn percentage_validation_rejects_invalid_totals() {
        let result = resolve_settings(BenchmarkCli {
            explicit_mix: Some(OperationMix {
                get_percent: 50,
                put_percent: 30,
                delete_percent: 10,
                scan_percent: 20,
            }),
            ..BenchmarkCli::default()
        });
        assert!(result.is_err());
    }

    #[test]
    fn populate_client_validation_rejects_zero() {
        let result = resolve_settings(BenchmarkCli {
            populate_clients: 0,
            ..BenchmarkCli::default()
        });
        assert!(result.is_err());
    }

    #[test]
    fn value_range_validation_rejects_min_greater_than_max() {
        let result = resolve_settings(BenchmarkCli {
            value_bytes_min: 4096,
            value_bytes_max: 1024,
            ..BenchmarkCli::default()
        });
        assert!(result.is_err());
    }

    #[test]
    fn percentile_summary_uses_sorted_samples_directly() {
        let samples = vec![10_u64, 20, 30, 40, 50, 60, 70, 80, 90, 100];
        let summary = summarize_sorted_samples(&samples).expect("sorted samples should summarize");
        assert_eq!(summary.min, 10);
        assert_eq!(summary.p50, 60);
        assert_eq!(summary.p95, 100);
        assert_eq!(summary.p99, 100);
        assert_eq!(summary.p999, 100);
        assert_eq!(summary.max, 100);
    }

    #[test]
    fn scan_workload_assembly_is_deterministic() {
        let universe = KeyUniverse::new(3);
        let scan = build_scan_workload(&universe, 7);
        assert_eq!(scan.start_key, universe.key(1).to_vec());
        assert_eq!(scan.max_records_per_page, DEFAULT_SCAN_MAX_RECORDS_PER_PAGE);
        assert_eq!(scan.max_bytes_per_page, DEFAULT_SCAN_MAX_BYTES_PER_PAGE);
        assert_eq!(scan.max_total_rows, DEFAULT_SCAN_MAX_TOTAL_ROWS);
    }

    #[test]
    fn resolve_config_path_builds_default_temp_store_when_config_is_missing() {
        let settings =
            resolve_settings(BenchmarkCli::default()).expect("default settings should resolve");
        let resolved =
            resolve_config_path(&settings).expect("missing config should create temp store");
        assert!(resolved.config_path.exists());
        assert!(looks_like_config_path(&resolved.config_path));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn populate_and_sync_uses_requested_populate_clients() {
        let settings = BenchmarkSettings {
            config: None,
            clients: 1,
            populate_clients: 3,
            populate_keys: 7,
            duration_secs: 1,
            mix: OperationMix {
                get_percent: 100,
                put_percent: 0,
                delete_percent: 0,
                scan_percent: 0,
            },
            value_bytes_min: 4,
            value_bytes_max: 4,
        };
        let key_universe = KeyUniverse::new(settings.populate_keys);
        let factory = Arc::new(RecordingFactory::default());

        populate_and_sync(&settings, &key_universe, factory.clone())
            .await
            .expect("populate should succeed");

        let recorded = factory.recorded_requests();
        let put_requests: Vec<_> = recorded
            .iter()
            .filter_map(|request| match &request.method {
                RecordedMethod::Put { key, value } => Some((request.client_id.clone(), key, value)),
                RecordedMethod::Sync => None,
            })
            .collect();
        assert_eq!(put_requests.len(), settings.populate_keys);
        assert_eq!(
            put_requests
                .iter()
                .map(|(client_id, _, _)| client_id.clone())
                .collect::<std::collections::BTreeSet<_>>(),
            std::collections::BTreeSet::from([
                "bench-populate-0000".to_string(),
                "bench-populate-0001".to_string(),
                "bench-populate-0002".to_string(),
            ])
        );
        for key_index in 0..settings.populate_keys {
            let expected_key = key_universe.key(key_index).to_vec();
            let expected_value = populate_value_for_key(
                key_index,
                settings.value_bytes_min,
                settings.value_bytes_max,
            );
            assert!(put_requests.iter().any(|(_, key, value)| {
                key.as_slice() == expected_key.as_slice()
                    && value.as_slice() == expected_value.as_slice()
            }));
        }
        assert_eq!(
            recorded
                .iter()
                .filter(|request| request.method == RecordedMethod::Sync)
                .count(),
            1
        );
        assert!(recorded.iter().any(|request| {
            request.client_id == "bench-populate-sync"
                && request.request_id == 1
                && request.method == RecordedMethod::Sync
        }));
    }

    #[test]
    fn live_benchmark_get_only_smoke_test() {
        let report = run_live_benchmark(&[
            "--clients",
            "1",
            "--populate-keys",
            "16",
            "--duration",
            "1",
            "--get-percent",
            "100",
            "--put-percent",
            "0",
            "--delete-percent",
            "0",
            "--scan-percent",
            "0",
            "--value-bytes-min",
            "4",
            "--value-bytes-max",
            "4",
        ]);
        assert!(report.aggregate.count > 0);
        assert!(report.per_operation.contains_key(&BenchOperation::Get));
    }

    #[test]
    fn live_benchmark_put_only_smoke_test() {
        let report = run_live_benchmark(&[
            "--clients",
            "1",
            "--populate-keys",
            "16",
            "--duration",
            "1",
            "--get-percent",
            "0",
            "--put-percent",
            "100",
            "--delete-percent",
            "0",
            "--scan-percent",
            "0",
            "--value-bytes-min",
            "4",
            "--value-bytes-max",
            "4",
        ]);
        assert!(report.aggregate.count > 0);
        assert!(report.per_operation.contains_key(&BenchOperation::Put));
    }

    #[test]
    fn live_benchmark_delete_only_smoke_test() {
        let report = run_live_benchmark(&[
            "--clients",
            "1",
            "--populate-keys",
            "16",
            "--duration",
            "1",
            "--get-percent",
            "0",
            "--put-percent",
            "0",
            "--delete-percent",
            "100",
            "--scan-percent",
            "0",
            "--value-bytes-min",
            "4",
            "--value-bytes-max",
            "4",
        ]);
        assert!(report.aggregate.count > 0);
        assert!(report.per_operation.contains_key(&BenchOperation::Delete));
    }

    #[test]
    fn live_benchmark_scan_only_smoke_test() {
        let report = run_live_benchmark(&[
            "--clients",
            "1",
            "--populate-keys",
            "16",
            "--duration",
            "1",
            "--get-percent",
            "0",
            "--put-percent",
            "0",
            "--delete-percent",
            "0",
            "--scan-percent",
            "100",
            "--value-bytes-min",
            "4",
            "--value-bytes-max",
            "4",
        ]);
        assert!(report.aggregate.count > 0);
        assert!(report.per_operation.contains_key(&BenchOperation::Scan));
    }
}
