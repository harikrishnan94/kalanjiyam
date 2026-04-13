//! Shared parsing and semantic validation for the repository's `config.toml` schema.

use std::fmt::{self, Display, Formatter};
use std::fs;
use std::net::SocketAddr;
use std::path::Path;

use serde::Deserialize;

use crate::pathivu::MAX_SINGLE_WAL_RECORD_BYTES;

const DEFAULT_PAGE_SIZE_BYTES: u32 = 4096;
const DEFAULT_SEGMENT_BYTES: u64 = 1_073_741_824;
const DEFAULT_GROUP_COMMIT_BYTES: u64 = 65_536;
const DEFAULT_GROUP_COMMIT_MAX_DELAY_MS: u64 = 50;
const DEFAULT_MEMTABLE_FLUSH_BYTES: u64 = 67_108_864;
const DEFAULT_BASE_LEVEL_BYTES: u64 = 268_435_456;
const DEFAULT_LEVEL_FANOUT: u32 = 10;
const DEFAULT_L0_FILE_THRESHOLD: u32 = 8;
const DEFAULT_MAX_LEVELS: u32 = 7;
const DEFAULT_MAX_PENDING_REQUESTS: u32 = 1024;
const DEFAULT_MAX_WORKER_TASKS: u32 = 256;
const DEFAULT_MAX_SCAN_SESSIONS: u32 = 128;
const DEFAULT_MAX_IN_FLIGHT_SCAN_TASKS: u32 = 64;
const DEFAULT_MAX_SCAN_FETCH_QUEUE_PER_SESSION: u32 = 64;
const DEFAULT_MAX_WAITING_DURABILITY_WAITERS: u32 = 1024;
const DEFAULT_SCAN_IDLE_TIMEOUT_MS: u64 = 300_000;

/// Loads the runtime config from disk and applies the schema defaults.
pub(crate) fn load_runtime_config(path: &Path) -> Result<RuntimeConfig, ConfigError> {
    let config_text = fs::read_to_string(path).map_err(ConfigError::Io)?;
    parse_runtime_config(&config_text)
}

/// Parses the runtime config from TOML text and validates the semantic schema.
pub(crate) fn parse_runtime_config(text: &str) -> Result<RuntimeConfig, ConfigError> {
    let raw: RawRuntimeConfig = toml::from_str(text).map_err(ConfigError::Parse)?;
    raw.normalize()
}

/// Errors raised while parsing or validating the shared TOML config.
#[derive(Debug)]
pub(crate) enum ConfigError {
    Io(std::io::Error),
    Parse(toml::de::Error),
    Invalid(String),
}

impl Display for ConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "failed to read config: {error}"),
            Self::Parse(error) => write!(formatter, "failed to parse config: {error}"),
            Self::Invalid(message) => write!(formatter, "invalid config: {message}"),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Parse(error) => Some(error),
            Self::Invalid(_) => None,
        }
    }
}

/// Shared config parsed by both the library and the binary bootstrap path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RuntimeConfig {
    pub(crate) engine: EngineConfig,
    pub(crate) wal: WalConfig,
    pub(crate) lsm: LsmConfig,
    pub(crate) maintenance: MaintenanceConfig,
    pub(crate) server_limits: ServerLimitsConfig,
    pub(crate) sevai: SevaiConfig,
}

impl RuntimeConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        validate_page_size_bytes(self.engine.page_size_bytes)?;
        validate_segment_bytes(self.wal.segment_bytes)?;
        validate_positive_u64(self.wal.group_commit_bytes, "wal.group_commit_bytes")?;
        if self.wal.group_commit_bytes >= self.wal.segment_bytes {
            return Err(ConfigError::Invalid(
                "wal.group_commit_bytes must be smaller than wal.segment_bytes".into(),
            ));
        }
        validate_positive_u64(
            self.wal.group_commit_max_delay_ms,
            "wal.group_commit_max_delay_ms",
        )?;

        let min_memtable_flush_bytes = u64::from(self.engine.page_size_bytes) * 4;
        if self.lsm.memtable_flush_bytes < min_memtable_flush_bytes {
            return Err(ConfigError::Invalid(format!(
                "lsm.memtable_flush_bytes must be at least {min_memtable_flush_bytes}"
            )));
        }
        if self.lsm.base_level_bytes < self.lsm.memtable_flush_bytes {
            return Err(ConfigError::Invalid(
                "lsm.base_level_bytes must be at least lsm.memtable_flush_bytes".into(),
            ));
        }
        if !(2..=32).contains(&self.lsm.level_fanout) {
            return Err(ConfigError::Invalid(
                "lsm.level_fanout must be in the range 2..=32".into(),
            ));
        }
        if self.lsm.l0_file_threshold < 2 {
            return Err(ConfigError::Invalid(
                "lsm.l0_file_threshold must be at least 2".into(),
            ));
        }
        if !(4..=16).contains(&self.lsm.max_levels) {
            return Err(ConfigError::Invalid(
                "lsm.max_levels must be in the range 4..=16".into(),
            ));
        }

        validate_positive_u64(
            self.maintenance.checkpoint_after_wal_bytes,
            "maintenance.checkpoint_after_wal_bytes",
        )?;
        validate_positive_u64(
            self.maintenance.logical_split_bytes,
            "maintenance.logical_split_bytes",
        )?;
        validate_positive_u64(
            self.maintenance.logical_merge_bytes,
            "maintenance.logical_merge_bytes",
        )?;
        if self.maintenance.logical_merge_bytes >= self.maintenance.logical_split_bytes {
            return Err(ConfigError::Invalid(
                "maintenance.logical_merge_bytes must be smaller than \
                 maintenance.logical_split_bytes"
                    .into(),
            ));
        }
        validate_positive_u32(
            self.maintenance.max_concurrent_flushes,
            "maintenance.max_concurrent_flushes",
        )?;
        validate_positive_u32(
            self.maintenance.max_concurrent_compactions,
            "maintenance.max_concurrent_compactions",
        )?;

        validate_positive_u32(
            self.server_limits.worker_parallelism,
            "server_limits.worker_parallelism",
        )?;
        validate_positive_u32(
            self.server_limits.max_pending_requests,
            "server_limits.max_pending_requests",
        )?;
        validate_positive_u32(
            self.server_limits.max_worker_tasks,
            "server_limits.max_worker_tasks",
        )?;
        validate_positive_u32(
            self.server_limits.max_scan_sessions,
            "server_limits.max_scan_sessions",
        )?;
        validate_positive_u32(
            self.server_limits.max_in_flight_scan_tasks,
            "server_limits.max_in_flight_scan_tasks",
        )?;
        validate_positive_u32(
            self.server_limits.max_scan_fetch_queue_per_session,
            "server_limits.max_scan_fetch_queue_per_session",
        )?;
        validate_positive_u32(
            self.server_limits.max_waiting_durability_waiters,
            "server_limits.max_waiting_durability_waiters",
        )?;
        validate_positive_u64(
            self.server_limits.scan_idle_timeout_ms,
            "server_limits.scan_idle_timeout_ms",
        )?;

        Ok(())
    }
}

/// Engine-facing settings that remain forward-compatible with the Pezhai spec.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub(crate) struct EngineConfig {
    #[serde(default)]
    pub(crate) sync_mode: SyncMode,
    #[serde(default = "default_page_size_bytes")]
    pub(crate) page_size_bytes: u32,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            sync_mode: SyncMode::default(),
            page_size_bytes: default_page_size_bytes(),
        }
    }
}

/// WAL-facing settings preserved so the file shape stays compatible over time.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub(crate) struct WalConfig {
    #[serde(default = "default_segment_bytes")]
    pub(crate) segment_bytes: u64,
    #[serde(default = "default_group_commit_bytes")]
    pub(crate) group_commit_bytes: u64,
    #[serde(default = "default_group_commit_max_delay_ms")]
    pub(crate) group_commit_max_delay_ms: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            segment_bytes: default_segment_bytes(),
            group_commit_bytes: default_group_commit_bytes(),
            group_commit_max_delay_ms: default_group_commit_max_delay_ms(),
        }
    }
}

/// LSM-facing settings preserved for future durable-engine wiring.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub(crate) struct LsmConfig {
    #[serde(default = "default_memtable_flush_bytes")]
    pub(crate) memtable_flush_bytes: u64,
    #[serde(default = "default_base_level_bytes")]
    pub(crate) base_level_bytes: u64,
    #[serde(default = "default_level_fanout")]
    pub(crate) level_fanout: u32,
    #[serde(default = "default_l0_file_threshold")]
    pub(crate) l0_file_threshold: u32,
    #[serde(default = "default_max_levels")]
    pub(crate) max_levels: u32,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            memtable_flush_bytes: default_memtable_flush_bytes(),
            base_level_bytes: default_base_level_bytes(),
            level_fanout: default_level_fanout(),
            l0_file_threshold: default_l0_file_threshold(),
            max_levels: default_max_levels(),
        }
    }
}

/// Maintenance limits and thresholds used by future background work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MaintenanceConfig {
    pub(crate) checkpoint_after_wal_bytes: u64,
    pub(crate) logical_split_bytes: u64,
    pub(crate) logical_merge_bytes: u64,
    pub(crate) max_concurrent_flushes: u32,
    pub(crate) max_concurrent_compactions: u32,
}

/// Admission limits and worker budgets shared by the server runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ServerLimitsConfig {
    pub(crate) worker_parallelism: u32,
    pub(crate) max_pending_requests: u32,
    pub(crate) max_worker_tasks: u32,
    pub(crate) max_scan_sessions: u32,
    pub(crate) max_in_flight_scan_tasks: u32,
    pub(crate) max_scan_fetch_queue_per_session: u32,
    pub(crate) max_waiting_durability_waiters: u32,
    pub(crate) scan_idle_timeout_ms: u64,
}

/// TCP adapter settings required by the `pezhai::sevai` runtime.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub(crate) struct SevaiConfig {
    pub(crate) listen_addr: SocketAddr,
}

/// Sync modes preserved from the Pezhai engine config schema.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SyncMode {
    #[default]
    PerWrite,
    Manual,
}

#[derive(Debug, Deserialize)]
struct RawRuntimeConfig {
    #[serde(default)]
    engine: EngineConfig,
    #[serde(default)]
    wal: WalConfig,
    #[serde(default)]
    lsm: LsmConfig,
    #[serde(default)]
    maintenance: RawMaintenanceConfig,
    #[serde(default)]
    server_limits: RawServerLimitsConfig,
    sevai: Option<SevaiConfig>,
}

impl RawRuntimeConfig {
    fn normalize(self) -> Result<RuntimeConfig, ConfigError> {
        let sevai = self
            .sevai
            .ok_or_else(|| ConfigError::Invalid("missing required [sevai] table".into()))?;
        let maintenance = MaintenanceConfig {
            checkpoint_after_wal_bytes: self
                .maintenance
                .checkpoint_after_wal_bytes
                .unwrap_or(self.wal.segment_bytes),
            logical_split_bytes: self
                .maintenance
                .logical_split_bytes
                .unwrap_or(self.lsm.base_level_bytes.saturating_mul(2)),
            logical_merge_bytes: self
                .maintenance
                .logical_merge_bytes
                .unwrap_or(self.lsm.base_level_bytes / 2),
            max_concurrent_flushes: self.maintenance.max_concurrent_flushes.unwrap_or(1),
            max_concurrent_compactions: self.maintenance.max_concurrent_compactions.unwrap_or(1),
        };
        let server_limits = ServerLimitsConfig {
            worker_parallelism: self
                .server_limits
                .worker_parallelism
                .unwrap_or_else(default_worker_parallelism),
            max_pending_requests: self
                .server_limits
                .max_pending_requests
                .unwrap_or(DEFAULT_MAX_PENDING_REQUESTS),
            max_worker_tasks: self
                .server_limits
                .max_worker_tasks
                .unwrap_or(DEFAULT_MAX_WORKER_TASKS),
            max_scan_sessions: self
                .server_limits
                .max_scan_sessions
                .unwrap_or(DEFAULT_MAX_SCAN_SESSIONS),
            max_in_flight_scan_tasks: self
                .server_limits
                .max_in_flight_scan_tasks
                .unwrap_or(DEFAULT_MAX_IN_FLIGHT_SCAN_TASKS),
            max_scan_fetch_queue_per_session: self
                .server_limits
                .max_scan_fetch_queue_per_session
                .unwrap_or(DEFAULT_MAX_SCAN_FETCH_QUEUE_PER_SESSION),
            max_waiting_durability_waiters: self
                .server_limits
                .max_waiting_durability_waiters
                .unwrap_or(DEFAULT_MAX_WAITING_DURABILITY_WAITERS),
            scan_idle_timeout_ms: self
                .server_limits
                .scan_idle_timeout_ms
                .unwrap_or(DEFAULT_SCAN_IDLE_TIMEOUT_MS),
        };
        let config = RuntimeConfig {
            engine: self.engine,
            wal: self.wal,
            lsm: self.lsm,
            maintenance,
            server_limits,
            sevai,
        };
        config.validate()?;
        Ok(config)
    }
}

#[derive(Debug, Default, Deserialize)]
struct RawMaintenanceConfig {
    checkpoint_after_wal_bytes: Option<u64>,
    logical_split_bytes: Option<u64>,
    logical_merge_bytes: Option<u64>,
    max_concurrent_flushes: Option<u32>,
    max_concurrent_compactions: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
struct RawServerLimitsConfig {
    worker_parallelism: Option<u32>,
    max_pending_requests: Option<u32>,
    max_worker_tasks: Option<u32>,
    max_scan_sessions: Option<u32>,
    max_in_flight_scan_tasks: Option<u32>,
    max_scan_fetch_queue_per_session: Option<u32>,
    max_waiting_durability_waiters: Option<u32>,
    scan_idle_timeout_ms: Option<u64>,
}

const fn default_page_size_bytes() -> u32 {
    DEFAULT_PAGE_SIZE_BYTES
}

const fn default_segment_bytes() -> u64 {
    DEFAULT_SEGMENT_BYTES
}

const fn default_group_commit_bytes() -> u64 {
    DEFAULT_GROUP_COMMIT_BYTES
}

const fn default_group_commit_max_delay_ms() -> u64 {
    DEFAULT_GROUP_COMMIT_MAX_DELAY_MS
}

const fn default_memtable_flush_bytes() -> u64 {
    DEFAULT_MEMTABLE_FLUSH_BYTES
}

const fn default_base_level_bytes() -> u64 {
    DEFAULT_BASE_LEVEL_BYTES
}

const fn default_level_fanout() -> u32 {
    DEFAULT_LEVEL_FANOUT
}

const fn default_l0_file_threshold() -> u32 {
    DEFAULT_L0_FILE_THRESHOLD
}

const fn default_max_levels() -> u32 {
    DEFAULT_MAX_LEVELS
}

fn default_worker_parallelism() -> u32 {
    std::thread::available_parallelism()
        .map(|parallelism| parallelism.get() as u32)
        .unwrap_or(1)
}

fn validate_page_size_bytes(page_size_bytes: u32) -> Result<(), ConfigError> {
    if !(4096..=32768).contains(&page_size_bytes) || !page_size_bytes.is_power_of_two() {
        return Err(ConfigError::Invalid(
            "engine.page_size_bytes must be a power of two in the range 4096..=32768".into(),
        ));
    }

    Ok(())
}

fn validate_segment_bytes(segment_bytes: u64) -> Result<(), ConfigError> {
    let minimum_segment_bytes = MAX_SINGLE_WAL_RECORD_BYTES + 256;
    if segment_bytes <= minimum_segment_bytes {
        return Err(ConfigError::Invalid(format!(
            "wal.segment_bytes must be greater than {minimum_segment_bytes}"
        )));
    }

    Ok(())
}

fn validate_positive_u32(value: u32, field_name: &str) -> Result<(), ConfigError> {
    if value == 0 {
        return Err(ConfigError::Invalid(format!(
            "{field_name} must be greater than zero"
        )));
    }

    Ok(())
}

fn validate_positive_u64(value: u64, field_name: &str) -> Result<(), ConfigError> {
    if value == 0 {
        return Err(ConfigError::Invalid(format!(
            "{field_name} must be greater than zero"
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::load_runtime_config;
    use super::{SyncMode, default_worker_parallelism, parse_runtime_config};

    static NEXT_CONFIG_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn config_parsing_applies_pezhai_defaults() {
        let config = parse_runtime_config(
            r#"
[sevai]
listen_addr = "127.0.0.1:7000"
"#,
        )
        .unwrap();

        assert_eq!(config.engine.sync_mode, SyncMode::PerWrite);
        assert_eq!(config.engine.page_size_bytes, 4096);
        assert_eq!(config.wal.segment_bytes, 1_073_741_824);
        assert_eq!(config.wal.group_commit_bytes, 65_536);
        assert_eq!(config.wal.group_commit_max_delay_ms, 50);
        assert_eq!(config.lsm.memtable_flush_bytes, 67_108_864);
        assert_eq!(config.lsm.base_level_bytes, 268_435_456);
        assert_eq!(config.lsm.level_fanout, 10);
        assert_eq!(config.lsm.l0_file_threshold, 8);
        assert_eq!(config.lsm.max_levels, 7);
        assert_eq!(config.maintenance.checkpoint_after_wal_bytes, 1_073_741_824);
        assert_eq!(config.maintenance.logical_split_bytes, 536_870_912);
        assert_eq!(config.maintenance.logical_merge_bytes, 134_217_728);
        assert_eq!(config.maintenance.max_concurrent_flushes, 1);
        assert_eq!(config.maintenance.max_concurrent_compactions, 1);
        assert_eq!(
            config.server_limits.worker_parallelism,
            default_worker_parallelism()
        );
        assert_eq!(config.server_limits.max_pending_requests, 1024);
        assert_eq!(config.server_limits.max_worker_tasks, 256);
        assert_eq!(config.server_limits.max_scan_sessions, 128);
        assert_eq!(config.server_limits.max_in_flight_scan_tasks, 64);
        assert_eq!(config.server_limits.max_scan_fetch_queue_per_session, 64);
        assert_eq!(config.server_limits.max_waiting_durability_waiters, 1024);
        assert_eq!(config.server_limits.scan_idle_timeout_ms, 300_000);
        assert_eq!(config.sevai.listen_addr.to_string(), "127.0.0.1:7000");
    }

    #[test]
    fn config_parsing_accepts_full_forward_compatible_shape() {
        let config = parse_runtime_config(
            r#"
[engine]
sync_mode = "manual"
page_size_bytes = 8192

[wal]
segment_bytes = 268436900
group_commit_bytes = 1024
group_commit_max_delay_ms = 3

[lsm]
memtable_flush_bytes = 65536
base_level_bytes = 131072
level_fanout = 4
l0_file_threshold = 3
max_levels = 6

[maintenance]
checkpoint_after_wal_bytes = 4096
logical_split_bytes = 262144
logical_merge_bytes = 65536
max_concurrent_flushes = 2
max_concurrent_compactions = 3

[server_limits]
worker_parallelism = 6
max_pending_requests = 77
max_worker_tasks = 88
max_scan_sessions = 9
max_in_flight_scan_tasks = 10
max_scan_fetch_queue_per_session = 11
max_waiting_durability_waiters = 12
scan_idle_timeout_ms = 13

[sevai]
listen_addr = "127.0.0.1:7001"
"#,
        )
        .unwrap();

        assert_eq!(config.engine.sync_mode, SyncMode::Manual);
        assert_eq!(config.engine.page_size_bytes, 8192);
        assert_eq!(config.wal.segment_bytes, 268_436_900);
        assert_eq!(config.wal.group_commit_bytes, 1024);
        assert_eq!(config.wal.group_commit_max_delay_ms, 3);
        assert_eq!(config.lsm.memtable_flush_bytes, 65_536);
        assert_eq!(config.lsm.base_level_bytes, 131_072);
        assert_eq!(config.lsm.level_fanout, 4);
        assert_eq!(config.lsm.l0_file_threshold, 3);
        assert_eq!(config.lsm.max_levels, 6);
        assert_eq!(config.maintenance.checkpoint_after_wal_bytes, 4096);
        assert_eq!(config.maintenance.logical_split_bytes, 262_144);
        assert_eq!(config.maintenance.logical_merge_bytes, 65_536);
        assert_eq!(config.maintenance.max_concurrent_flushes, 2);
        assert_eq!(config.maintenance.max_concurrent_compactions, 3);
        assert_eq!(config.server_limits.worker_parallelism, 6);
        assert_eq!(config.server_limits.max_pending_requests, 77);
        assert_eq!(config.server_limits.max_worker_tasks, 88);
        assert_eq!(config.server_limits.max_scan_sessions, 9);
        assert_eq!(config.server_limits.max_in_flight_scan_tasks, 10);
        assert_eq!(config.server_limits.max_scan_fetch_queue_per_session, 11);
        assert_eq!(config.server_limits.max_waiting_durability_waiters, 12);
        assert_eq!(config.server_limits.scan_idle_timeout_ms, 13);
        assert_eq!(config.sevai.listen_addr.to_string(), "127.0.0.1:7001");
    }

    #[test]
    fn config_parsing_requires_sevai_table() {
        let error = parse_runtime_config(
            r#"
[engine]
sync_mode = "per_write"
"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("sevai"));
    }

    #[test]
    fn config_parsing_rejects_invalid_page_size() {
        let error = parse_runtime_config(
            r#"
[engine]
page_size_bytes = 5000

[sevai]
listen_addr = "127.0.0.1:7000"
"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("engine.page_size_bytes"));
    }

    #[test]
    fn config_parsing_rejects_invalid_maintenance_relationships() {
        let error = parse_runtime_config(
            r#"
[maintenance]
logical_split_bytes = 4096
logical_merge_bytes = 4096

[sevai]
listen_addr = "127.0.0.1:7000"
"#,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("maintenance.logical_merge_bytes")
        );
    }

    #[test]
    fn config_parsing_rejects_zero_server_limits() {
        let error = parse_runtime_config(
            r#"
[server_limits]
max_pending_requests = 0

[sevai]
listen_addr = "127.0.0.1:7000"
"#,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("server_limits.max_pending_requests")
        );
    }

    #[test]
    fn config_parsing_rejects_the_remaining_scalar_validation_failures() {
        let cases = [
            ("[wal]\nsegment_bytes = 1", "wal.segment_bytes"),
            ("[wal]\ngroup_commit_bytes = 0", "wal.group_commit_bytes"),
            (
                "[wal]\ngroup_commit_bytes = 1073741824",
                "wal.group_commit_bytes",
            ),
            (
                "[wal]\ngroup_commit_max_delay_ms = 0",
                "wal.group_commit_max_delay_ms",
            ),
            (
                "[lsm]\nmemtable_flush_bytes = 8192",
                "lsm.memtable_flush_bytes",
            ),
            (
                "[lsm]\nmemtable_flush_bytes = 65536\nbase_level_bytes = 32768",
                "lsm.base_level_bytes",
            ),
            ("[lsm]\nlevel_fanout = 1", "lsm.level_fanout"),
            ("[lsm]\nl0_file_threshold = 1", "lsm.l0_file_threshold"),
            ("[lsm]\nmax_levels = 3", "lsm.max_levels"),
            (
                "[maintenance]\ncheckpoint_after_wal_bytes = 0",
                "maintenance.checkpoint_after_wal_bytes",
            ),
            (
                "[maintenance]\nlogical_split_bytes = 0",
                "maintenance.logical_split_bytes",
            ),
            (
                "[maintenance]\nmax_concurrent_flushes = 0",
                "maintenance.max_concurrent_flushes",
            ),
            (
                "[maintenance]\nmax_concurrent_compactions = 0",
                "maintenance.max_concurrent_compactions",
            ),
            (
                "[server_limits]\nworker_parallelism = 0",
                "server_limits.worker_parallelism",
            ),
            (
                "[server_limits]\nmax_worker_tasks = 0",
                "server_limits.max_worker_tasks",
            ),
            (
                "[server_limits]\nmax_scan_sessions = 0",
                "server_limits.max_scan_sessions",
            ),
            (
                "[server_limits]\nmax_in_flight_scan_tasks = 0",
                "server_limits.max_in_flight_scan_tasks",
            ),
            (
                "[server_limits]\nmax_scan_fetch_queue_per_session = 0",
                "server_limits.max_scan_fetch_queue_per_session",
            ),
            (
                "[server_limits]\nmax_waiting_durability_waiters = 0",
                "server_limits.max_waiting_durability_waiters",
            ),
            (
                "[server_limits]\nscan_idle_timeout_ms = 0",
                "server_limits.scan_idle_timeout_ms",
            ),
        ];

        for (snippet, expected_field) in cases {
            let config_text = format!(
                r#"
{snippet}

[sevai]
listen_addr = "127.0.0.1:7000"
"#
            );
            let error = parse_runtime_config(&config_text).unwrap_err();
            assert!(
                error.to_string().contains(expected_field),
                "expected `{expected_field}` in `{error}`",
            );
        }
    }

    #[test]
    fn load_runtime_config_reads_from_disk() {
        let path = write_test_config(
            r#"
[engine]
sync_mode = "manual"

[sevai]
listen_addr = "127.0.0.1:7010"
"#,
        );

        let config = load_runtime_config(&path).unwrap();
        assert_eq!(config.engine.sync_mode, SyncMode::Manual);
        assert_eq!(config.sevai.listen_addr.to_string(), "127.0.0.1:7010");
    }

    #[test]
    fn load_runtime_config_reports_io_failures() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let config_id = NEXT_CONFIG_ID.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("missing-pezhai-config-{unique}-{config_id}.toml"));

        let error = load_runtime_config(&path).unwrap_err();
        assert!(error.to_string().contains("failed to read config"));
    }

    fn write_test_config(contents: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let config_id = NEXT_CONFIG_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("pezhai-config-{unique}-{config_id}.toml"));
        fs::write(&path, contents).unwrap();
        path
    }
}
