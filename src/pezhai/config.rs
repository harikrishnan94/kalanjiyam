//! Config parsing and validation for `store/config.toml`.

use std::fs;
use std::path::{Path, PathBuf};

use toml::Value;

use crate::error::KalanjiyamError;
use crate::pezhai::types::{
    DEFAULT_PAGE_SIZE_BYTES, MAX_SINGLE_WAL_RECORD_BYTES, SyncMode, validate_page_size_bytes,
};

/// Engine-facing configuration values derived from `[engine]`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineConfig {
    /// Durability acknowledgment mode.
    pub sync_mode: SyncMode,
    /// Page size used for newly written `.kjm` artifacts.
    pub page_size_bytes: usize,
}

/// WAL configuration values derived from `[wal]`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalConfig {
    /// Maximum segment file size including header and footer.
    pub segment_bytes: u64,
    /// Threshold that forces one shared sync batch.
    pub group_commit_bytes: u64,
    /// Maximum age of the oldest pending sync waiter in milliseconds.
    pub group_commit_max_delay_ms: u64,
}

/// LSM configuration values derived from `[lsm]`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LsmConfig {
    /// Active-memtable freeze threshold in bytes.
    pub memtable_flush_bytes: u64,
    /// Base level target bytes.
    pub base_level_bytes: u64,
    /// Level fanout.
    pub level_fanout: u32,
    /// L0 file-count threshold.
    pub l0_file_threshold: u32,
    /// Maximum number of levels tracked in stats and manifests.
    pub max_levels: u32,
}

/// Implementation-specific host policy values from `[maintenance]`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceConfig {
    /// Maximum queued external requests accepted by the server.
    pub max_external_requests: usize,
    /// Maximum queued scan fetch requests per scan session.
    pub max_scan_fetch_queue: usize,
    /// Maximum active scan sessions.
    pub max_scan_sessions: usize,
    /// Maximum queued worker tasks.
    pub max_worker_tasks: usize,
    /// Maximum waiting WAL sync requests.
    pub max_waiting_wal_syncs: usize,
    /// Scan-session idle expiry in milliseconds.
    pub scan_expiry_ms: u64,
    /// Maintenance retry delay in milliseconds.
    pub retry_delay_ms: u64,
    /// Number of generic background worker threads.
    pub worker_threads: usize,
    /// Garbage-collection polling cadence in seconds.
    pub gc_interval_secs: u64,
    /// Checkpoint polling cadence in seconds.
    pub checkpoint_interval_secs: u64,
}

/// Fully validated pezhai configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PezhaiConfig {
    /// Canonical absolute path to `store/config.toml`.
    pub config_path: PathBuf,
    /// Canonical absolute path to the enclosing `store/` directory.
    pub store_dir: PathBuf,
    /// Parsed engine settings.
    pub engine: EngineConfig,
    /// Parsed WAL settings.
    pub wal: WalConfig,
    /// Parsed LSM settings.
    pub lsm: LsmConfig,
    /// Parsed host policy settings.
    pub maintenance: MaintenanceConfig,
}

impl PezhaiConfig {
    /// Loads and validates one `store/config.toml` file.
    ///
    /// This entry point canonicalizes the provided path, derives the enclosing
    /// `store/` directory from that canonical path, applies spec defaults for
    /// omitted sections or keys, and validates only the semantic TOML values.
    /// Unknown keys are tolerated because the spec allows implementations to
    /// ignore them.
    ///
    /// # Complexity
    /// Time: O(p + b), where `p` is the length of the input path and `b` is
    /// the size of `config.toml` in bytes; this covers canonicalizing the path,
    /// reading `b` bytes, and parsing/validating the resulting TOML table. If
    /// the underlying filesystem is slow, I/O latency for `canonicalize` and
    /// `read_to_string` may dominate despite the linear scan over `b` bytes.
    /// Space: O(p + b) for the canonicalized path, the TOML contents held in
    /// memory, and the temporary parsed table used to build the validated config.
    pub fn load(config_path: impl AsRef<Path>) -> Result<Self, KalanjiyamError> {
        let canonical_config_path = fs::canonicalize(config_path.as_ref())
            .map_err(|error| KalanjiyamError::io("canonicalize config path", error))?;
        let store_dir = canonical_config_path
            .parent()
            .ok_or_else(|| {
                KalanjiyamError::InvalidArgument(
                    "config path must have a parent store directory".to_string(),
                )
            })?
            .to_path_buf();
        let bytes = fs::read_to_string(&canonical_config_path)
            .map_err(|error| KalanjiyamError::io("read config.toml", error))?;
        let value: Value = toml::from_str(&bytes).map_err(|error| {
            KalanjiyamError::InvalidArgument(format!("config.toml is not valid TOML: {error}"))
        })?;
        let table = value.as_table().ok_or_else(|| {
            KalanjiyamError::InvalidArgument("config root must be a table".to_string())
        })?;

        let engine = parse_engine(table.get("engine"))?;
        let wal = parse_wal(table.get("wal"))?;
        let lsm = parse_lsm(table.get("lsm"), engine.page_size_bytes)?;
        let maintenance = parse_maintenance(table.get("maintenance"))?;

        Ok(Self {
            config_path: canonical_config_path,
            store_dir,
            engine,
            wal,
            lsm,
            maintenance,
        })
    }
}

fn parse_engine(value: Option<&Value>) -> Result<EngineConfig, KalanjiyamError> {
    let table = optional_table(value, "engine")?;
    let sync_mode = match table.and_then(|table| table.get("sync_mode")) {
        None => SyncMode::PerWrite,
        Some(Value::String(mode)) if mode == "per_write" => SyncMode::PerWrite,
        Some(Value::String(mode)) if mode == "manual" => SyncMode::Manual,
        Some(Value::String(mode)) => {
            return Err(KalanjiyamError::InvalidArgument(format!(
                "engine.sync_mode must be \"per_write\" or \"manual\", got \"{mode}\""
            )));
        }
        Some(_) => {
            return Err(KalanjiyamError::InvalidArgument(
                "engine.sync_mode must be a string".to_string(),
            ));
        }
    };

    let page_size_bytes =
        table_usize(table, "engine.page_size_bytes")?.unwrap_or(DEFAULT_PAGE_SIZE_BYTES);
    validate_page_size_bytes(page_size_bytes)?;

    Ok(EngineConfig {
        sync_mode,
        page_size_bytes,
    })
}

fn parse_wal(value: Option<&Value>) -> Result<WalConfig, KalanjiyamError> {
    let table = optional_table(value, "wal")?;
    let segment_bytes = table_u64(table, "wal.segment_bytes")?.unwrap_or(1_073_741_824);
    let group_commit_bytes = table_u64(table, "wal.group_commit_bytes")?.unwrap_or(65_536);
    let group_commit_max_delay_ms =
        table_u64(table, "wal.group_commit_max_delay_ms")?.unwrap_or(50);

    if segment_bytes <= MAX_SINGLE_WAL_RECORD_BYTES as u64 + 256 {
        return Err(KalanjiyamError::InvalidArgument(
            "wal.segment_bytes must be > max_single_wal_record_bytes + 256".to_string(),
        ));
    }
    if group_commit_bytes == 0 || group_commit_bytes >= segment_bytes {
        return Err(KalanjiyamError::InvalidArgument(
            "wal.group_commit_bytes must be > 0 and < wal.segment_bytes".to_string(),
        ));
    }
    if group_commit_max_delay_ms == 0 {
        return Err(KalanjiyamError::InvalidArgument(
            "wal.group_commit_max_delay_ms must be > 0".to_string(),
        ));
    }

    Ok(WalConfig {
        segment_bytes,
        group_commit_bytes,
        group_commit_max_delay_ms,
    })
}

fn parse_lsm(value: Option<&Value>, page_size_bytes: usize) -> Result<LsmConfig, KalanjiyamError> {
    let table = optional_table(value, "lsm")?;
    let memtable_flush_bytes = table_u64(table, "lsm.memtable_flush_bytes")?.unwrap_or(67_108_864);
    let base_level_bytes = table_u64(table, "lsm.base_level_bytes")?.unwrap_or(268_435_456);
    let level_fanout = table_u32(table, "lsm.level_fanout")?.unwrap_or(10);
    let l0_file_threshold = table_u32(table, "lsm.l0_file_threshold")?.unwrap_or(8);
    let max_levels = table_u32(table, "lsm.max_levels")?.unwrap_or(7);

    if memtable_flush_bytes < 4 * page_size_bytes as u64 {
        return Err(KalanjiyamError::InvalidArgument(
            "lsm.memtable_flush_bytes must be >= 4 * page_size_bytes".to_string(),
        ));
    }
    if base_level_bytes < memtable_flush_bytes {
        return Err(KalanjiyamError::InvalidArgument(
            "lsm.base_level_bytes must be >= lsm.memtable_flush_bytes".to_string(),
        ));
    }
    if !(2..=32).contains(&level_fanout) {
        return Err(KalanjiyamError::InvalidArgument(
            "lsm.level_fanout must be in 2..=32".to_string(),
        ));
    }
    if l0_file_threshold < 2 {
        return Err(KalanjiyamError::InvalidArgument(
            "lsm.l0_file_threshold must be >= 2".to_string(),
        ));
    }
    if !(4..=16).contains(&max_levels) {
        return Err(KalanjiyamError::InvalidArgument(
            "lsm.max_levels must be in 4..=16".to_string(),
        ));
    }

    Ok(LsmConfig {
        memtable_flush_bytes,
        base_level_bytes,
        level_fanout,
        l0_file_threshold,
        max_levels,
    })
}

fn parse_maintenance(value: Option<&Value>) -> Result<MaintenanceConfig, KalanjiyamError> {
    let table = optional_table(value, "maintenance")?;
    let config = MaintenanceConfig {
        max_external_requests: table_usize(table, "maintenance.max_external_requests")?
            .unwrap_or(64),
        max_scan_fetch_queue: table_usize(table, "maintenance.max_scan_fetch_queue")?.unwrap_or(8),
        max_scan_sessions: table_usize(table, "maintenance.max_scan_sessions")?.unwrap_or(32),
        max_worker_tasks: table_usize(table, "maintenance.max_worker_tasks")?.unwrap_or(64),
        max_waiting_wal_syncs: table_usize(table, "maintenance.max_waiting_wal_syncs")?
            .unwrap_or(64),
        scan_expiry_ms: table_u64(table, "maintenance.scan_expiry_ms")?.unwrap_or(30_000),
        retry_delay_ms: table_u64(table, "maintenance.retry_delay_ms")?.unwrap_or(25),
        worker_threads: table_usize(table, "maintenance.worker_threads")?.unwrap_or(2),
        gc_interval_secs: table_u64(table, "maintenance.gc_interval_secs")?.unwrap_or(300),
        checkpoint_interval_secs: table_u64(table, "maintenance.checkpoint_interval_secs")?
            .unwrap_or(300),
    };

    if config.max_external_requests == 0
        || config.max_scan_fetch_queue == 0
        || config.max_scan_sessions == 0
        || config.max_worker_tasks == 0
        || config.max_waiting_wal_syncs == 0
        || config.scan_expiry_ms == 0
        || config.retry_delay_ms == 0
        || config.worker_threads == 0
        || config.gc_interval_secs == 0
        || config.checkpoint_interval_secs == 0
    {
        return Err(KalanjiyamError::InvalidArgument(
            "maintenance limits and delays must be greater than zero".to_string(),
        ));
    }

    Ok(config)
}

fn optional_table<'a>(
    value: Option<&'a Value>,
    table_name: &str,
) -> Result<Option<&'a toml::map::Map<String, Value>>, KalanjiyamError> {
    match value {
        None => Ok(None),
        Some(Value::Table(table)) => Ok(Some(table)),
        Some(_) => Err(KalanjiyamError::InvalidArgument(format!(
            "{table_name} must be a table"
        ))),
    }
}

fn table_u64(
    table: Option<&toml::map::Map<String, Value>>,
    key: &str,
) -> Result<Option<u64>, KalanjiyamError> {
    let Some(value) = table.and_then(|table| table.get(last_path_segment(key))) else {
        return Ok(None);
    };
    let integer = value.as_integer().ok_or_else(|| {
        KalanjiyamError::InvalidArgument(format!("{key} must be a non-negative integer"))
    })?;
    let converted = u64::try_from(integer).map_err(|_| {
        KalanjiyamError::InvalidArgument(format!("{key} must be a non-negative integer"))
    })?;
    Ok(Some(converted))
}

fn table_u32(
    table: Option<&toml::map::Map<String, Value>>,
    key: &str,
) -> Result<Option<u32>, KalanjiyamError> {
    match table_u64(table, key)? {
        None => Ok(None),
        Some(value) => {
            let converted = u32::try_from(value).map_err(|_| {
                KalanjiyamError::InvalidArgument(format!("{key} exceeds u32 range"))
            })?;
            Ok(Some(converted))
        }
    }
}

fn table_usize(
    table: Option<&toml::map::Map<String, Value>>,
    key: &str,
) -> Result<Option<usize>, KalanjiyamError> {
    match table_u64(table, key)? {
        None => Ok(None),
        Some(value) => {
            let converted = usize::try_from(value).map_err(|_| {
                KalanjiyamError::InvalidArgument(format!("{key} exceeds usize range"))
            })?;
            Ok(Some(converted))
        }
    }
}

fn last_path_segment(path: &str) -> &str {
    path.rsplit('.').next().unwrap_or(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_config(body: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let tempdir = tempfile::tempdir().expect("tempdir should be creatable for config tests");
        let config_path = tempdir.path().join("config.toml");
        fs::write(&config_path, body).expect("config fixture should be writable");
        (tempdir, config_path)
    }

    #[test]
    fn load_rejects_scalar_root_and_remaining_validation_cases() {
        let cases = [
            "1\n",
            "[wal]\nsegment_bytes = 8192\ngroup_commit_bytes = 8192\ngroup_commit_max_delay_ms = 1\n",
            "[lsm]\nmemtable_flush_bytes = 16384\nbase_level_bytes = 16384\nlevel_fanout = 1\n",
        ];

        for body in cases {
            let (_tempdir, config_path) = write_config(body);
            let error = PezhaiConfig::load(&config_path)
                .expect_err("invalid config fixture should fail to load");
            assert!(matches!(error, KalanjiyamError::InvalidArgument(_)));
        }
    }

    #[test]
    fn load_rejects_root_path_without_parent_store_directory() {
        let error = PezhaiConfig::load("/")
            .expect_err("root path should not be accepted as one config file path");
        assert!(matches!(error, KalanjiyamError::InvalidArgument(_)));
    }
}
