//! Shared parsing for the repository's startup `config.toml` shape.

use std::fmt::{self, Display, Formatter};
use std::fs;
use std::net::SocketAddr;
use std::path::Path;

use serde::Deserialize;

/// Loads the runtime config from disk and applies the schema defaults.
pub(crate) fn load_runtime_config(path: &Path) -> Result<RuntimeConfig, ConfigError> {
    let config_text = fs::read_to_string(path).map_err(ConfigError::Io)?;
    parse_runtime_config(&config_text)
}

/// Parses the runtime config from TOML text.
pub(crate) fn parse_runtime_config(text: &str) -> Result<RuntimeConfig, ConfigError> {
    toml::from_str(text).map_err(ConfigError::Parse)
}

/// Errors raised while parsing the shared TOML config.
#[derive(Debug)]
pub(crate) enum ConfigError {
    Io(std::io::Error),
    Parse(toml::de::Error),
}

impl Display for ConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "failed to read config: {error}"),
            Self::Parse(error) => write!(formatter, "failed to parse config: {error}"),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Parse(error) => Some(error),
        }
    }
}

/// Shared config parsed by both the library and the binary bootstrap path.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub(crate) struct RuntimeConfig {
    #[serde(default)]
    pub(crate) engine: EngineConfig,
    #[serde(default)]
    pub(crate) wal: WalConfig,
    #[serde(default)]
    pub(crate) lsm: LsmConfig,
    pub(crate) sevai: SevaiConfig,
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

/// LSM-facing settings preserved for future real-engine wiring.
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

/// TCP adapter settings required by the sevai scaffold.
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

const fn default_page_size_bytes() -> u32 {
    4096
}

const fn default_segment_bytes() -> u64 {
    1_073_741_824
}

const fn default_group_commit_bytes() -> u64 {
    65_536
}

const fn default_group_commit_max_delay_ms() -> u64 {
    50
}

const fn default_memtable_flush_bytes() -> u64 {
    67_108_864
}

const fn default_base_level_bytes() -> u64 {
    268_435_456
}

const fn default_level_fanout() -> u32 {
    10
}

const fn default_l0_file_threshold() -> u32 {
    8
}

const fn default_max_levels() -> u32 {
    7
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::load_runtime_config;
    use super::{SyncMode, parse_runtime_config};

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
segment_bytes = 2048
group_commit_bytes = 1024
group_commit_max_delay_ms = 3

[lsm]
memtable_flush_bytes = 4096
base_level_bytes = 8192
level_fanout = 4
l0_file_threshold = 3
max_levels = 6

[sevai]
listen_addr = "127.0.0.1:7001"
"#,
        )
        .unwrap();

        assert_eq!(config.engine.sync_mode, SyncMode::Manual);
        assert_eq!(config.engine.page_size_bytes, 8192);
        assert_eq!(config.wal.segment_bytes, 2048);
        assert_eq!(config.wal.group_commit_bytes, 1024);
        assert_eq!(config.wal.group_commit_max_delay_ms, 3);
        assert_eq!(config.lsm.memtable_flush_bytes, 4096);
        assert_eq!(config.lsm.base_level_bytes, 8192);
        assert_eq!(config.lsm.level_fanout, 4);
        assert_eq!(config.lsm.l0_file_threshold, 3);
        assert_eq!(config.lsm.max_levels, 6);
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
    fn load_runtime_config_reads_from_disk() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let config_id = NEXT_CONFIG_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("pezhai-config-{unique}-{config_id}.toml"));
        fs::write(
            &path,
            r#"
[sevai]
listen_addr = "127.0.0.1:7020"
"#,
        )
        .unwrap();

        let config = load_runtime_config(&path).unwrap();
        assert_eq!(config.sevai.listen_addr.to_string(), "127.0.0.1:7020");
    }

    #[test]
    fn load_runtime_config_reports_io_failures() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let config_id = NEXT_CONFIG_ID.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("pezhai-missing-config-{unique}-{config_id}.toml"));

        let error = load_runtime_config(&path).unwrap_err();
        assert!(error.to_string().contains("failed to read config"));
    }
}
