//! Store-layout helpers for durable engine paths and neighboring on-disk modules.

use std::path::{Path, PathBuf};

/// Canonical store paths derived from the operator-supplied `config.toml` location.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoreLayout {
    config_path: PathBuf,
    store_root: PathBuf,
    wal_dir: PathBuf,
    meta_dir: PathBuf,
    data_dir: PathBuf,
}

impl StoreLayout {
    /// Resolves the standard store subdirectories relative to one config path.
    #[must_use]
    pub(crate) fn from_config_path(config_path: &Path) -> Self {
        let config_path = config_path.to_path_buf();
        let store_root = config_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let wal_dir = store_root.join("wal");
        let meta_dir = store_root.join("meta");
        let data_dir = store_root.join("data");

        Self {
            config_path,
            store_root,
            wal_dir,
            meta_dir,
            data_dir,
        }
    }

    /// Returns the original config path used during bootstrap.
    #[must_use]
    pub(crate) fn config_path(&self) -> &Path {
        &self.config_path
    }

    /// Returns the store root that owns `CURRENT`, `wal/`, `meta/`, and `data/`.
    #[must_use]
    pub(crate) fn store_root(&self) -> &Path {
        &self.store_root
    }

    /// Returns the canonical WAL directory path.
    #[must_use]
    pub(crate) fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    /// Returns the canonical metadata-checkpoint directory path.
    #[must_use]
    pub(crate) fn meta_dir(&self) -> &Path {
        &self.meta_dir
    }

    /// Returns the canonical shared-data directory path.
    #[must_use]
    pub(crate) fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::StoreLayout;

    #[test]
    fn derives_standard_store_paths_from_config_path() {
        let layout = StoreLayout::from_config_path(Path::new("/srv/pezhai/config.toml"));

        assert_eq!(layout.config_path(), Path::new("/srv/pezhai/config.toml"));
        assert_eq!(layout.store_root(), Path::new("/srv/pezhai"));
        assert_eq!(layout.wal_dir(), Path::new("/srv/pezhai/wal"));
        assert_eq!(layout.meta_dir(), Path::new("/srv/pezhai/meta"));
        assert_eq!(layout.data_dir(), Path::new("/srv/pezhai/data"));
    }

    #[test]
    fn falls_back_to_the_current_directory_without_a_parent() {
        let layout = StoreLayout::from_config_path(Path::new("config.toml"));

        assert_eq!(layout.store_root(), Path::new("."));
        assert_eq!(layout.wal_dir(), Path::new("./wal"));
        assert_eq!(layout.meta_dir(), Path::new("./meta"));
        assert_eq!(layout.data_dir(), Path::new("./data"));
    }
}
