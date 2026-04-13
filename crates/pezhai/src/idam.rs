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
    #[cfg_attr(not(test), allow(dead_code))]
    #[must_use]
    pub(crate) fn config_path(&self) -> &Path {
        &self.config_path
    }

    /// Returns the store root that owns `CURRENT`, `wal/`, `meta/`, and `data/`.
    #[cfg_attr(not(test), allow(dead_code))]
    #[must_use]
    pub(crate) fn store_root(&self) -> &Path {
        &self.store_root
    }

    /// Returns the canonical `CURRENT` file path.
    #[must_use]
    pub(crate) fn current_path(&self) -> PathBuf {
        self.store_root.join("CURRENT")
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

    /// Returns the canonical metadata-checkpoint filename for one checkpoint generation.
    #[must_use]
    pub(crate) fn meta_file_path(&self, checkpoint_generation: u64) -> PathBuf {
        self.meta_dir
            .join(format!("meta-{checkpoint_generation:020}.kjm"))
    }

    /// Returns the canonical shared-data directory path.
    #[must_use]
    pub(crate) fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Returns the canonical shared-data filename for one durable `FileId`.
    #[must_use]
    pub(crate) fn data_file_path(&self, file_id: u64) -> PathBuf {
        self.data_dir.join(format!("{file_id:020}.kjm"))
    }

    /// Returns one non-canonical temporary shared-data filename.
    ///
    /// The temporary filename deliberately does not match the canonical
    /// `<file_id_20d>.kjm` pattern so crash recovery can keep unpublished files
    /// orphaned and invisible.
    #[must_use]
    pub(crate) fn temp_data_file_path(&self, tag: &str) -> PathBuf {
        self.data_dir.join(format!(".tmp-{tag}.kjm"))
    }

    /// Returns one non-canonical temporary metadata-checkpoint filename.
    #[must_use]
    pub(crate) fn temp_meta_file_path(&self, tag: &str) -> PathBuf {
        self.meta_dir.join(format!(".tmp-{tag}.kjm"))
    }

    /// Returns one non-canonical temporary `CURRENT` filename.
    #[must_use]
    pub(crate) fn temp_current_path(&self, tag: &str) -> PathBuf {
        self.store_root.join(format!(".tmp-CURRENT-{tag}"))
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
        assert_eq!(layout.current_path(), Path::new("/srv/pezhai/CURRENT"));
        assert_eq!(layout.wal_dir(), Path::new("/srv/pezhai/wal"));
        assert_eq!(layout.meta_dir(), Path::new("/srv/pezhai/meta"));
        assert_eq!(layout.data_dir(), Path::new("/srv/pezhai/data"));
        assert_eq!(
            layout.meta_file_path(7),
            Path::new("/srv/pezhai/meta/meta-00000000000000000007.kjm")
        );
    }

    #[test]
    fn falls_back_to_the_current_directory_without_a_parent() {
        let layout = StoreLayout::from_config_path(Path::new("config.toml"));

        assert_eq!(layout.store_root(), Path::new("."));
        assert_eq!(layout.current_path(), Path::new("./CURRENT"));
        assert_eq!(layout.wal_dir(), Path::new("./wal"));
        assert_eq!(layout.meta_dir(), Path::new("./meta"));
        assert_eq!(layout.data_dir(), Path::new("./data"));
    }

    #[test]
    fn derives_canonical_and_temporary_data_paths() {
        let layout = StoreLayout::from_config_path(Path::new("/srv/pezhai/config.toml"));

        assert_eq!(
            layout.data_file_path(42),
            Path::new("/srv/pezhai/data/00000000000000000042.kjm")
        );
        assert_eq!(
            layout.temp_data_file_path("flush-12-24"),
            Path::new("/srv/pezhai/data/.tmp-flush-12-24.kjm")
        );
        assert_eq!(
            layout.temp_meta_file_path("checkpoint-9"),
            Path::new("/srv/pezhai/meta/.tmp-checkpoint-9.kjm")
        );
        assert_eq!(
            layout.temp_current_path("checkpoint-9"),
            Path::new("/srv/pezhai/.tmp-CURRENT-checkpoint-9")
        );
    }
}
