//! Integration matrix for the thin binary bootstrap.

use std::process::Command;

use crate::common::{StoreOptions, create_store};

#[test]
fn binary_rejects_missing_arguments() {
    let status = Command::new(env!("CARGO_BIN_EXE_kalanjiyam"))
        .status()
        .expect("binary should be executable from integration tests");
    assert!(
        !status.success(),
        "missing bootstrap arguments should fail the thin binary entrypoint"
    );
}

#[test]
fn binary_rejects_wrong_flag() {
    let status = Command::new(env!("CARGO_BIN_EXE_kalanjiyam"))
        .arg("--wrong")
        .arg("config.toml")
        .status()
        .expect("binary should be executable from integration tests");
    assert!(
        !status.success(),
        "unexpected bootstrap flag should fail the thin binary entrypoint"
    );
}

#[test]
fn binary_rejects_extra_arguments() {
    let status = Command::new(env!("CARGO_BIN_EXE_kalanjiyam"))
        .arg("--config")
        .arg("config.toml")
        .arg("extra")
        .status()
        .expect("binary should be executable from integration tests");
    assert!(
        !status.success(),
        "unexpected extra bootstrap arguments should fail the thin binary entrypoint"
    );
}

#[test]
fn binary_rejects_missing_config_path_after_flag() {
    let status = Command::new(env!("CARGO_BIN_EXE_kalanjiyam"))
        .arg("--config")
        .status()
        .expect("binary should be executable from integration tests");
    assert!(
        !status.success(),
        "missing config path should fail the thin binary entrypoint"
    );
}

#[test]
fn binary_fails_when_bootstrap_config_path_is_missing() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let missing = config_path
        .parent()
        .expect("config path should have a store parent")
        .join("missing.toml");
    let status = Command::new(env!("CARGO_BIN_EXE_kalanjiyam"))
        .arg("--config")
        .arg(&missing)
        .status()
        .expect("binary should be executable from integration tests");
    assert!(
        !status.success(),
        "server bootstrap should fail when the config path does not exist"
    );
}
