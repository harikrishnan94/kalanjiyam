//! Build script for generating sevai protobuf bindings for the server package.

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let workspace_root = workspace_root();
    let package_root = Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf();
    let proto_dir = package_root.join("proto");
    let proto_file = proto_dir.join("sevai.proto");
    let buf_yaml = workspace_root.join("buf.yaml");
    let mut config = buffa_build::Config::new().include_file("sevai_proto_include.rs");

    println!("cargo:rerun-if-changed={}", proto_file.display());
    println!("cargo:rerun-if-env-changed=PROTOC");
    if buf_yaml.exists() {
        println!("cargo:rerun-if-changed={}", buf_yaml.display());
    }

    if has_command("buf") && buf_yaml.exists() {
        env::set_current_dir(&workspace_root).unwrap_or_else(|error| {
            panic!(
                "failed to switch build-script cwd to workspace root {}: {error}",
                workspace_root.display()
            )
        });
        config = config
            .use_buf()
            .files(&["crates/pezhai-sevai/proto/sevai.proto"]);
    } else if has_command(protoc_name().as_str()) {
        let proto_file = proto_file.to_string_lossy().into_owned();
        let proto_dir = proto_dir.to_string_lossy().into_owned();
        config = config.files(&[proto_file]).includes(&[proto_dir]);
    } else {
        panic!(
            "protobuf code generation requires either `buf` (with `buf.yaml`) or `protoc` on \
             PATH. Install one of those tools and rebuild."
        );
    }

    config
        .compile()
        .unwrap_or_else(|error| panic!("failed to generate sevai protobuf code: {error}"));
}

fn has_command(command: &str) -> bool {
    Command::new(command)
        .arg("--version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn protoc_name() -> String {
    env::var("PROTOC").unwrap_or_else(|_| "protoc".to_string())
}

fn workspace_root() -> PathBuf {
    // The package lives under `crates/pezhai-sevai`, so the workspace root is two levels up.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| {
            panic!(
                "failed to derive workspace root from {}",
                env!("CARGO_MANIFEST_DIR")
            )
        })
        .to_path_buf()
}
