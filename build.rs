//! Build-script code generation for the Buffa-backed `sevai` protobuf schema.
//!
//! The repository keeps `.proto` files under `proto/` and generates Rust into
//! `OUT_DIR` at compile time so generated sources never need to be checked in.

use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    if let Err(error) = run() {
        panic!("failed to generate Buffa protobuf bindings: {error}");
    }
}

fn run() -> Result<(), String> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").map_err(|error| {
        format!("CARGO_MANIFEST_DIR is not set for build.rs execution: {error}")
    })?);
    let out_dir =
        PathBuf::from(env::var("OUT_DIR").map_err(|error| format!("OUT_DIR is not set: {error}"))?);
    let proto_dir = manifest_dir.join("proto");
    let proto_files = vec![proto_dir.join("sevai.proto")];
    let generated_dir = out_dir.join("sevai_proto");
    let include_file = out_dir.join("sevai_proto_include.rs");

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=proto/sevai.proto");
    println!("cargo:rerun-if-env-changed=PROTOC");
    println!("cargo:rerun-if-env-changed=PROTOC_GEN_BUFFA");

    fs::create_dir_all(&generated_dir).map_err(|error| {
        format!(
            "failed to create generated protobuf output dir {}: {error}",
            generated_dir.display()
        )
    })?;

    // Remove stale Rust files so package or file renames do not leave old
    // includes behind in the generated directory.
    for entry in fs::read_dir(&generated_dir).map_err(|error| {
        format!(
            "failed to read generated protobuf output dir {}: {error}",
            generated_dir.display()
        )
    })? {
        let entry = entry.map_err(|error| {
            format!(
                "failed to inspect generated protobuf output dir {}: {error}",
                generated_dir.display()
            )
        })?;
        let path = entry.path();
        if path.extension().is_some_and(|extension| extension == "rs") {
            fs::remove_file(&path).map_err(|error| {
                format!(
                    "failed to remove stale generated file {}: {error}",
                    path.display()
                )
            })?;
        }
    }

    invoke_protoc(&proto_dir, &generated_dir, &proto_files)?;
    write_include_file(&generated_dir, &include_file)?;
    Ok(())
}

fn invoke_protoc(
    proto_dir: &Path,
    generated_dir: &Path,
    proto_files: &[PathBuf],
) -> Result<(), String> {
    let protoc = env::var_os("PROTOC").unwrap_or_else(|| OsString::from("protoc"));
    let protoc_gen_buffa =
        env::var_os("PROTOC_GEN_BUFFA").unwrap_or_else(|| OsString::from("protoc-gen-buffa"));
    let plugin_flag = OsString::from(format!(
        "--plugin=protoc-gen-buffa={}",
        PathBuf::from(&protoc_gen_buffa).display()
    ));
    let out_flag = OsString::from(format!("--buffa_out={}", generated_dir.display()));
    let opt_flag = OsString::from("--buffa_opt=views=true");

    // Drive code generation through the protoc plugin path required by the
    // transport contract instead of via `buf build`.
    let mut command = Command::new(&protoc);
    command.arg(plugin_flag);
    command.arg(out_flag);
    command.arg(opt_flag);
    command.arg(OsString::from(format!(
        "--proto_path={}",
        proto_dir.display()
    )));
    for proto_file in proto_files {
        command.arg(proto_file);
    }

    let output = command.output().map_err(|error| {
        format!(
            "failed to invoke protoc at {} with protoc-gen-buffa at {}: {error}",
            PathBuf::from(&protoc).display(),
            PathBuf::from(&protoc_gen_buffa).display()
        )
    })?;

    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    Err(format!(
        "protoc returned {}.\nstdout:\n{}\nstderr:\n{}\nSet PROTOC or PROTOC_GEN_BUFFA if the tools are not on PATH.",
        output.status,
        stdout.trim(),
        stderr.trim(),
    ))
}

fn write_include_file(generated_dir: &Path, include_file: &Path) -> Result<(), String> {
    let mut entries = fs::read_dir(generated_dir)
        .map_err(|error| {
            format!(
                "failed to read generated protobuf output dir {}: {error}",
                generated_dir.display()
            )
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            format!(
                "failed to enumerate generated protobuf output dir {}: {error}",
                generated_dir.display()
            )
        })?;
    entries.sort_by_key(|entry| entry.file_name());

    // Assemble one stable include file so the library source does not need to
    // know Buffa's generated file naming scheme.
    let mut body = String::new();
    for entry in entries {
        let path = entry.path();
        if path.extension().is_none_or(|extension| extension != "rs") {
            continue;
        }
        let file_name = path.file_name().ok_or_else(|| {
            format!(
                "generated protobuf output path is missing a file name: {}",
                path.display()
            )
        })?;
        body.push_str(&format!(
            "include!(concat!(env!(\"OUT_DIR\"), \"/sevai_proto/{}\"));\n",
            file_name.to_string_lossy()
        ));
    }

    if body.is_empty() {
        return Err(format!(
            "protoc succeeded but no Rust files were generated in {}",
            generated_dir.display()
        ));
    }

    fs::write(include_file, body).map_err(|error| {
        format!(
            "failed to write generated protobuf include file {}: {error}",
            include_file.display()
        )
    })?;
    Ok(())
}
