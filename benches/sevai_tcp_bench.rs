//! Custom benchmark entrypoint for the loopback TCP `sevai` transport.
//!
//! The benchmark harness is disabled (`harness = false`) in `Cargo.toml`.
//! This binary delegates workload parsing and execution to
//! `kalanjiyam::sevai::benchmark`.

fn main() {
    if let Err(error) =
        kalanjiyam::sevai::benchmark::run_sevai_tcp_benchmark(std::env::args().skip(1))
    {
        eprintln!("sevai_tcp_bench failed: {error}");
        std::process::exit(1);
    }
}
