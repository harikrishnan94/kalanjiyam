//! Thin binary bootstrap for the in-process `PezhaiServer`.

use std::future::Future;
use std::path::PathBuf;

use kalanjiyam::error::{Error, Result};
use kalanjiyam::sevai::{PezhaiServer, ServerBootstrapArgs};

fn main() -> Result<()> {
    run_with_entry(std::env::args().skip(1), |config_path| async move {
        let server = PezhaiServer::start(ServerBootstrapArgs { config_path }).await?;
        server.wait_stopped().await
    })
}

// Keep the binary success path testable without changing the public CLI contract.
fn run_with_entry<I, S, F, Fut>(args: I, entry: F) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
    F: FnOnce(PathBuf) -> Fut,
    Fut: Future<Output = std::result::Result<(), kalanjiyam::error::KalanjiyamError>>,
{
    let config_path = parse_config_path(args)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .map_err(|error| Error::io(error.to_string()))?;
    runtime.block_on(entry(config_path)).map_err(Into::into)
}

// Parse the stable thin-binary argument contract before the runtime starts.
fn parse_config_path<I, S>(args: I) -> Result<PathBuf>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut args = args.into_iter().map(Into::into);
    let flag = args.next().ok_or_else(|| {
        Error::invalid_argument("expected command line arguments: --config <path>")
    })?;
    if flag != "--config" {
        return Err(Error::invalid_argument(
            "expected command line arguments: --config <path>",
        ));
    }
    let config_path = args.next().ok_or_else(|| {
        Error::invalid_argument("expected command line arguments: --config <path>")
    })?;
    if args.next().is_some() {
        return Err(Error::invalid_argument(
            "unexpected extra command line arguments",
        ));
    }
    Ok(PathBuf::from(config_path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn parse_config_path_rejects_missing_flag_and_extra_args() {
        assert!(parse_config_path(Vec::<String>::new()).is_err());
        assert!(parse_config_path(["--wrong", "config.toml"]).is_err());
        assert!(parse_config_path(["--config", "config.toml", "extra"]).is_err());
    }

    #[test]
    fn run_with_entry_parses_args_and_runs_injected_success_path() {
        let seen = Arc::new(Mutex::new(None));
        run_with_entry(["--config", "/tmp/config.toml"], {
            let seen = Arc::clone(&seen);
            move |config_path| async move {
                *seen.lock().expect("test mutex should not poison") = Some(config_path);
                Ok(())
            }
        })
        .expect("injected bootstrap future should succeed");

        assert_eq!(
            seen.lock().expect("test mutex should not poison").clone(),
            Some(PathBuf::from("/tmp/config.toml"))
        );
    }
}
