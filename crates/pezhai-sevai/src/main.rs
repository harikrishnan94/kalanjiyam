//! Thin Tokio binary for bootstrapping the `pezhai::sevai` server.

use std::env;
use std::future::Future;
use std::path::PathBuf;

use pezhai::sevai::{PezhaiServer, ServerBootstrapArgs};
use pezhai_sevai::adapter::run_tcp_adapter;

type MainResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
async fn main() -> MainResult {
    run_until_signal(env::args().skip(1), tokio::signal::ctrl_c()).await
}

async fn run_until_signal(
    args: impl Iterator<Item = String>,
    shutdown_signal: impl Future<Output = Result<(), std::io::Error>>,
) -> MainResult {
    let config_path = parse_config_path(args)?;
    let server = PezhaiServer::start(ServerBootstrapArgs { config_path }).await?;

    let adapter_server = server.clone();
    let adapter_task = tokio::spawn(async move { run_tcp_adapter(adapter_server).await });

    shutdown_signal.await?;
    server.shutdown().await?;
    server.wait_stopped().await?;

    match adapter_task.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(Box::<dyn std::error::Error + Send + Sync>::from(error)),
        Err(error) => Err(Box::<dyn std::error::Error + Send + Sync>::from(error)),
    }
}

fn parse_config_path(
    mut args: impl Iterator<Item = String>,
) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    match (args.next(), args.next(), args.next()) {
        (Some(flag), Some(path), None) if flag == "--config" => Ok(PathBuf::from(path)),
        _ => Err("usage: pezhai-sevai --config <path>".into()),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::net::{SocketAddr, TcpListener as StdTcpListener};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use tokio::net::TcpStream;

    use super::{parse_config_path, run_until_signal};

    static NEXT_CONFIG_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn parse_config_path_accepts_expected_cli_shape() {
        let parsed = parse_config_path(
            ["--config".to_string(), "/tmp/pezhai-sevai.toml".to_string()].into_iter(),
        )
        .unwrap();

        assert_eq!(parsed, PathBuf::from("/tmp/pezhai-sevai.toml"));
    }

    #[test]
    fn parse_config_path_rejects_unexpected_cli_shape() {
        let error = parse_config_path(["--verbose".to_string()].into_iter()).unwrap_err();
        assert_eq!(error.to_string(), "usage: pezhai-sevai --config <path>");
    }

    #[tokio::test]
    async fn run_until_signal_starts_and_stops_the_server() {
        let addr = reserve_local_addr();
        let config_path = write_test_config(addr);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        let run_task = tokio::spawn(async move {
            run_until_signal(
                [
                    "--config".to_string(),
                    config_path.to_string_lossy().into_owned(),
                ]
                .into_iter(),
                async move {
                    shutdown_rx
                        .await
                        .map_err(|_| std::io::Error::other("test signal dropped"))?;
                    Ok(())
                },
            )
            .await
        });

        wait_for_listener(addr).await;
        shutdown_tx.send(()).unwrap();

        let result = run_task.await.unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn run_until_signal_reports_startup_errors() {
        let error = run_until_signal(
            [
                "--config".to_string(),
                "/tmp/definitely-missing-pezhai-sevai.toml".to_string(),
            ]
            .into_iter(),
            async { Ok(()) },
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("I/O error"));
    }

    fn reserve_local_addr() -> SocketAddr {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        addr
    }

    fn write_test_config(addr: SocketAddr) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let config_id = NEXT_CONFIG_ID.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("pezhai-sevai-main-{unique}-{config_id}.toml"));
        fs::write(
            &path,
            format!(
                r#"
[engine]
sync_mode = "per_write"

[sevai]
listen_addr = "{addr}"
"#
            ),
        )
        .unwrap();
        path
    }

    async fn wait_for_listener(addr: SocketAddr) {
        for _ in 0..50 {
            if TcpStream::connect(addr).await.is_ok() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        panic!("listener never became ready at {addr}");
    }
}
