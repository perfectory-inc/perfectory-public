//! Identity Platform HTTP server.

use std::ffi::OsStr;
use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use identity_api::{router, AppState, ProductionConfig};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if healthcheck_requested(std::env::args_os().nth(1).as_deref()) {
        return probe_readiness(healthcheck_address(bind_address()?), "/readyz");
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .json()
        .init();

    let config = ProductionConfig::from_env()?;
    let state = Arc::new(AppState::production(config).await?);
    let address = bind_address()?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    info!(%address, "Identity API listening");
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

fn healthcheck_requested(argument: Option<&OsStr>) -> bool {
    argument == Some(OsStr::new("--healthcheck"))
}

const fn healthcheck_address(bind_address: SocketAddr) -> SocketAddr {
    let address = match bind_address.ip() {
        IpAddr::V4(address) if address.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(address) if address.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        address => address,
    };
    SocketAddr::new(address, bind_address.port())
}

fn probe_readiness(address: SocketAddr, path: &str) -> anyhow::Result<()> {
    let timeout = Duration::from_secs(3);
    let mut stream = TcpStream::connect_timeout(&address, timeout)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    write!(
        stream,
        "GET {path} HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
    )?;
    stream.flush()?;

    let mut status_line = String::new();
    BufReader::new(stream).read_line(&mut status_line)?;
    if status_line.starts_with("HTTP/1.0 200 ") || status_line.starts_with("HTTP/1.1 200 ") {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Identity API readiness probe returned a non-ready status"
        ))
    }
}

fn bind_address() -> anyhow::Result<SocketAddr> {
    std::env::var("IDENTITY_API_BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_owned())
        .parse()
        .map_err(|_| anyhow::anyhow!("IDENTITY_API_BIND_ADDR is invalid"))
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("Identity API shutdown requested");
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    use super::{healthcheck_address, healthcheck_requested, probe_readiness};

    #[test]
    fn healthcheck_mode_is_explicit() {
        assert!(healthcheck_requested(Some(OsStr::new("--healthcheck"))));
        assert!(!healthcheck_requested(Some(OsStr::new("serve"))));
        assert!(!healthcheck_requested(None));
    }

    #[test]
    fn healthcheck_address_preserves_specific_bindings_and_maps_wildcards_to_loopback(
    ) -> anyhow::Result<()> {
        assert_eq!(
            healthcheck_address("0.0.0.0:8080".parse()?),
            "127.0.0.1:8080".parse()?
        );
        assert_eq!(
            healthcheck_address("[::]:8080".parse()?),
            "[::1]:8080".parse()?
        );
        assert_eq!(
            healthcheck_address("192.0.2.10:8080".parse()?),
            "192.0.2.10:8080".parse()?
        );
        Ok(())
    }

    #[test]
    fn native_probe_requires_ready_http_status() -> anyhow::Result<()> {
        for (status, expected) in [("200 OK", true), ("503 Service Unavailable", false)] {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            let address = listener.local_addr()?;
            let server = std::thread::spawn(move || -> std::io::Result<()> {
                let (mut stream, _) = listener.accept()?;
                let mut reader = BufReader::new(&mut stream);
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line)? == 0 || line == "\r\n" {
                        break;
                    }
                }
                drop(reader);
                write!(stream, "HTTP/1.1 {status}\r\nContent-Length: 0\r\n\r\n")?;
                stream.flush()
            });

            let result = probe_readiness(address, "/readyz");
            server
                .join()
                .map_err(|_| anyhow::anyhow!("health server panicked"))??;
            assert_eq!(result.is_ok(), expected, "probe result: {result:?}");
        }
        Ok(())
    }
}
