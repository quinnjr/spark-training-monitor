use std::fs::OpenOptions;
use std::io::Write as _;
use std::sync::Arc;
use std::time::Duration;

use mdns_sd::{ServiceDaemon, ServiceInfo};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::time::timeout;

use spark_training_monitor::protocol::{CHECKIN_RESPONSE, INSTANCE_NAME, PORT, SERVICE_TYPE};

const MAX_SETUP_ATTEMPTS: u32 = 5;
const MAX_CONCURRENT_CHECKINS: usize = 8;
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const BIND_ADDR_ENV_VAR: &str = "SPARK_MONITOR_BIND";
const LOG_FILE_ENV_VAR: &str = "SPARK_MONITOR_LOG_FILE";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let raw_host = hostname::get()?.to_string_lossy().to_string();
    let host_name = normalize_local_hostname(&raw_host);

    let mdns = setup_with_retry("mDNS daemon startup", ServiceDaemon::new);
    let service_info = ServiceInfo::new(
        SERVICE_TYPE,
        INSTANCE_NAME,
        &host_name,
        (),
        PORT,
        &[("role", "training-host")][..],
    )?
    .enable_addr_auto();

    mdns.register(service_info)?;
    log(&format!(
        "[spark-monitor-server] advertising {SERVICE_TYPE} as '{INSTANCE_NAME}' on {host_name}:{PORT}"
    ));

    // "::" listens dual-stack on Linux (both IPv4 and IPv6), matching
    // addresses mDNS may advertise via enable_addr_auto(). Override with
    // SPARK_MONITOR_BIND to scope to a single interface/address if the
    // host has network paths (VPN, hotspot) this shouldn't be reachable on.
    let bind_addr = std::env::var(BIND_ADDR_ENV_VAR).unwrap_or_else(|_| "::".to_string());
    let listener =
        setup_with_retry_async("TCP bind", || TcpListener::bind((bind_addr.as_str(), PORT)))
            .await;
    log(&format!(
        "[spark-monitor-server] listening for check-ins on port {PORT} (bind: {bind_addr})"
    ));

    // Bounds concurrent in-flight check-in handlers so a flood of
    // connections from any LAN device can't exhaust threads/resources.
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_CHECKINS));

    loop {
        match listener.accept().await {
            Ok((mut socket, peer)) => match Arc::clone(&semaphore).try_acquire_owned() {
                Ok(permit) => {
                    tokio::spawn(async move {
                        let _permit = permit;
                        match timeout(WRITE_TIMEOUT, socket.write_all(CHECKIN_RESPONSE)).await {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => log_err(&format!(
                                "[spark-monitor-server] failed to write response to {peer}: {e}"
                            )),
                            Err(_) => log_err(&format!(
                                "[spark-monitor-server] write to {peer} timed out"
                            )),
                        }
                    });
                }
                Err(_) => {
                    log_err(&format!(
                        "[spark-monitor-server] too many concurrent check-ins (>{MAX_CONCURRENT_CHECKINS}), dropping connection from {peer}"
                    ));
                    // `socket` is dropped here, closing the connection.
                }
            },
            Err(e) => log_err(&format!("[spark-monitor-server] connection error: {e}")),
        }
    }
}

fn normalize_local_hostname(raw_host: &str) -> String {
    let trimmed = raw_host.trim_end_matches('.').trim_end_matches(".local");
    format!("{trimmed}.local.")
}

fn setup_with_retry<T, E: std::fmt::Display>(what: &str, mut f: impl FnMut() -> Result<T, E>) -> T {
    let mut delay = Duration::from_secs(1);
    for attempt in 1..=MAX_SETUP_ATTEMPTS {
        match f() {
            Ok(v) => return v,
            Err(e) if attempt < MAX_SETUP_ATTEMPTS => {
                log_err(&format!(
                    "[spark-monitor-server] {what} failed (attempt {attempt}/{MAX_SETUP_ATTEMPTS}): {e}; retrying in {delay:?}"
                ));
                std::thread::sleep(delay);
                delay *= 2;
            }
            Err(e) => {
                log_err(&format!("[spark-monitor-server] {what} failed permanently: {e}"));
                std::process::exit(1);
            }
        }
    }
    unreachable!()
}

async fn setup_with_retry_async<T, E, F, Fut>(what: &str, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut delay = Duration::from_secs(1);
    for attempt in 1..=MAX_SETUP_ATTEMPTS {
        match f().await {
            Ok(v) => return v,
            Err(e) if attempt < MAX_SETUP_ATTEMPTS => {
                log_err(&format!(
                    "[spark-monitor-server] {what} failed (attempt {attempt}/{MAX_SETUP_ATTEMPTS}): {e}; retrying in {delay:?}"
                ));
                tokio::time::sleep(delay).await;
                delay *= 2;
            }
            Err(e) => {
                log_err(&format!("[spark-monitor-server] {what} failed permanently: {e}"));
                std::process::exit(1);
            }
        }
    }
    unreachable!()
}

/// Prints to stdout/stderr and, if `SPARK_MONITOR_LOG_FILE` is set, also
/// appends to that file — so diagnostics survive an unattended process's
/// stdio being discarded (e.g. run outside a supervisor that captures it).
fn log(msg: &str) {
    println!("{msg}");
    append_to_log_file(msg);
}

fn log_err(msg: &str) {
    eprintln!("{msg}");
    append_to_log_file(msg);
}

fn append_to_log_file(msg: &str) {
    if let Ok(path) = std::env::var(LOG_FILE_ENV_VAR) {
        if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&path) {
            let _ = writeln!(f, "{msg}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_plain_hostname() {
        assert_eq!(normalize_local_hostname("spark"), "spark.local.");
    }

    #[test]
    fn does_not_double_up_local_suffix() {
        assert_eq!(normalize_local_hostname("spark.local"), "spark.local.");
        assert_eq!(normalize_local_hostname("spark.local."), "spark.local.");
    }
}
