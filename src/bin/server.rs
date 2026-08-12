use std::fs::OpenOptions;
use std::io::Write as _;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use mdns_sd::{ServiceDaemon, ServiceInfo};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::time::timeout;

use spark_training_monitor::protocol::{
    CHECKIN_RESPONSE, HTTP_PORT, HTTP_PORT_TXT_KEY, INSTANCE_NAME, PORT, SERVICE_TYPE,
};
use spark_training_monitor::status::{GpuStat, StatusSnapshot, TrainingReport};

const MAX_SETUP_ATTEMPTS: u32 = 5;
const MAX_CONCURRENT_CHECKINS: usize = 8;
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const ACCEPT_BACKOFF_START: Duration = Duration::from_millis(10);
const ACCEPT_BACKOFF_MAX: Duration = Duration::from_secs(1);
/// nvidia-smi costs a fork/exec (100-500ms); a polling dashboard must not
/// pay it per request.
const GPU_CACHE_TTL: Duration = Duration::from_secs(2);
const BIND_ADDR_ENV_VAR: &str = "SPARK_MONITOR_BIND";
const LOG_FILE_ENV_VAR: &str = "SPARK_MONITOR_LOG_FILE";
const INSTANCE_ENV_VAR: &str = "SPARK_MONITOR_INSTANCE";
const HTTP_PORT_ENV_VAR: &str = "SPARK_MONITOR_HTTP_PORT";

/// Latest training metrics pushed by a local training script, shared
/// between the POST /training and GET /status handlers.
type SharedTraining = Arc<Mutex<Option<TrainingReport>>>;

#[derive(Clone)]
struct ApiState {
    instance: String,
    hostname: String,
    training: SharedTraining,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let raw_host = hostname::get()?.to_string_lossy().to_string();
    let host_name = normalize_local_hostname(&raw_host);
    let instance_name = resolve_instance_name(std::env::var(INSTANCE_ENV_VAR).ok());

    let http_port = resolve_http_port(std::env::var(HTTP_PORT_ENV_VAR).ok());

    let mdns = setup_with_retry("mDNS daemon startup", ServiceDaemon::new);
    let http_port_str = http_port.to_string();
    let service_info = ServiceInfo::new(
        SERVICE_TYPE,
        &instance_name,
        &host_name,
        (),
        PORT,
        &[
            ("role", "training-host"),
            (HTTP_PORT_TXT_KEY, http_port_str.as_str()),
        ][..],
    )?
    .enable_addr_auto();

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

    // HTTP status API: GET /status (system + GPU + training snapshot),
    // POST /training (training scripts push their latest metrics).
    let api_state = ApiState {
        instance: instance_name.clone(),
        hostname: raw_host.clone(),
        training: Arc::new(Mutex::new(None)),
    };
    let http_listener = setup_with_retry_async("HTTP bind", || {
        TcpListener::bind((bind_addr.as_str(), http_port))
    })
    .await;
    let app = Router::new()
        .route("/status", get(handle_status))
        .route("/training", post(handle_training))
        .with_state(api_state);
    tokio::spawn(async move {
        if let Err(e) = axum::serve(http_listener, app).await {
            log_err(&format!("[spark-monitor-server] HTTP server exited: {e}"));
        }
    });
    log(&format!(
        "[spark-monitor-server] status API listening on port {http_port} (bind: {bind_addr})"
    ));

    // Registered only after both listeners are bound: a bind retry backoff
    // can take ~15s, and advertising before then points clients at a port
    // nothing answers on.
    mdns.register(service_info)?;
    log(&format!(
        "[spark-monitor-server] advertising {SERVICE_TYPE} as '{instance_name}' on {host_name}:{PORT}"
    ));

    // Bounds concurrent in-flight check-in handlers so a flood of
    // connections from any LAN device can't exhaust threads/resources.
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_CHECKINS));

    // Sticky accept errors (EMFILE/ENFILE) return immediately every
    // iteration; backing off keeps that from becoming a hot spin loop.
    let mut accept_backoff = Duration::ZERO;

    loop {
        match listener.accept().await {
            Ok((mut socket, peer)) => {
                accept_backoff = Duration::ZERO;
                match Arc::clone(&semaphore).try_acquire_owned() {
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
                }
            }
            Err(e) => {
                log_err(&format!("[spark-monitor-server] connection error: {e}"));
                if !accept_backoff.is_zero() {
                    tokio::time::sleep(accept_backoff).await;
                }
                accept_backoff = next_accept_backoff(accept_backoff);
            }
        }
    }
}

/// Escalating accept-error backoff: first error retries immediately, then
/// 10ms doubling up to a 1s cap.
fn next_accept_backoff(current: Duration) -> Duration {
    if current.is_zero() {
        ACCEPT_BACKOFF_START
    } else {
        (current * 2).min(ACCEPT_BACKOFF_MAX)
    }
}

async fn handle_status(State(state): State<ApiState>) -> Json<StatusSnapshot> {
    let training = state
        .training
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    // collect_status blocks on /proc reads and (on a cache miss) a
    // nvidia-smi fork/exec, so it must not run on a worker thread.
    let snapshot = tokio::task::spawn_blocking(move || {
        collect_status(&state.instance, &state.hostname, training)
    })
    .await
    .expect("collect_status must not panic");
    Json(snapshot)
}

async fn handle_training(
    State(state): State<ApiState>,
    Json(metrics): Json<serde_json::Value>,
) -> (StatusCode, Json<serde_json::Value>) {
    if !metrics.is_object() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "expected a JSON object of training metrics"})),
        );
    }
    let report = TrainingReport {
        received_at: chrono::Local::now().to_rfc3339(),
        metrics,
    };
    *state
        .training
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(report);
    (StatusCode::OK, Json(serde_json::json!({"status": "ok"})))
}

/// Builds the full status snapshot from /proc and nvidia-smi. Every
/// collector degrades to None/empty rather than failing the request, so a
/// host without a GPU (or with a hidden /proc) still reports what it can.
fn collect_status(
    instance: &str,
    hostname: &str,
    training: Option<TrainingReport>,
) -> StatusSnapshot {
    // Read once; both fields come out of the same snapshot.
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok();
    StatusSnapshot {
        instance: instance.to_string(),
        hostname: hostname.to_string(),
        timestamp: chrono::Local::now().to_rfc3339(),
        uptime_secs: std::fs::read_to_string("/proc/uptime")
            .ok()
            .and_then(|s| parse_uptime(&s)),
        load_avg: std::fs::read_to_string("/proc/loadavg")
            .ok()
            .and_then(|s| parse_loadavg(&s)),
        mem_total_kb: meminfo
            .as_deref()
            .and_then(|m| parse_meminfo_field(m, "MemTotal")),
        mem_available_kb: meminfo
            .as_deref()
            .and_then(|m| parse_meminfo_field(m, "MemAvailable")),
        gpus: cached_gpus(),
        training,
    }
}

fn parse_uptime(contents: &str) -> Option<f64> {
    contents.split_whitespace().next()?.parse().ok()
}

fn parse_loadavg(contents: &str) -> Option<[f64; 3]> {
    let mut parts = contents.split_whitespace();
    Some([
        parts.next()?.parse().ok()?,
        parts.next()?.parse().ok()?,
        parts.next()?.parse().ok()?,
    ])
}

fn parse_meminfo_field(meminfo: &str, field: &str) -> Option<u64> {
    meminfo.lines().find_map(|line| {
        let rest = line.strip_prefix(field)?.strip_prefix(':')?;
        rest.split_whitespace().next()?.parse().ok()
    })
}

/// Serves `query_gpus()` from a `GPU_CACHE_TTL` memo so concurrent or
/// rapid /status polls share one nvidia-smi invocation.
fn cached_gpus() -> Vec<GpuStat> {
    static CACHE: Mutex<Option<(Instant, Vec<GpuStat>)>> = Mutex::new(None);

    let mut cache = CACHE.lock().unwrap_or_else(|p| p.into_inner());
    if let Some((fetched_at, gpus)) = cache.as_ref() {
        if fetched_at.elapsed() < GPU_CACHE_TTL {
            return gpus.clone();
        }
    }
    let gpus = query_gpus();
    *cache = Some((Instant::now(), gpus.clone()));
    gpus
}

fn query_gpus() -> Vec<GpuStat> {
    let output = std::process::Command::new("nvidia-smi")
        .args([
            "--query-gpu=index,name,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw",
            "--format=csv,noheader,nounits",
        ])
        .output();
    match output {
        Ok(out) if out.status.success() => parse_gpu_csv(&String::from_utf8_lossy(&out.stdout)),
        _ => Vec::new(),
    }
}

/// Parses `nvidia-smi --format=csv,noheader,nounits` output. Fields the
/// driver can't report come back as "[Not Supported]"/"[N/A]" and map to
/// None instead of dropping the whole GPU row.
fn parse_gpu_csv(csv: &str) -> Vec<GpuStat> {
    csv.lines()
        .filter_map(|line| {
            let fields: Vec<&str> = line.split(',').map(str::trim).collect();
            if fields.len() != 7 {
                return None;
            }
            Some(GpuStat {
                index: fields[0].parse().ok()?,
                name: fields[1].to_string(),
                utilization_pct: fields[2].parse().ok(),
                mem_used_mib: fields[3].parse().ok(),
                mem_total_mib: fields[4].parse().ok(),
                temperature_c: fields[5].parse().ok(),
                power_w: fields[6].parse().ok(),
            })
        })
        .collect()
}

/// Port 0 is rejected: it would bind an ephemeral port while the mDNS TXT
/// record still advertised 0, sending clients to http://ip:0/.
fn resolve_http_port(env_value: Option<String>) -> u16 {
    env_value
        .and_then(|v| v.trim().parse().ok())
        .filter(|p| *p != 0)
        .unwrap_or(HTTP_PORT)
}

/// Suffixes are stripped once each, not repeatedly: "spark.local.local"
/// keeps one "local" label, and a host literally named "local" survives.
fn normalize_local_hostname(raw_host: &str) -> String {
    let trimmed = raw_host.trim();
    let stem = trimmed.strip_suffix('.').unwrap_or(trimmed);
    let stem = stem.strip_suffix(".local").unwrap_or(stem);
    let stem = if stem.is_empty() { trimmed } else { stem };
    format!("{stem}.local.")
}

/// The mDNS instance name this server advertises under. Defaults to
/// `INSTANCE_NAME` ("spark"); `SPARK_MONITOR_INSTANCE` overrides it so
/// several training hosts can run the server without colliding on the
/// same mDNS instance name.
fn resolve_instance_name(env_value: Option<String>) -> String {
    env_value
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| INSTANCE_NAME.to_string())
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

/// The env var is read and the file opened once, at first use: log lines
/// are emitted per check-in error and must not re-open the file each time.
/// Unset (or unopenable) means no file logging for the process lifetime.
fn append_to_log_file(msg: &str) {
    static FILE: OnceLock<Option<Mutex<std::fs::File>>> = OnceLock::new();

    let handle = FILE.get_or_init(|| {
        let path = std::env::var(LOG_FILE_ENV_VAR).ok()?;
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .ok()
            .map(Mutex::new)
    });
    if let Some(file) = handle {
        let mut f = file.lock().unwrap_or_else(|p| p.into_inner());
        let _ = writeln!(f, "{msg}");
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

    #[test]
    fn strips_local_suffix_only_once() {
        assert_eq!(
            normalize_local_hostname("spark.local.local"),
            "spark.local.local."
        );
        assert_eq!(normalize_local_hostname("local"), "local.local.");
    }

    #[test]
    fn instance_name_defaults_to_spark() {
        assert_eq!(resolve_instance_name(None), "spark");
        assert_eq!(resolve_instance_name(Some("  ".to_string())), "spark");
    }

    #[test]
    fn instance_name_honors_env_override() {
        assert_eq!(resolve_instance_name(Some("spark2".to_string())), "spark2");
    }

    #[test]
    fn http_port_defaults_and_overrides() {
        assert_eq!(resolve_http_port(None), HTTP_PORT);
        assert_eq!(resolve_http_port(Some("9000".to_string())), 9000);
        assert_eq!(resolve_http_port(Some("junk".to_string())), HTTP_PORT);
        assert_eq!(resolve_http_port(Some("0".to_string())), HTTP_PORT);
    }

    #[test]
    fn accept_backoff_escalates_and_caps() {
        let first = next_accept_backoff(Duration::ZERO);
        assert_eq!(first, ACCEPT_BACKOFF_START);
        assert_eq!(next_accept_backoff(first), first * 2);
        assert_eq!(next_accept_backoff(ACCEPT_BACKOFF_MAX), ACCEPT_BACKOFF_MAX);
    }

    #[test]
    fn parses_uptime_and_loadavg() {
        assert_eq!(parse_uptime("12345.67 99999.99\n"), Some(12345.67));
        assert_eq!(
            parse_loadavg("0.52 1.10 2.00 2/1234 5678\n"),
            Some([0.52, 1.10, 2.00])
        );
        assert_eq!(parse_loadavg("garbage\n"), None);
    }

    #[test]
    fn parses_meminfo_field() {
        let meminfo = "MemTotal:       131072000 kB\nMemFree:  1000 kB\nMemAvailable:   98304000 kB\n";
        assert_eq!(parse_meminfo_field(meminfo, "MemTotal"), Some(131072000));
        assert_eq!(parse_meminfo_field(meminfo, "MemAvailable"), Some(98304000));
        assert_eq!(parse_meminfo_field(meminfo, "SwapTotal"), None);
    }

    #[test]
    fn parses_gpu_csv_rows() {
        let csv = "0, NVIDIA GB10, 87, 102400, 131072, 71, 95.30\n";
        let gpus = parse_gpu_csv(csv);
        assert_eq!(gpus.len(), 1);
        assert_eq!(gpus[0].index, 0);
        assert_eq!(gpus[0].name, "NVIDIA GB10");
        assert_eq!(gpus[0].utilization_pct, Some(87.0));
        assert_eq!(gpus[0].mem_total_mib, Some(131072.0));
        assert_eq!(gpus[0].power_w, Some(95.30));
    }

    #[test]
    fn gpu_csv_maps_unsupported_fields_to_none() {
        let csv = "0, NVIDIA GB10, [Not Supported], 1024, 131072, [N/A], [N/A]\n";
        let gpus = parse_gpu_csv(csv);
        assert_eq!(gpus.len(), 1);
        assert_eq!(gpus[0].utilization_pct, None);
        assert_eq!(gpus[0].temperature_c, None);
        assert_eq!(gpus[0].mem_used_mib, Some(1024.0));
    }

    #[test]
    fn gpu_csv_skips_malformed_lines() {
        assert!(parse_gpu_csv("not,a,gpu\n\n").is_empty());
    }

    fn test_state() -> ApiState {
        ApiState {
            instance: "spark-test".to_string(),
            hostname: "testhost".to_string(),
            training: Arc::new(Mutex::new(None)),
        }
    }

    #[tokio::test]
    async fn training_rejects_non_object_payloads() {
        for payload in [serde_json::json!([1, 2, 3]), serde_json::json!(42)] {
            let (status, Json(body)) =
                handle_training(State(test_state()), Json(payload)).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(body.get("error").is_some());
        }
    }

    #[tokio::test]
    async fn training_object_round_trips_into_status() {
        let state = test_state();
        let metrics = serde_json::json!({"loss": 0.125, "step": 42, "tags": ["a"]});
        let (status, _) =
            handle_training(State(state.clone()), Json(metrics.clone())).await;
        assert_eq!(status, StatusCode::OK);

        let Json(snapshot) = handle_status(State(state)).await;
        let training = snapshot.training.expect("training report stored");
        assert_eq!(training.metrics, metrics);
        assert!(!training.received_at.is_empty());
    }

    #[tokio::test]
    async fn status_reports_configured_identity() {
        let Json(snapshot) = handle_status(State(test_state())).await;
        assert_eq!(snapshot.instance, "spark-test");
        assert_eq!(snapshot.hostname, "testhost");
        assert!(snapshot.training.is_none());
    }
}
