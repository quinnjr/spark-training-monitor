use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use mdns_sd::{ServiceDaemon, ServiceEvent};
use notify_rust::{Notification, Timeout};
use serde::Serialize;

use spark_training_monitor::protocol::{
    CHECKIN_RESPONSE, HTTP_PORT, HTTP_PORT_TXT_KEY, SERVICE_TYPE,
};

const CHECK_INTERVAL: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(3);
const STATUS_FETCH_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_DAEMON_SETUP_ATTEMPTS: u32 = 5;
const LOG_FILE_ENV_VAR: &str = "SPARK_MONITOR_LOG_FILE";
const API_BIND_ENV_VAR: &str = "SPARK_MONITOR_API_BIND";
const DEFAULT_API_BIND: &str = "127.0.0.1:7881";

/// A resolved server instance: which mDNS instance it came from (so
/// `ServiceRemoved` for an unrelated instance can't wipe out a live target)
/// and where to reach it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Discovered {
    fullname: String,
    addr: SocketAddr,
    /// Port of the host's HTTP status API, from the mDNS TXT record
    /// (`http_port`), falling back to the protocol default.
    http_port: u16,
}

/// All currently discovered hosts, keyed by mDNS fullname.
type SharedTargets = Arc<Mutex<HashMap<String, Discovered>>>;

/// What the REST API serves for one registered device: liveness from the
/// TCP check-in plus the latest `/status` snapshot fetched from the host.
#[derive(Clone, Debug, Serialize)]
struct DeviceRecord {
    name: String,
    fullname: String,
    addr: String,
    /// Port of the host's own HTTP status API (`/status`, `/training`).
    http_port: u16,
    online: bool,
    last_checkin: Option<String>,
    last_error: Option<String>,
    status: Option<serde_json::Value>,
}

/// Devices as served by the REST API, keyed by mDNS fullname. Updated by
/// the check-in loop, read by the API thread.
type SharedRecords = Arc<Mutex<HashMap<String, DeviceRecord>>>;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mdns = setup_with_retry("mDNS daemon startup", ServiceDaemon::new);
    let receiver = setup_with_retry("mDNS browse", || mdns.browse(SERVICE_TYPE));

    let targets: SharedTargets = Arc::new(Mutex::new(HashMap::new()));
    // The discovery thread gets its own daemon handle so it can re-browse
    // after a channel close; `mdns` stays alive here for the process lifetime.
    spawn_discovery_thread(mdns.clone(), receiver, Arc::clone(&targets));

    let records: SharedRecords = Arc::new(Mutex::new(HashMap::new()));
    spawn_api_thread(Arc::clone(&records));

    log(&format!(
        "[spark-monitor-client] waiting to discover spark hosts via mDNS ({SERVICE_TYPE})..."
    ));

    let notifier = DesktopNotifier;
    let mut states: HashMap<String, NotifyState> = HashMap::new();

    loop {
        // Fixed-rate schedule: the next wake is a deadline from the start of
        // the cycle, so the work done below doesn't stretch the interval.
        let next = Instant::now() + CHECK_INTERVAL;

        let mut discovered: Vec<Discovered> =
            lock_targets(&targets).values().cloned().collect();
        discovered.sort_by(|a, b| a.fullname.cmp(&b.fullname));

        if discovered.is_empty() {
            log(&format!(
                "[{}] no spark hosts discovered on mDNS",
                now_str()
            ));
        }

        for (host, result, status) in check_in_all(&discovered) {
            process_result(&mut states, &records, host, result, status, &notifier);
        }

        // Hosts we were tracking that are no longer advertised: report the
        // disappearance once, then forget them (a reappearance starts with a
        // fresh notify state).
        reap_disappeared(&mut states, &discovered, &notifier);
        reap_records(&records, &discovered);

        std::thread::sleep(next.saturating_duration_since(Instant::now()));
    }
}

/// Handles one host's check-in outcome: logs it, decides whether the
/// transition warrants a notification, and folds the result into the records
/// the REST API serves.
fn process_result(
    states: &mut HashMap<String, NotifyState>,
    records: &SharedRecords,
    host: &Discovered,
    result: Result<(), String>,
    status: Option<serde_json::Value>,
    notifier: &impl Notifier,
) {
    let label = instance_label(&host.fullname);
    let state = states.entry(host.fullname.clone()).or_default();
    match result {
        Ok(()) => {
            log(&format!(
                "[{}] check-in OK: '{label}' is active and putzing along ({})",
                now_str(),
                host.addr
            ));
            if let Some((summary, body)) = state.record_success(label, host.addr) {
                notifier.notify(&summary, &body);
            }
            update_record(records, host, Ok(()), status);
        }
        Err(e) => {
            log(&format!(
                "[{}] check-in FAILED for '{label}': {e}",
                now_str()
            ));
            if let Some((summary, body)) =
                state.record_failure(label, format!("{label} check-in failed: {e}"))
            {
                notifier.notify(&summary, &body);
            }
            update_record(records, host, Err(e), None);
        }
    }
}

/// Drops notify states for hosts that are no longer discovered, sending one
/// final "not found" notification per disappeared host.
fn reap_disappeared(
    states: &mut HashMap<String, NotifyState>,
    discovered: &[Discovered],
    notifier: &impl Notifier,
) {
    states.retain(|fullname, state| {
        if discovered.iter().any(|d| &d.fullname == fullname) {
            return true;
        }
        let label = instance_label(fullname);
        if let Some((summary, body)) = state.record_failure(
            label,
            format!("{label} not found on the network (mDNS discovery lost it)"),
        ) {
            notifier.notify(&summary, &body);
        }
        false
    });
}

/// Fetches the host's `/status` snapshot from its HTTP API. Returns None on
/// any failure — liveness is already covered by the TCP check-in, so a
/// missing snapshot just means the dashboard shows the device without
/// metrics (e.g. an older server binary without the HTTP API).
fn fetch_status(host: &Discovered) -> Option<serde_json::Value> {
    let status_addr = SocketAddr::new(host.addr.ip(), host.http_port);
    let url = format!("http://{status_addr}/status");
    match ureq::get(&url).timeout(STATUS_FETCH_TIMEOUT).call() {
        Ok(resp) => match resp.into_json::<serde_json::Value>() {
            Ok(json) => Some(json),
            Err(e) => {
                log_err(&format!(
                    "[spark-monitor-client] bad /status payload from {status_addr}: {e}"
                ));
                None
            }
        },
        Err(e) => {
            log_err(&format!(
                "[spark-monitor-client] failed to fetch {url}: {e}"
            ));
            None
        }
    }
}

/// Records the outcome of one check-in cycle for a host so the REST API can
/// serve it. On failure the previous status snapshot is kept (stale data
/// beats no data for a host that's flapping) but `online` flips to false.
fn update_record(
    records: &SharedRecords,
    host: &Discovered,
    result: Result<(), String>,
    status: Option<serde_json::Value>,
) {
    let mut guard = lock_records(records);
    let record = guard
        .entry(host.fullname.clone())
        .or_insert_with(|| DeviceRecord {
            name: instance_label(&host.fullname).to_string(),
            fullname: host.fullname.clone(),
            addr: host.addr.to_string(),
            http_port: host.http_port,
            online: false,
            last_checkin: None,
            last_error: None,
            status: None,
        });
    record.addr = host.addr.to_string();
    record.http_port = host.http_port;
    match result {
        Ok(()) => {
            record.online = true;
            record.last_checkin = Some(chrono::Local::now().to_rfc3339());
            record.last_error = None;
            if status.is_some() {
                record.status = status;
            }
        }
        Err(e) => {
            record.online = false;
            record.last_error = Some(e);
        }
    }
}

/// Drops records for hosts that are no longer advertised on mDNS, matching
/// `reap_disappeared`'s bookkeeping for notifications.
fn reap_records(records: &SharedRecords, discovered: &[Discovered]) {
    lock_records(records)
        .retain(|fullname, _| discovered.iter().any(|d| &d.fullname == fullname));
}

fn lock_records(records: &SharedRecords) -> MutexGuard<'_, HashMap<String, DeviceRecord>> {
    records.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Runs the REST API on a dedicated thread with its own single-threaded
/// tokio runtime, leaving the sync check-in loop untouched.
fn spawn_api_thread(records: SharedRecords) {
    std::thread::spawn(move || {
        let bind =
            std::env::var(API_BIND_ENV_VAR).unwrap_or_else(|_| DEFAULT_API_BIND.to_string());
        let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
            Ok(rt) => rt,
            Err(e) => {
                log_err(&format!(
                    "[spark-monitor-client] failed to start API runtime: {e}"
                ));
                return;
            }
        };
        rt.block_on(async move {
            let app = Router::new()
                .route("/api/health", get(api_health))
                .route("/api/devices", get(api_devices))
                .route("/api/devices/{name}", get(api_device))
                .layer(tower_http::cors::CorsLayer::permissive())
                .with_state(records);
            let listener = match tokio::net::TcpListener::bind(&bind).await {
                Ok(l) => l,
                Err(e) => {
                    log_err(&format!(
                        "[spark-monitor-client] REST API failed to bind {bind}: {e}"
                    ));
                    return;
                }
            };
            log(&format!("[spark-monitor-client] REST API listening on http://{bind}"));
            if let Err(e) = axum::serve(listener, app).await {
                log_err(&format!("[spark-monitor-client] REST API exited: {e}"));
            }
        });
    });
}

async fn api_health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

/// All registered devices, sorted by name for a stable dashboard order.
async fn api_devices(State(records): State<SharedRecords>) -> Json<Vec<DeviceRecord>> {
    let mut devices: Vec<DeviceRecord> = lock_records(&records).values().cloned().collect();
    devices.sort_by(|a, b| a.name.cmp(&b.name));
    Json(devices)
}

async fn api_device(
    State(records): State<SharedRecords>,
    Path(name): Path<String>,
) -> Result<Json<DeviceRecord>, (StatusCode, Json<serde_json::Value>)> {
    lock_records(&records)
        .values()
        .find(|d| d.name == name)
        .cloned()
        .map(Json)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": format!("no device named '{name}'")})),
            )
        })
}

/// Connects to `addr` and verifies the server actually sends back the
/// expected check-in payload, rather than treating a bare TCP accept as a
/// full health signal.
fn check_in(addr: SocketAddr) -> Result<(), String> {
    let mut stream = TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT).map_err(|e| e.to_string())?;
    stream
        .set_read_timeout(Some(READ_TIMEOUT))
        .map_err(|e| e.to_string())?;
    let mut buf = [0u8; CHECKIN_RESPONSE.len()];
    stream
        .read_exact(&mut buf)
        .map_err(|e| format!("no valid check-in response: {e}"))?;
    if buf == CHECKIN_RESPONSE {
        Ok(())
    } else {
        Err(format!("unexpected check-in response: {buf:?}"))
    }
}

/// Checks in with every discovered host concurrently (one scoped thread per
/// host, each with its own timeouts) so several unreachable hosts can't
/// stretch the check interval serially. Each worker also fetches the host's
/// `/status` snapshot right after a successful check-in, keeping that HTTP
/// round trip off the serial path too; the snapshot is `None` when the
/// check-in failed or the fetch didn't produce usable JSON.
fn check_in_all(
    discovered: &[Discovered],
) -> Vec<(&Discovered, Result<(), String>, Option<serde_json::Value>)> {
    std::thread::scope(|s| {
        discovered
            .iter()
            .map(|d| {
                (
                    d,
                    s.spawn(move || match check_in(d.addr) {
                        Ok(()) => (Ok(()), fetch_status(d)),
                        Err(e) => (Err(e), None),
                    }),
                )
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|(d, handle)| {
                let (result, status) = handle
                    .join()
                    .unwrap_or_else(|_| (Err("check-in worker panicked".to_string()), None));
                (d, result, status)
            })
            .collect()
    })
}

/// Tracks notify-once-per-streak state for one host across
/// connect/disconnect transitions. Kept free of I/O so it can be unit
/// tested without a network or D-Bus session.
#[derive(Default)]
struct NotifyState {
    already_notified: bool,
    was_connected: bool,
}

impl NotifyState {
    fn record_success(&mut self, label: &str, addr: SocketAddr) -> Option<(String, String)> {
        let msg = if !self.was_connected {
            Some((
                format!("spark host '{label}' connected"),
                format!("{label} is active at {addr}"),
            ))
        } else {
            None
        };
        self.was_connected = true;
        self.already_notified = false;
        msg
    }

    fn record_failure(&mut self, label: &str, reason: String) -> Option<(String, String)> {
        let msg = if !self.already_notified {
            Some((format!("spark host '{label}' check-in failed"), reason))
        } else {
            None
        };
        self.already_notified = true;
        self.was_connected = false;
        msg
    }
}

/// Picks an address to connect to, preferring IPv4: link-local IPv6
/// addresses lose their scope id once converted, making them unconnectable.
fn pick_addr(addrs: &[IpAddr], port: u16) -> Option<SocketAddr> {
    addrs
        .iter()
        .copied()
        .find(|ip| ip.is_ipv4())
        .or_else(|| addrs.first().copied())
        .map(|ip| SocketAddr::new(ip, port))
}

/// Display name for a discovered host: the mDNS instance name with the
/// service-type suffix stripped, e.g. "spark" out of
/// "spark._spark-monitor._tcp.local.".
fn instance_label(fullname: &str) -> &str {
    fullname
        .strip_suffix(SERVICE_TYPE)
        .and_then(|prefix| prefix.strip_suffix('.'))
        .unwrap_or(fullname)
}

/// Owns a clone of the mDNS daemon handle so a closed event channel is
/// recoverable: targets are cleared and the browse is retried with backoff
/// until a fresh receiver arrives, rather than discovery stopping for good.
fn spawn_discovery_thread(
    mdns: ServiceDaemon,
    receiver: mdns_sd::Receiver<ServiceEvent>,
    targets: SharedTargets,
) {
    std::thread::spawn(move || {
        let mut receiver = receiver;
        loop {
            run_discovery_loop(&receiver, &targets);
            // The discovery channel closed (mDNS daemon died, was dropped, or
            // the browse was stopped). Forget all targets so the main loop
            // starts reporting failures instead of silently trusting stale
            // cached addresses, then try to get a fresh receiver.
            log_err("[spark-monitor-client] mDNS discovery channel closed — discovery has stopped");
            lock_targets(&targets).clear();
            receiver = rebrowse_with_backoff(&mdns);
            log("[spark-monitor-client] mDNS browse re-established — discovery resumed");
        }
    });
}

/// Retries `browse` forever, backing off from 1s up to a 60s cap.
fn rebrowse_with_backoff(mdns: &ServiceDaemon) -> mdns_sd::Receiver<ServiceEvent> {
    let mut delay = Duration::from_secs(1);
    loop {
        std::thread::sleep(delay);
        match mdns.browse(SERVICE_TYPE) {
            Ok(receiver) => return receiver,
            Err(e) => {
                log_err(&format!(
                    "[spark-monitor-client] mDNS re-browse failed: {e}; retrying in {delay:?}"
                ));
                delay = (delay * 2).min(Duration::from_secs(60));
            }
        }
    }
}

/// Consumes discovery events until the channel closes.
fn run_discovery_loop(receiver: &mdns_sd::Receiver<ServiceEvent>, targets: &SharedTargets) {
    while let Ok(event) = receiver.recv() {
        match event {
            ServiceEvent::ServiceResolved(info) => {
                // Every instance of the service type is a host to
                // monitor; there is no single expected instance name.
                let addrs: Vec<IpAddr> = info.addresses.iter().map(|a| a.to_ip_addr()).collect();
                let Some(addr) = pick_addr(&addrs, info.port) else {
                    continue;
                };
                let http_port = info
                    .get_property_val_str(HTTP_PORT_TXT_KEY)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(HTTP_PORT);
                let label = instance_label(&info.fullname).to_string();
                let mut guard = lock_targets(targets);
                if guard
                    .get(&info.fullname)
                    .is_some_and(|d| d.addr == addr && d.http_port == http_port)
                {
                    continue; // already tracking this host at this address
                }
                log(&format!(
                    "[spark-monitor-client] discovered '{label}' at {addr}"
                ));
                guard.insert(
                    info.fullname.clone(),
                    Discovered {
                        fullname: info.fullname,
                        addr,
                        http_port,
                    },
                );
            }
            ServiceEvent::ServiceRemoved(_, fullname) => {
                if lock_targets(targets).remove(&fullname).is_some() {
                    log(&format!(
                        "[spark-monitor-client] host '{}' left mDNS",
                        instance_label(&fullname)
                    ));
                }
            }
            _ => {}
        }
    }
}

fn lock_targets(targets: &SharedTargets) -> MutexGuard<'_, HashMap<String, Discovered>> {
    targets.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn setup_with_retry<T, E: std::fmt::Display>(what: &str, mut f: impl FnMut() -> Result<T, E>) -> T {
    let mut delay = Duration::from_secs(1);
    for attempt in 1..=MAX_DAEMON_SETUP_ATTEMPTS {
        match f() {
            Ok(v) => return v,
            Err(e) if attempt < MAX_DAEMON_SETUP_ATTEMPTS => {
                log_err(&format!(
                    "[spark-monitor-client] {what} failed (attempt {attempt}/{MAX_DAEMON_SETUP_ATTEMPTS}): {e}; retrying in {delay:?}"
                ));
                std::thread::sleep(delay);
                delay *= 2;
            }
            Err(e) => {
                log_err(&format!("[spark-monitor-client] {what} failed permanently: {e}"));
                std::process::exit(1);
            }
        }
    }
    unreachable!()
}

/// Seam separating "decide to notify" from "how a notification is actually
/// delivered", so the decision logic (`NotifyState`) never has to touch
/// D-Bus, and any future test double can swap in for `DesktopNotifier`.
trait Notifier {
    fn notify(&self, summary: &str, body: &str);
}

struct DesktopNotifier;

impl Notifier for DesktopNotifier {
    fn notify(&self, summary: &str, body: &str) {
        if let Err(e) = Notification::new()
            .summary(summary)
            .body(body)
            .appname("spark-training-monitor")
            .timeout(Timeout::Never)
            .show()
        {
            log_err(&format!(
                "[spark-monitor-client] failed to send desktop notification: {e}"
            ));
        }
    }
}

fn now_str() -> String {
    chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

/// Prints to stdout and, if `SPARK_MONITOR_LOG_FILE` is set, also appends to
/// that file — so diagnostics survive an unattended process's stdio being
/// discarded (e.g. run outside a supervisor that captures it).
fn log(msg: &str) {
    println!("{msg}");
    append_to_log_file(msg);
}

fn log_err(msg: &str) {
    eprintln!("{msg}");
    append_to_log_file(msg);
}

/// The log file handle, resolved from `SPARK_MONITOR_LOG_FILE` and opened on
/// first use: `None` when the variable is unset or the file can't be opened.
static LOG_FILE: std::sync::OnceLock<Option<Mutex<std::fs::File>>> = std::sync::OnceLock::new();

fn append_to_log_file(msg: &str) {
    let file = LOG_FILE.get_or_init(|| {
        let path = std::env::var(LOG_FILE_ENV_VAR).ok()?;
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .ok()
            .map(Mutex::new)
    });
    if let Some(file) = file {
        let mut f = file.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let _ = writeln!(f, "{msg}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn v4(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(a, b, c, d))
    }

    fn v6() -> IpAddr {
        IpAddr::V6(Ipv6Addr::LOCALHOST)
    }

    #[test]
    fn pick_addr_prefers_ipv4_over_ipv6() {
        let ip4 = v4(192, 168, 1, 10);
        assert_eq!(
            pick_addr(&[v6(), ip4], 7879),
            Some(SocketAddr::new(ip4, 7879))
        );
    }

    #[test]
    fn pick_addr_falls_back_to_first_when_no_ipv4() {
        assert_eq!(pick_addr(&[v6()], 7879), Some(SocketAddr::new(v6(), 7879)));
    }

    #[test]
    fn pick_addr_returns_none_when_empty() {
        assert_eq!(pick_addr(&[], 7879), None);
    }

    fn sock() -> SocketAddr {
        SocketAddr::new(v4(127, 0, 0, 1), 7879)
    }

    #[test]
    fn notify_state_notifies_once_on_repeated_success() {
        let mut state = NotifyState::default();
        assert!(state.record_success("spark", sock()).is_some());
        assert!(state.record_success("spark", sock()).is_none());
    }

    #[test]
    fn notify_state_notifies_once_on_repeated_failure() {
        let mut state = NotifyState::default();
        assert!(state.record_failure("spark", "x".to_string()).is_some());
        assert!(state.record_failure("spark", "x".to_string()).is_none());
    }

    #[test]
    fn notify_state_rearms_after_a_flap() {
        let mut state = NotifyState::default();
        assert!(state.record_success("spark", sock()).is_some());
        assert!(state.record_failure("spark", "x".to_string()).is_some());
        assert!(state.record_success("spark", sock()).is_some());
    }

    #[test]
    fn notify_state_messages_include_host_label() {
        let mut state = NotifyState::default();
        let (summary, body) = state.record_success("spark2", sock()).unwrap();
        assert!(summary.contains("spark2"));
        assert!(body.contains("spark2"));
        let (summary, _body) = state.record_failure("spark2", "x".to_string()).unwrap();
        assert!(summary.contains("spark2"));
    }

    #[test]
    fn instance_label_strips_service_type_suffix() {
        assert_eq!(instance_label("spark._spark-monitor._tcp.local."), "spark");
        assert_eq!(
            instance_label("gpu-box-2._spark-monitor._tcp.local."),
            "gpu-box-2"
        );
    }

    #[test]
    fn instance_label_falls_back_to_fullname_when_suffix_missing() {
        assert_eq!(instance_label("something-else.local."), "something-else.local.");
    }

    #[derive(Default)]
    struct RecordingNotifier(std::cell::RefCell<Vec<(String, String)>>);

    impl Notifier for RecordingNotifier {
        fn notify(&self, summary: &str, body: &str) {
            self.0
                .borrow_mut()
                .push((summary.to_string(), body.to_string()));
        }
    }

    #[test]
    fn reap_disappeared_notifies_once_then_forgets() {
        let mut states = HashMap::new();
        states.insert(
            "spark._spark-monitor._tcp.local.".to_string(),
            NotifyState::default(),
        );
        let notifier = RecordingNotifier::default();
        reap_disappeared(&mut states, &[], &notifier);
        let sent = notifier.0.borrow();
        assert_eq!(sent.len(), 1);
        assert!(sent[0].0.contains("'spark'"));
        assert!(states.is_empty());
    }

    fn disc(fullname: &str) -> Discovered {
        Discovered {
            fullname: fullname.to_string(),
            addr: sock(),
            http_port: HTTP_PORT,
        }
    }

    #[test]
    fn update_record_marks_online_and_stores_status() {
        let records: SharedRecords = Arc::new(Mutex::new(HashMap::new()));
        let host = disc("spark._spark-monitor._tcp.local.");
        let status = serde_json::json!({"gpus": []});
        update_record(&records, &host, Ok(()), Some(status.clone()));
        let guard = lock_records(&records);
        let rec = guard.get(&host.fullname).unwrap();
        assert_eq!(rec.name, "spark");
        assert!(rec.online);
        assert!(rec.last_checkin.is_some());
        assert_eq!(rec.status, Some(status));
        assert!(rec.last_error.is_none());
    }

    #[test]
    fn update_record_failure_keeps_stale_status_but_flips_offline() {
        let records: SharedRecords = Arc::new(Mutex::new(HashMap::new()));
        let host = disc("spark._spark-monitor._tcp.local.");
        let status = serde_json::json!({"gpus": []});
        update_record(&records, &host, Ok(()), Some(status.clone()));
        update_record(&records, &host, Err("boom".to_string()), None);
        let guard = lock_records(&records);
        let rec = guard.get(&host.fullname).unwrap();
        assert!(!rec.online);
        assert_eq!(rec.last_error.as_deref(), Some("boom"));
        assert_eq!(rec.status, Some(status), "stale snapshot should survive a flap");
    }

    #[test]
    fn success_without_status_keeps_previous_snapshot() {
        let records: SharedRecords = Arc::new(Mutex::new(HashMap::new()));
        let host = disc("spark._spark-monitor._tcp.local.");
        let status = serde_json::json!({"load_avg": [1.0, 1.0, 1.0]});
        update_record(&records, &host, Ok(()), Some(status.clone()));
        update_record(&records, &host, Ok(()), None);
        assert_eq!(
            lock_records(&records).get(&host.fullname).unwrap().status,
            Some(status)
        );
    }

    #[test]
    fn reap_records_drops_hosts_no_longer_discovered() {
        let records: SharedRecords = Arc::new(Mutex::new(HashMap::new()));
        let gone = disc("gone._spark-monitor._tcp.local.");
        let kept = disc("kept._spark-monitor._tcp.local.");
        update_record(&records, &gone, Ok(()), None);
        update_record(&records, &kept, Ok(()), None);
        reap_records(&records, std::slice::from_ref(&kept));
        let guard = lock_records(&records);
        assert!(guard.contains_key(&kept.fullname));
        assert!(!guard.contains_key(&gone.fullname));
    }

    #[test]
    fn process_result_notifies_on_each_transition_and_tracks_records() {
        let records: SharedRecords = Arc::new(Mutex::new(HashMap::new()));
        let host = disc("spark._spark-monitor._tcp.local.");
        let mut states = HashMap::new();
        let notifier = RecordingNotifier::default();

        process_result(
            &mut states,
            &records,
            &host,
            Err("refused".to_string()),
            None,
            &notifier,
        );
        assert!(!lock_records(&records).get(&host.fullname).unwrap().online);

        let status = serde_json::json!({"gpus": []});
        process_result(
            &mut states,
            &records,
            &host,
            Ok(()),
            Some(status.clone()),
            &notifier,
        );
        {
            let guard = lock_records(&records);
            let rec = guard.get(&host.fullname).unwrap();
            assert!(rec.online);
            assert_eq!(rec.status, Some(status));
        }

        process_result(
            &mut states,
            &records,
            &host,
            Err("refused".to_string()),
            None,
            &notifier,
        );

        let sent = notifier.0.borrow();
        assert_eq!(sent.len(), 3, "fail → connect → fail should notify each time");
        assert!(sent[0].0.contains("check-in failed"));
        assert!(sent[1].0.contains("connected"));
        assert!(sent[2].0.contains("check-in failed"));
        let guard = lock_records(&records);
        let rec = guard.get(&host.fullname).unwrap();
        assert!(!rec.online);
        assert_eq!(rec.last_error.as_deref(), Some("refused"));
    }

    #[test]
    fn process_result_notifies_once_per_failure_streak() {
        let records: SharedRecords = Arc::new(Mutex::new(HashMap::new()));
        let host = disc("spark._spark-monitor._tcp.local.");
        let mut states = HashMap::new();
        let notifier = RecordingNotifier::default();
        for _ in 0..3 {
            process_result(
                &mut states,
                &records,
                &host,
                Err("refused".to_string()),
                None,
                &notifier,
            );
        }
        assert_eq!(notifier.0.borrow().len(), 1);
    }

    /// Spawns a one-shot TCP listener on an ephemeral port that runs `handle`
    /// on the first accepted connection, returning the address to dial.
    fn spawn_listener(handle: impl FnOnce(std::net::TcpStream) + Send + 'static) -> SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                handle(stream);
            }
        });
        addr
    }

    #[test]
    fn check_in_accepts_the_expected_response() {
        let addr = spawn_listener(|mut stream| {
            let _ = stream.write_all(b"OK\n");
            let _ = stream.flush();
        });
        assert_eq!(check_in(addr), Ok(()));
    }

    #[test]
    fn check_in_rejects_an_unexpected_response() {
        let addr = spawn_listener(|mut stream| {
            let _ = stream.write_all(b"NO\n");
            let _ = stream.flush();
        });
        let err = check_in(addr).unwrap_err();
        assert!(
            err.contains("unexpected check-in response"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn check_in_rejects_a_bare_accept() {
        let addr = spawn_listener(|stream| drop(stream));
        let err = check_in(addr).unwrap_err();
        assert!(
            err.contains("no valid check-in response"),
            "unexpected error: {err}"
        );
    }

    fn populated_records() -> SharedRecords {
        let records: SharedRecords = Arc::new(Mutex::new(HashMap::new()));
        update_record(&records, &disc("zeta._spark-monitor._tcp.local."), Ok(()), None);
        update_record(&records, &disc("alpha._spark-monitor._tcp.local."), Ok(()), None);
        records
    }

    #[tokio::test]
    async fn api_health_reports_the_crate_version() {
        let Json(body) = api_health().await;
        assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
    }

    #[tokio::test]
    async fn api_devices_returns_devices_sorted_by_name() {
        let Json(devices) = api_devices(State(populated_records())).await;
        let names: Vec<&str> = devices.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, vec!["alpha", "zeta"]);
    }

    #[tokio::test]
    async fn api_device_returns_not_found_for_an_unknown_name() {
        let result = api_device(State(populated_records()), Path("nope".to_string())).await;
        let (status, _body) = result.err().expect("unknown device should be an error");
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn reap_disappeared_keeps_still_discovered_hosts() {
        let fullname = "spark._spark-monitor._tcp.local.".to_string();
        let mut states = HashMap::new();
        states.insert(fullname.clone(), NotifyState::default());
        let discovered = vec![Discovered {
            fullname,
            addr: sock(),
            http_port: HTTP_PORT,
        }];
        let notifier = RecordingNotifier::default();
        reap_disappeared(&mut states, &discovered, &notifier);
        assert_eq!(states.len(), 1);
        assert!(notifier.0.borrow().is_empty());
    }
}
