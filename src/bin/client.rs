use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use mdns_sd::{ServiceDaemon, ServiceEvent};
use notify_rust::{Notification, Timeout};

use spark_training_monitor::protocol::{CHECKIN_RESPONSE, SERVICE_TYPE, expected_fullname};

const CHECK_INTERVAL: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_DAEMON_SETUP_ATTEMPTS: u32 = 5;
const LOG_FILE_ENV_VAR: &str = "SPARK_MONITOR_LOG_FILE";

/// A resolved server instance: which mDNS instance it came from (so
/// `ServiceRemoved` for an unrelated instance can't wipe out a live target)
/// and where to reach it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Discovered {
    fullname: String,
    addr: SocketAddr,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mdns = setup_with_retry("mDNS daemon startup", ServiceDaemon::new);
    let receiver = setup_with_retry("mDNS browse", || mdns.browse(SERVICE_TYPE));

    let target: Arc<Mutex<Option<Discovered>>> = Arc::new(Mutex::new(None));

    {
        let target = Arc::clone(&target);
        spawn_discovery_thread(receiver, target);
    }

    log(&format!(
        "[spark-monitor-client] waiting to discover spark via mDNS ({SERVICE_TYPE})..."
    ));

    let notifier = DesktopNotifier;
    let mut state = NotifyState::default();
    let mut first_tick = true;

    loop {
        let discovered = lock_target(&target).clone();
        match discovered {
            None => {
                log(&format!(
                    "[{}] no spark host discovered yet on mDNS",
                    now_str()
                ));
                if !first_tick {
                    if let Some((summary, body)) = state.record_failure(
                        "spark not found on the network (mDNS discovery failed)".to_string(),
                    ) {
                        notifier.notify(summary, &body);
                    }
                }
            }
            Some(Discovered { addr, .. }) => match check_in(addr) {
                Ok(()) => {
                    log(&format!(
                        "[{}] check-in OK: spark is active and putzing along ({addr})",
                        now_str()
                    ));
                    if let Some((summary, body)) = state.record_success(addr) {
                        notifier.notify(summary, &body);
                    }
                }
                Err(e) => {
                    log(&format!("[{}] check-in FAILED: {e}", now_str()));
                    if let Some((summary, body)) =
                        state.record_failure(format!("spark check-in failed: {e}"))
                    {
                        notifier.notify(summary, &body);
                    }
                }
            },
        }
        first_tick = false;
        std::thread::sleep(CHECK_INTERVAL);
    }
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

/// Tracks notify-once-per-streak state across connect/disconnect transitions.
/// Kept free of I/O so it can be unit tested without a network or D-Bus session.
#[derive(Default)]
struct NotifyState {
    already_notified: bool,
    was_connected: bool,
}

impl NotifyState {
    fn record_success(&mut self, addr: SocketAddr) -> Option<(&'static str, String)> {
        let msg = if !self.was_connected {
            Some(("spark host connected", format!("spark is active at {addr}")))
        } else {
            None
        };
        self.was_connected = true;
        self.already_notified = false;
        msg
    }

    fn record_failure(&mut self, reason: String) -> Option<(&'static str, String)> {
        let msg = if !self.already_notified {
            Some(("spark host check-in failed", reason))
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

fn spawn_discovery_thread(
    receiver: mdns_sd::Receiver<ServiceEvent>,
    target: Arc<Mutex<Option<Discovered>>>,
) {
    std::thread::spawn(move || {
        let expected = expected_fullname();
        while let Ok(event) = receiver.recv() {
            match event {
                ServiceEvent::ServiceResolved(info) => {
                    if info.fullname != expected {
                        log(&format!(
                            "[spark-monitor-client] ignoring untrusted mDNS instance '{}' (expected '{expected}')",
                            info.fullname
                        ));
                        continue;
                    }
                    let addrs: Vec<IpAddr> =
                        info.addresses.iter().map(|a| a.to_ip_addr()).collect();
                    if let Some(addr) = pick_addr(&addrs, info.port) {
                        log(&format!("[spark-monitor-client] discovered spark at {addr}"));
                        *lock_target(&target) = Some(Discovered {
                            fullname: info.fullname,
                            addr,
                        });
                    }
                }
                ServiceEvent::ServiceRemoved(_, fullname) => {
                    let mut guard = lock_target(&target);
                    if guard.as_ref().is_some_and(|d| d.fullname == fullname) {
                        log(&format!(
                            "[spark-monitor-client] spark service left mDNS: {fullname}"
                        ));
                        *guard = None;
                    }
                }
                _ => {}
            }
        }
        // The discovery channel closed for good (mDNS daemon died or was
        // dropped). Reset the target so the main loop starts reporting
        // failures instead of silently trusting a stale cached address.
        log_err("[spark-monitor-client] mDNS discovery channel closed — discovery has stopped");
        *lock_target(&target) = None;
    });
}

fn lock_target(target: &Mutex<Option<Discovered>>) -> MutexGuard<'_, Option<Discovered>> {
    target.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
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
        assert!(state.record_success(sock()).is_some());
        assert!(state.record_success(sock()).is_none());
    }

    #[test]
    fn notify_state_notifies_once_on_repeated_failure() {
        let mut state = NotifyState::default();
        assert!(state.record_failure("x".to_string()).is_some());
        assert!(state.record_failure("x".to_string()).is_none());
    }

    #[test]
    fn notify_state_rearms_after_a_flap() {
        let mut state = NotifyState::default();
        assert!(state.record_success(sock()).is_some());
        assert!(state.record_failure("x".to_string()).is_some());
        assert!(state.record_success(sock()).is_some());
    }

    #[test]
    fn expected_fullname_matches_server_registration_format() {
        assert_eq!(expected_fullname(), "spark._spark-monitor._tcp.local.");
    }
}
