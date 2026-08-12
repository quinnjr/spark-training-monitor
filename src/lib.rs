pub mod protocol {
    pub const SERVICE_TYPE: &str = "_spark-monitor._tcp.local.";
    pub const INSTANCE_NAME: &str = "spark";
    pub const PORT: u16 = 7879;
    /// Default port for the spark-side HTTP status API (`GET /status`,
    /// `POST /training`). Advertised in the mDNS TXT record as `http_port`
    /// so a host that overrides it is still reachable.
    pub const HTTP_PORT: u16 = 7880;
    pub const HTTP_PORT_TXT_KEY: &str = "http_port";
    pub const CHECKIN_RESPONSE: &[u8] = b"OK\n";
}

pub mod status {
    use serde::{Deserialize, Serialize};

    /// One GPU's health as reported by `nvidia-smi`. Every field except
    /// `index`/`name` is optional because `nvidia-smi` prints
    /// "[Not Supported]"/"[N/A]" for fields a given board can't report.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct GpuStat {
        pub index: u32,
        pub name: String,
        pub utilization_pct: Option<f64>,
        pub mem_used_mib: Option<f64>,
        pub mem_total_mib: Option<f64>,
        pub temperature_c: Option<f64>,
        pub power_w: Option<f64>,
    }

    /// Latest training metrics pushed by a training script via
    /// `POST /training`. The payload is deliberately schemaless: whatever
    /// JSON object the trainer sends is stored and relayed as-is.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct TrainingReport {
        pub received_at: String,
        pub metrics: serde_json::Value,
    }

    /// Full health snapshot served by a spark host at `GET /status`.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct StatusSnapshot {
        pub instance: String,
        pub hostname: String,
        pub timestamp: String,
        pub uptime_secs: Option<f64>,
        /// 1, 5 and 15 minute load averages.
        pub load_avg: Option<[f64; 3]>,
        pub mem_total_kb: Option<u64>,
        pub mem_available_kb: Option<u64>,
        pub gpus: Vec<GpuStat>,
        pub training: Option<TrainingReport>,
    }
}
