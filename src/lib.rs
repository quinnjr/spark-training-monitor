pub mod protocol {
    pub const SERVICE_TYPE: &str = "_spark-monitor._tcp.local.";
    pub const INSTANCE_NAME: &str = "spark";
    pub const PORT: u16 = 7879;
    pub const CHECKIN_RESPONSE: &[u8] = b"OK\n";

    /// Full mDNS instance name a genuine spark-monitor server registers
    /// under, e.g. "spark._spark-monitor._tcp.local.". Clients use this to
    /// reject other devices that advertise the same service type under a
    /// different instance name.
    pub fn expected_fullname() -> String {
        format!("{INSTANCE_NAME}.{SERVICE_TYPE}")
    }
}
