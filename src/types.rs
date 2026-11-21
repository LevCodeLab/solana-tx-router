use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Monotonically increasing slot number in the Solana cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
pub struct Slot(pub u64);

impl Slot {
    pub const ZERO: Self = Self(0);

    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    pub fn prev(self) -> Option<Self> {
        self.0.checked_sub(1).map(Self)
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Slot {
    fn from(val: u64) -> Self {
        Self(val)
    }
}

/// Base58 hash identifying a sealed block on Solana.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Blockhash(pub String);

impl Blockhash {
    pub fn new(hash: impl Into<String>) -> Self {
        Self(hash.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Blockhash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Validator-local bank identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BankId(pub u64);

impl fmt::Display for BankId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bank-{}", self.0)
    }
}

/// Explicit provenance tracking for telemetry fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FieldProvenance {
    Direct,
    Derived,
    Inferred,
    Estimated,
    Unavailable,
}

impl fmt::Display for FieldProvenance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Direct => write!(f, "DIRECT"),
            Self::Derived => write!(f, "DERIVED"),
            Self::Inferred => write!(f, "INFERRED"),
            Self::Estimated => write!(f, "ESTIMATED"),
            Self::Unavailable => write!(f, "UNAVAILABLE"),
        }
    }
}

/// Identifier for an ingestion provider (e.g. "local-rpc", "devnet-ws", "mainnet-rpc").
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProviderId(pub String);

impl ProviderId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProviderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Validator public key or node identity scheduled as slot leader.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LeaderId(pub String);

impl LeaderId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LeaderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Provider connection health lifecycle status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderConnectionStatus {
    Connected,
    Degraded(String),
    Reconnecting(u32),
    Disconnected,
}

impl fmt::Display for ProviderConnectionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connected => write!(f, "CONNECTED"),
            Self::Degraded(reason) => write!(f, "DEGRADED: {}", reason),
            Self::Reconnecting(attempt) => write!(f, "RECONNECTING (attempt {})", attempt),
            Self::Disconnected => write!(f, "DISCONNECTED"),
        }
    }
}

/// Slot duration representation supporting staged reductions (400ms down to 200ms).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotDuration(pub Duration);

impl SlotDuration {
    pub const MS_400: Self = Self(Duration::from_millis(400));
    pub const MS_350: Self = Self(Duration::from_millis(350));
    pub const MS_300: Self = Self(Duration::from_millis(300));
    pub const MS_250: Self = Self(Duration::from_millis(250));
    pub const MS_200: Self = Self(Duration::from_millis(200));

    pub fn from_millis(ms: u64) -> Self {
        Self(Duration::from_millis(ms))
    }

    pub fn as_millis(&self) -> u64 {
        self.0.as_millis() as u64
    }

    pub fn as_duration(&self) -> Duration {
        self.0
    }
}

impl Default for SlotDuration {
    fn default() -> Self {
        Self::MS_400
    }
}

impl fmt::Display for SlotDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ms", self.as_millis())
    }
}

/// Returns current UNIX epoch milliseconds.
#[inline]
pub fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Formats current UTC time as compact `YYYYMMDD-HHMMSS` using civil calendar arithmetic.
pub fn utc_compact_timestamp() -> String {
    let total_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let days = (total_secs / 86_400) as i64;
    let rem = total_secs % 86_400;
    let hour = rem / 3_600;
    let min = (rem % 3_600) / 60;
    let sec = rem % 60;

    // Howard Hinnant civil-from-days algorithm
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };

    format!("{:04}{:02}{:02}-{:02}{:02}{:02}", year, m, d, hour, min, sec)
}
