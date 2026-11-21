//! Smart QUIC TPU transaction router, compute-unit priority fee calculator,
//! slot-4 dead-zone circuit breaker, and leader latency benchmark library.

pub mod clock;
pub mod config;
pub mod decision;
pub mod freshness;
pub mod types;

pub const ROUTER_VERSION: &str = env!("CARGO_PKG_VERSION");
