//! Smart QUIC TPU transaction router, compute-unit priority fee calculator,
//! slot-4 dead-zone circuit breaker, and leader latency benchmark library.

pub mod blockhash_stream;
pub mod circuit_breaker;
pub mod clock;
pub mod cluster_probe;
pub mod config;
pub mod decision;
pub mod experiment;
pub mod explainer;
pub mod fee_calculator;
pub mod freshness;
pub mod leader_transport;
pub mod multi_leader_fanout;
pub mod route;
pub mod rpc_client;
pub mod runner;
pub mod stats;
pub mod storage;
pub mod tpu_quic;
pub mod tx_builder;
pub mod types;
pub mod ws_stream;

pub const ROUTER_VERSION: &str = env!("CARGO_PKG_VERSION");
