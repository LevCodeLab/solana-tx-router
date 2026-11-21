//! Live cluster validator topology & QUIC TPU readiness probe.

use crate::blockhash_stream::{BlockhashSnapshot, BlockhashStream};
use crate::leader_transport::LeaderTransportResolver;
use crate::tpu_quic::TpuQuicRouter;
use crate::types::unix_now_ms;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderProbeResult {
    pub slot: u64,
    pub slot_delta: i64,
    pub slot_in_window: u64,
    pub leader_pubkey: String,
    pub tpu_quic: Option<SocketAddr>,
    pub quic_latency_us: u64,
    pub is_dead_zone: bool,
    pub status: String,
    pub routing_verdict: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterProbeReport {
    pub cluster: String,
    pub rpc_url: String,
    pub ws_url: String,
    pub current_slot: u64,
    pub slot_in_window: u64,
    pub is_current_dead_zone: bool,
    pub live_blockhash: String,
    pub blockhash_age_ms: u64,
    pub ram_lookup_ns: u64,
    pub total_cluster_nodes: usize,
    pub quic_verified_nodes: usize,
    pub leader_probes: Vec<LeaderProbeResult>,
}

pub struct ClusterProbe {
    rpc_url: String,
    ws_url: String,
    http_client: reqwest::Client,
}

impl ClusterProbe {
    pub fn new(rpc_url: impl Into<String>, ws_url: impl Into<String>) -> Self {
        Self {
            rpc_url: rpc_url.into(),
            ws_url: ws_url.into(),
            http_client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
        }
    }

    pub fn devnet_default() -> Self {
        Self::new(
            "https://api.devnet.solana.com",
            "wss://api.devnet.solana.com",
        )
    }

    /// Generates an offline synthetic probe report for local verification without network access.
    pub async fn run_offline(&self, start_slot: u64) -> ClusterProbeReport {
        let stream = BlockhashStream::new("http://127.0.0.1:8899");
        stream
            .seed_snapshot(BlockhashSnapshot {
                blockhash: "11111111111111111111111111111111".to_string(),
                last_valid_block_height: start_slot + 150,
                slot: start_slot,
                acquired_at_ms: unix_now_ms(),
                source: "offline_probe".to_string(),
            })
            .await;

        let t_ram = Instant::now();
        let snap = stream.get_snapshot().await;
        let ram_lookup_ns = t_ram.elapsed().as_nanos() as u64;

        let current_slot_in_window = (start_slot % 4) + 1;
        let is_current_dead_zone = current_slot_in_window == 4;

        let mut leader_probes = Vec::with_capacity(5);
        for idx in 0..5usize {
            let slot = start_slot + idx as u64;
            let slot_in_win = (slot % 4) + 1;
            let is_dz = slot_in_win == 4;
            let routing_verdict = if is_dz {
                "DEAD_ZONE: Defers to Slot 1 of Next Leader".to_string()
            } else if idx == 0 {
                "EXECUTE_NOW: Immediate QUIC Dispatch to Current Leader".to_string()
            } else {
                format!("PRE-WARM: Ready for Slot +{} Handover", idx)
            };

            leader_probes.push(LeaderProbeResult {
                slot,
                slot_delta: idx as i64,
                slot_in_window: slot_in_win,
                leader_pubkey: format!("ValidatorNode{:02}1111111111111111111111111111", idx + 1),
                tpu_quic: Some(
                    format!("127.0.0.1:{}", 8009 + idx)
                        .parse()
                        .expect("valid socket"),
                ),
                quic_latency_us: 140 + (idx as u64 * 20),
                is_dead_zone: is_dz,
                status: "QUIC_WARMED_READY".to_string(),
                routing_verdict,
            });
        }

        ClusterProbeReport {
            cluster: "offline-fixture".to_string(),
            rpc_url: self.rpc_url.clone(),
            ws_url: self.ws_url.clone(),
            current_slot: start_slot,
            slot_in_window: current_slot_in_window,
            is_current_dead_zone,
            live_blockhash: snap.blockhash,
            blockhash_age_ms: 0,
            ram_lookup_ns,
            total_cluster_nodes: 5,
            quic_verified_nodes: 5,
            leader_probes,
        }
    }

    pub async fn run(&self) -> Result<ClusterProbeReport, Box<dyn std::error::Error + Send + Sync>> {
        let stream = Arc::new(BlockhashStream::with_ws(
            self.rpc_url.clone(),
            self.ws_url.clone(),
        ));
        stream.start().await;

        let resolver = Arc::new(LeaderTransportResolver::new(self.rpc_url.clone()));
        let quic_nodes_count = resolver.sync_cluster_nodes().await.unwrap_or(0);

        let slot_resp = self
            .http_client
            .post(&self.rpc_url)
            .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "getSlot"}))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;
        let current_slot = slot_resp.get("result").and_then(|r| r.as_u64()).unwrap_or(0);

        let leaders_resp = self
            .http_client
            .post(&self.rpc_url)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "getSlotLeaders",
                "params": [current_slot, 5]
            }))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        let leaders: Vec<String> = leaders_resp
            .get("result")
            .and_then(|r| r.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        let t_ram_start = Instant::now();
        let snap = stream.get_snapshot().await;
        let ram_lookup_ns = t_ram_start.elapsed().as_nanos() as u64;
        let blockhash_age_ms = unix_now_ms().saturating_sub(snap.acquired_at_ms);

        let router = TpuQuicRouter::new(self.rpc_url.clone())?;

        let mut leader_probes = Vec::new();
        let current_slot_in_window = (current_slot % 4) + 1;
        let is_current_dead_zone = current_slot_in_window == 4;

        for (idx, leader_pubkey) in leaders.iter().enumerate() {
            let slot = current_slot + idx as u64;
            let slot_delta = idx as i64;
            let slot_in_win = (slot % 4) + 1;
            let is_dz = slot_in_win == 4;

            let endpoint_opt = resolver.resolve(leader_pubkey).await;
            let tpu_quic = endpoint_opt.as_ref().and_then(|e| e.tpu_quic);

            let (quic_latency_us, status) = if let Some(socket_addr) = tpu_quic {
                let t_quic_start = Instant::now();
                router.insert_node(leader_pubkey.clone(), socket_addr).await;
                router.prewarm_leader(leader_pubkey).await;
                (
                    t_quic_start.elapsed().as_micros() as u64,
                    "QUIC_WARMED_READY".to_string(),
                )
            } else {
                (0, "TPU_PORT_UNADVERTISED".to_string())
            };

            let routing_verdict = if is_dz {
                "DEAD_ZONE: Defers to Slot 1 of Next Leader".to_string()
            } else if idx == 0 {
                "EXECUTE_NOW: Immediate QUIC Dispatch to Current Leader".to_string()
            } else {
                format!("PRE-WARM: Ready for Slot +{} Handover", idx)
            };

            leader_probes.push(LeaderProbeResult {
                slot,
                slot_delta,
                slot_in_window: slot_in_win,
                leader_pubkey: leader_pubkey.clone(),
                tpu_quic,
                quic_latency_us,
                is_dead_zone: is_dz,
                status,
                routing_verdict,
            });
        }

        stream.stop();

        Ok(ClusterProbeReport {
            cluster: "live-cluster".to_string(),
            rpc_url: self.rpc_url.clone(),
            ws_url: self.ws_url.clone(),
            current_slot,
            slot_in_window: current_slot_in_window,
            is_current_dead_zone,
            live_blockhash: snap.blockhash,
            blockhash_age_ms,
            ram_lookup_ns,
            total_cluster_nodes: resolver.total_cached_nodes().await,
            quic_verified_nodes: quic_nodes_count,
            leader_probes,
        })
    }
}
