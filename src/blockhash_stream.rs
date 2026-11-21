use crate::config::RouterConfig;
use crate::types::unix_now_ms;
use crate::ws_stream::{RawSlotNotification, SolanaWsStream};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockhashSnapshot {
    pub blockhash: String,
    pub last_valid_block_height: u64,
    pub slot: u64,
    pub acquired_at_ms: u64,
    pub source: String,
}

impl Default for BlockhashSnapshot {
    fn default() -> Self {
        Self {
            blockhash: String::new(),
            last_valid_block_height: 0,
            slot: 0,
            acquired_at_ms: 0,
            source: "uninitialized".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockhashStreamStats {
    pub total_hits: u64,
    pub total_refreshes: u64,
    pub slots_observed: u64,
    pub current_age_ms: u64,
    pub is_running: bool,
    pub rpc_url: String,
    pub ws_url: String,
}

/// Zero-RPC-lag continuous RAM blockhash stream.
///
/// Eliminates the 150-350ms HTTP `getLatestBlockhash` round-trip from the hot dispatch path
/// by keeping a fresh blockhash warm in RAM via `slotSubscribe` notifications and a 600ms ticker.
pub struct BlockhashStream {
    rpc_url: String,
    ws_url: String,
    current: Arc<RwLock<BlockhashSnapshot>>,
    history: Arc<RwLock<Vec<BlockhashSnapshot>>>,
    is_running: Arc<AtomicBool>,
    http_client: reqwest::Client,
    stats_hits: Arc<AtomicU64>,
    stats_refreshes: Arc<AtomicU64>,
    stats_slots: Arc<AtomicU64>,
    refresh_in_flight: Arc<AtomicBool>,
}

impl BlockhashStream {
    pub fn new(rpc_url: impl Into<String>) -> Self {
        let rpc = rpc_url.into();
        let ws = RouterConfig::derive_ws_url(&rpc);
        Self::with_ws(rpc, ws)
    }

    pub fn with_ws(rpc_url: impl Into<String>, ws_url: impl Into<String>) -> Self {
        Self {
            rpc_url: rpc_url.into(),
            ws_url: ws_url.into(),
            current: Arc::new(RwLock::new(BlockhashSnapshot::default())),
            history: Arc::new(RwLock::new(Vec::with_capacity(32))),
            is_running: Arc::new(AtomicBool::new(false)),
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .tcp_nodelay(true)
                .build()
                .unwrap_or_default(),
            stats_hits: Arc::new(AtomicU64::new(0)),
            stats_refreshes: Arc::new(AtomicU64::new(0)),
            stats_slots: Arc::new(AtomicU64::new(0)),
            refresh_in_flight: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Seeds a snapshot directly in memory (useful for local fixture tests and offline dry runs).
    pub async fn seed_snapshot(&self, snapshot: BlockhashSnapshot) {
        {
            let mut write = self.current.write().await;
            *write = snapshot.clone();
        }
        let mut hist = self.history.write().await;
        if hist.len() >= 32 {
            hist.remove(0);
        }
        hist.push(snapshot);
    }

    /// Fast RAM lookup of the cached `(blockhash, last_valid_block_height)` with zero blocking network I/O.
    pub async fn get_latest_blockhash(&self) -> (String, u64) {
        self.stats_hits.fetch_add(1, Ordering::Relaxed);
        let read = self.current.read().await;
        let age_ms = unix_now_ms().saturating_sub(read.acquired_at_ms);

        if (read.blockhash.is_empty() || age_ms > 4_000) && self.is_running.load(Ordering::Relaxed)
        {
            let self_clone = self.clone_handle();
            tokio::spawn(async move {
                let _ = self_clone.fetch_and_update_blockhash("eager_refresh").await;
            });
        }

        (read.blockhash.clone(), read.last_valid_block_height)
    }

    pub async fn get_snapshot(&self) -> BlockhashSnapshot {
        self.current.read().await.clone()
    }

    pub async fn get_stats(&self) -> BlockhashStreamStats {
        let read = self.current.read().await;
        let age = unix_now_ms().saturating_sub(read.acquired_at_ms);

        BlockhashStreamStats {
            total_hits: self.stats_hits.load(Ordering::Relaxed),
            total_refreshes: self.stats_refreshes.load(Ordering::Relaxed),
            slots_observed: self.stats_slots.load(Ordering::Relaxed),
            current_age_ms: age,
            is_running: self.is_running.load(Ordering::Relaxed),
            rpc_url: self.rpc_url.clone(),
            ws_url: self.ws_url.clone(),
        }
    }

    /// Starts the continuous background WebSocket + fallback ticker pipeline.
    pub async fn start(&self) {
        if self.is_running.swap(true, Ordering::SeqCst) {
            return;
        }

        info!(
            "Starting continuous blockhash stream: RPC={} WS={}",
            self.rpc_url, self.ws_url
        );

        if let Err(e) = self.fetch_and_update_blockhash("initial_seed").await {
            warn!(
                "Failed to seed initial blockhash: {}. Will retry in background...",
                e
            );
        }

        let is_running = self.is_running.clone();
        let ws_url = self.ws_url.clone();
        let self_handle = self.clone_handle();

        tokio::spawn(async move {
            let (slot_tx, mut slot_rx) = mpsc::channel::<RawSlotNotification>(100);
            let ws_stream = SolanaWsStream::new(ws_url);
            ws_stream.start(slot_tx);

            let mut last_refresh_slot = 0u64;

            while is_running.load(Ordering::SeqCst) {
                match tokio::time::timeout(Duration::from_millis(1500), slot_rx.recv()).await {
                    Ok(Some(slot_notif)) => {
                        let slot = slot_notif.slot.0;
                        self_handle.stats_slots.fetch_add(1, Ordering::Relaxed);

                        if slot >= last_refresh_slot.saturating_add(2) {
                            last_refresh_slot = slot;
                            let h = self_handle.clone();
                            tokio::spawn(async move {
                                let _ = h.fetch_and_update_blockhash("ws_slot_trigger").await;
                            });
                        }
                    }
                    Ok(None) => {
                        debug!("WebSocket slot channel closed, backing off...");
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    Err(_) => {
                        let h = self_handle.clone();
                        tokio::spawn(async move {
                            let _ = h.fetch_and_update_blockhash("heartbeat_tick").await;
                        });
                    }
                }
            }

            ws_stream.stop();
        });

        let is_running_hb = self.is_running.clone();
        let self_handle_hb = self.clone_handle();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(600));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            while is_running_hb.load(Ordering::SeqCst) {
                interval.tick().await;
                let age = {
                    let read = self_handle_hb.current.read().await;
                    unix_now_ms().saturating_sub(read.acquired_at_ms)
                };
                if age >= 600 {
                    let _ = self_handle_hb
                        .fetch_and_update_blockhash("interval_hb")
                        .await;
                }
            }
        });
    }

    pub async fn fetch_and_update_blockhash(
        &self,
        trigger_reason: &str,
    ) -> Result<BlockhashSnapshot, String> {
        if self.refresh_in_flight.swap(true, Ordering::SeqCst) {
            return Ok(self.current.read().await.clone());
        }

        let t_start = Instant::now();
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getLatestBlockhash",
            "params": [{"commitment": "confirmed"}]
        });

        let resp_res = self
            .http_client
            .post(&self.rpc_url)
            .json(&payload)
            .send()
            .await;

        self.refresh_in_flight.store(false, Ordering::SeqCst);

        let resp = resp_res.map_err(|e| format!("HTTP request error: {}", e))?;
        let json_val = resp
            .json::<serde_json::Value>()
            .await
            .map_err(|e| format!("JSON decode error: {}", e))?;

        if let Some(err) = json_val.get("error") {
            return Err(format!("RPC error response: {}", err));
        }

        let result = json_val
            .get("result")
            .ok_or_else(|| "Missing result in getLatestBlockhash".to_string())?;
        let value = result
            .get("value")
            .ok_or_else(|| "Missing result.value".to_string())?;

        let blockhash = value
            .get("blockhash")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Missing blockhash string".to_string())?
            .to_string();
        let last_valid_block_height = value
            .get("lastValidBlockHeight")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let slot = result
            .get("context")
            .and_then(|c| c.get("slot"))
            .and_then(|s| s.as_u64())
            .unwrap_or(0);

        let snapshot = BlockhashSnapshot {
            blockhash,
            last_valid_block_height,
            slot,
            acquired_at_ms: unix_now_ms(),
            source: trigger_reason.to_string(),
        };

        self.seed_snapshot(snapshot.clone()).await;
        self.stats_refreshes.fetch_add(1, Ordering::Relaxed);

        debug!(
            "Cached blockhash refreshed (trigger: {}, slot: {}, fetch: {}us)",
            trigger_reason,
            slot,
            t_start.elapsed().as_micros()
        );

        Ok(snapshot)
    }

    pub fn stop(&self) {
        self.is_running.store(false, Ordering::SeqCst);
    }

    fn clone_handle(&self) -> Arc<Self> {
        Arc::new(Self {
            rpc_url: self.rpc_url.clone(),
            ws_url: self.ws_url.clone(),
            current: self.current.clone(),
            history: self.history.clone(),
            is_running: self.is_running.clone(),
            http_client: self.http_client.clone(),
            stats_hits: self.stats_hits.clone(),
            stats_refreshes: self.stats_refreshes.clone(),
            stats_slots: self.stats_slots.clone(),
            refresh_in_flight: self.refresh_in_flight.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_blockhash_stream_ram_seed_and_lookup() {
        let stream = BlockhashStream::new("http://127.0.0.1:8899");
        stream
            .seed_snapshot(BlockhashSnapshot {
                blockhash: "11111111111111111111111111111111".to_string(),
                last_valid_block_height: 550_000,
                slot: 420_000,
                acquired_at_ms: unix_now_ms(),
                source: "unit_test".to_string(),
            })
            .await;

        let (bh, lv) = stream.get_latest_blockhash().await;
        assert_eq!(bh, "11111111111111111111111111111111");
        assert_eq!(lv, 550_000);

        let stats = stream.get_stats().await;
        assert_eq!(stats.total_hits, 1);
        assert_eq!(stats.ws_url, "ws://127.0.0.1:8900");
    }
}
