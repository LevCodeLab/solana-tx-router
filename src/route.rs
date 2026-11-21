use crate::blockhash_stream::BlockhashStream;
use crate::experiment::ExecutionStatus;
use crate::leader_transport::LeaderTransportResolver;
use crate::rpc_client::SolanaRpcClient;
use crate::tpu_quic::TpuQuicRouter;
use crate::types::unix_now_ms;
use async_trait::async_trait;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum RouteError {
    #[error("Transport error: {0}")]
    Transport(String),
    #[error("RPC error: {0}")]
    Rpc(String),
    #[error("Blockhash expired: {0}")]
    ExpiredBlockhash(String),
    #[error("Timeout: {0}")]
    Timeout(String),
    #[error("Other error: {0}")]
    Other(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmissionAck {
    pub signature: String,
    pub ack_time_ns: u64,
    pub route_name: String,
    #[serde(default)]
    pub handshake_time_ns: Option<u64>,
    #[serde(default)]
    pub handoff_time_ns: Option<u64>,
    #[serde(default)]
    pub target_leader: Option<String>,
    #[serde(default)]
    pub tpu_socket: Option<String>,
    #[serde(default)]
    pub connection_reused: bool,
    #[serde(default)]
    pub fallback_triggered: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FallbackPolicy {
    QuicOnly,
    QuicThenRpc,
    RpcOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LeaderTargetPolicy {
    CurrentLeaderOnly,
    CurrentPlusNext,
    MultiLeaderFanout(usize),
    HandoverAware,
}

#[async_trait]
pub trait ExecutionRoute: Send + Sync {
    fn route_name(&self) -> &str;
    async fn get_latest_blockhash(&self) -> Result<(String, u64), RouteError>;
    async fn get_current_slot(&self) -> Result<u64, RouteError>;
    async fn submit_transaction(&self, tx_base64: &str) -> Result<SubmissionAck, RouteError>;
    async fn poll_status(
        &self,
        signature: &str,
        timeout_ms: u64,
    ) -> Result<(ExecutionStatus, Option<u64>), RouteError>;
}

/// Standard JSON-RPC route (Control Group baseline).
pub struct StandardRpcRoute {
    #[allow(dead_code)]
    rpc_client: Arc<SolanaRpcClient>,
    raw_rpc_url: String,
    http_client: reqwest::Client,
}

impl StandardRpcRoute {
    pub fn new(rpc_url: impl Into<String>) -> Self {
        let url = rpc_url.into();
        Self {
            rpc_client: Arc::new(SolanaRpcClient::new(url.clone())),
            raw_rpc_url: url,
            http_client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
        }
    }
}

#[async_trait]
impl ExecutionRoute for StandardRpcRoute {
    fn route_name(&self) -> &str {
        "standard_rpc_control"
    }

    async fn get_latest_blockhash(&self) -> Result<(String, u64), RouteError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getLatestBlockhash",
            "params": [{"commitment": "confirmed"}]
        });

        let resp = self
            .http_client
            .post(&self.raw_rpc_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| RouteError::Transport(e.to_string()))?
            .json::<serde_json::Value>()
            .await
            .map_err(|e| RouteError::Transport(e.to_string()))?;

        if let Some(err) = resp.get("error") {
            return Err(RouteError::Rpc(err.to_string()));
        }

        let value = resp
            .get("result")
            .and_then(|r| r.get("value"))
            .ok_or_else(|| RouteError::Rpc("Missing result.value in getLatestBlockhash".into()))?;

        let blockhash = value
            .get("blockhash")
            .and_then(|v| v.as_str())
            .ok_or_else(|| RouteError::Rpc("Missing blockhash field".into()))?
            .to_string();

        let last_valid = value
            .get("lastValidBlockHeight")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        Ok((blockhash, last_valid))
    }

    async fn get_current_slot(&self) -> Result<u64, RouteError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSlot",
            "params": [{"commitment": "confirmed"}]
        });

        let resp = self
            .http_client
            .post(&self.raw_rpc_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| RouteError::Transport(e.to_string()))?
            .json::<serde_json::Value>()
            .await
            .map_err(|e| RouteError::Transport(e.to_string()))?;

        if let Some(err) = resp.get("error") {
            return Err(RouteError::Rpc(err.to_string()));
        }

        resp.get("result")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| RouteError::Rpc("Missing slot in getSlot response".into()))
    }

    async fn submit_transaction(&self, tx_base64: &str) -> Result<SubmissionAck, RouteError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                tx_base64,
                {
                    "encoding": "base64",
                    "skipPreflight": false,
                    "preflightCommitment": "processed"
                }
            ]
        });

        let t_start = Instant::now();
        let resp = self
            .http_client
            .post(&self.raw_rpc_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| RouteError::Transport(e.to_string()))?
            .json::<serde_json::Value>()
            .await
            .map_err(|e| RouteError::Transport(e.to_string()))?;

        let ack_time_ns = t_start.elapsed().as_nanos() as u64;

        if let Some(err) = resp.get("error") {
            let err_str = err.to_string();
            if err_str.contains("BlockhashNotFound") || err_str.contains("expired") {
                return Err(RouteError::ExpiredBlockhash(err_str));
            }
            return Err(RouteError::Rpc(err_str));
        }

        let sig = resp
            .get("result")
            .and_then(|v| v.as_str())
            .ok_or_else(|| RouteError::Rpc("Missing signature in sendTransaction response".into()))?
            .to_string();

        Ok(SubmissionAck {
            signature: sig,
            ack_time_ns,
            route_name: self.route_name().to_string(),
            handshake_time_ns: None,
            handoff_time_ns: None,
            target_leader: None,
            tpu_socket: None,
            connection_reused: false,
            fallback_triggered: false,
        })
    }

    async fn poll_status(
        &self,
        signature: &str,
        timeout_ms: u64,
    ) -> Result<(ExecutionStatus, Option<u64>), RouteError> {
        let start = Instant::now();
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignatureStatuses",
            "params": [[signature], {"searchTransactionHistory": false}]
        });

        while start.elapsed().as_millis() < timeout_ms as u128 {
            if let Ok(resp) = self
                .http_client
                .post(&self.raw_rpc_url)
                .json(&payload)
                .send()
                .await
            {
                if let Ok(json_val) = resp.json::<serde_json::Value>().await {
                    if let Some(statuses) = json_val
                        .get("result")
                        .and_then(|r| r.get("value"))
                        .and_then(|v| v.as_array())
                    {
                        if let Some(Some(item)) = statuses.first().map(|v| v.as_object()) {
                            let landed_slot = item.get("slot").and_then(|s| s.as_u64());
                            if let Some(err) = item.get("err") {
                                if !err.is_null() {
                                    return Ok((ExecutionStatus::Rejected, landed_slot));
                                }
                            }
                            if let Some(conf_status) =
                                item.get("confirmationStatus").and_then(|c| c.as_str())
                            {
                                match conf_status {
                                    "finalized" => {
                                        return Ok((ExecutionStatus::Finalized, landed_slot))
                                    }
                                    "confirmed" => {
                                        return Ok((ExecutionStatus::Confirmed, landed_slot))
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
        }

        Ok((ExecutionStatus::Timeout, None))
    }
}

/// Router-Aware RPC route backed by cached RAM `BlockhashStream` and slot-window timing.
pub struct RouterAwareRoute {
    standard_route: StandardRpcRoute,
    cached_blockhash: tokio::sync::RwLock<(String, u64, u64)>,
    blockhash_stream: Option<Arc<BlockhashStream>>,
}

impl RouterAwareRoute {
    pub fn new(rpc_url: impl Into<String>) -> Self {
        Self {
            standard_route: StandardRpcRoute::new(rpc_url),
            cached_blockhash: tokio::sync::RwLock::new((String::new(), 0, 0)),
            blockhash_stream: None,
        }
    }

    pub fn with_blockhash_stream(mut self, stream: Arc<BlockhashStream>) -> Self {
        self.blockhash_stream = Some(stream);
        self
    }

    pub async fn update_blockhash(&self, blockhash: String, last_valid: u64) {
        let mut write = self.cached_blockhash.write().await;
        *write = (blockhash, last_valid, unix_now_ms());
    }
}

#[async_trait]
impl ExecutionRoute for RouterAwareRoute {
    fn route_name(&self) -> &str {
        "router_aware_route"
    }

    async fn get_latest_blockhash(&self) -> Result<(String, u64), RouteError> {
        if let Some(ref stream) = self.blockhash_stream {
            let (bh, lv) = stream.get_latest_blockhash().await;
            if !bh.is_empty() {
                return Ok((bh, lv));
            }
        }

        let read = self.cached_blockhash.read().await;
        let now_ms = unix_now_ms();
        if !read.0.is_empty() && now_ms.saturating_sub(read.2) < 8_000 {
            return Ok((read.0.clone(), read.1));
        }
        drop(read);

        let (bh, lv) = self.standard_route.get_latest_blockhash().await?;
        self.update_blockhash(bh.clone(), lv).await;
        Ok((bh, lv))
    }

    async fn get_current_slot(&self) -> Result<u64, RouteError> {
        self.standard_route.get_current_slot().await
    }

    async fn submit_transaction(&self, tx_base64: &str) -> Result<SubmissionAck, RouteError> {
        self.standard_route.submit_transaction(tx_base64).await
    }

    async fn poll_status(
        &self,
        signature: &str,
        timeout_ms: u64,
    ) -> Result<(ExecutionStatus, Option<u64>), RouteError> {
        self.standard_route.poll_status(signature, timeout_ms).await
    }
}

/// Deterministic zero-network local fixture route for fast offline benchmarks and CI tests.
pub struct LocalFixtureRoute {
    simulated_rtt_ms: u64,
    route_label: String,
}

impl LocalFixtureRoute {
    pub fn new(simulated_rtt_ms: u64) -> Self {
        Self {
            simulated_rtt_ms,
            route_label: "local_fixture_route".to_string(),
        }
    }

    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.route_label = label.into();
        self
    }
}

#[async_trait]
impl ExecutionRoute for LocalFixtureRoute {
    fn route_name(&self) -> &str {
        &self.route_label
    }

    async fn get_latest_blockhash(&self) -> Result<(String, u64), RouteError> {
        if self.simulated_rtt_ms > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(self.simulated_rtt_ms)).await;
        }
        Ok(("LocalFixtureBlockhash_CandidateTip".to_string(), 1_000_150))
    }

    async fn get_current_slot(&self) -> Result<u64, RouteError> {
        Ok(448_160_000)
    }

    async fn submit_transaction(&self, _tx_base64: &str) -> Result<SubmissionAck, RouteError> {
        let t_start = Instant::now();
        if self.simulated_rtt_ms > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(self.simulated_rtt_ms)).await;
        }
        let ack_time_ns = t_start.elapsed().as_nanos() as u64;
        let mock_sig = format!(
            "SimulatedSig_{}",
            bs58::encode(&ack_time_ns.to_le_bytes()).into_string()
        );
        Ok(SubmissionAck {
            signature: mock_sig,
            ack_time_ns,
            route_name: self.route_name().to_string(),
            handshake_time_ns: Some(0),
            handoff_time_ns: Some(ack_time_ns),
            target_leader: Some("LocalLeader11111111111111111111111111111111".to_string()),
            tpu_socket: Some("127.0.0.1:8009".to_string()),
            connection_reused: true,
            fallback_triggered: false,
        })
    }

    async fn poll_status(
        &self,
        _signature: &str,
        _timeout_ms: u64,
    ) -> Result<(ExecutionStatus, Option<u64>), RouteError> {
        if self.simulated_rtt_ms > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(self.simulated_rtt_ms)).await;
        }
        Ok((ExecutionStatus::Confirmed, Some(448_160_001)))
    }
}

/// Direct Leader QUIC TPU execution route powered by `TpuQuicRouter` (4-lane striped sockets).
pub struct DirectLeaderQuicRoute {
    route_name: String,
    rpc_url: String,
    resolver: Arc<LeaderTransportResolver>,
    quic_router: Arc<TpuQuicRouter>,
    fallback_route: Arc<StandardRpcRoute>,
    fallback_policy: FallbackPolicy,
    target_policy: LeaderTargetPolicy,
    is_router_aware: bool,
    cached_blockhash: tokio::sync::RwLock<(String, u64, u64)>,
    http_client: reqwest::Client,
    connection_reused_count: Arc<AtomicU64>,
    total_handoffs: Arc<AtomicU64>,
    blockhash_stream: Option<Arc<BlockhashStream>>,
}

impl DirectLeaderQuicRoute {
    pub fn new(
        rpc_url: impl Into<String>,
        resolver: Arc<LeaderTransportResolver>,
    ) -> Result<Self, RouteError> {
        let url = rpc_url.into();
        let quic_router = TpuQuicRouter::new(url.clone()).map_err(RouteError::Transport)?;

        Ok(Self {
            route_name: "direct_leader_quic".to_string(),
            fallback_route: Arc::new(StandardRpcRoute::new(url.clone())),
            rpc_url: url,
            resolver,
            quic_router: Arc::new(quic_router),
            fallback_policy: FallbackPolicy::QuicThenRpc,
            target_policy: LeaderTargetPolicy::CurrentPlusNext,
            is_router_aware: true,
            cached_blockhash: tokio::sync::RwLock::new((String::new(), 0, 0)),
            http_client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
            connection_reused_count: Arc::new(AtomicU64::new(0)),
            total_handoffs: Arc::new(AtomicU64::new(0)),
            blockhash_stream: None,
        })
    }

    pub fn with_blockhash_stream(mut self, stream: Arc<BlockhashStream>) -> Self {
        self.blockhash_stream = Some(stream);
        self
    }

    pub fn with_router_awareness(mut self, enabled: bool) -> Self {
        self.is_router_aware = enabled;
        if enabled {
            self.route_name = "router_aware_quic".to_string();
        } else {
            self.route_name = "standard_quic_control".to_string();
        }
        self
    }

    pub fn with_fallback_policy(mut self, policy: FallbackPolicy) -> Self {
        self.fallback_policy = policy;
        self
    }

    pub fn with_target_policy(mut self, policy: LeaderTargetPolicy) -> Self {
        self.target_policy = policy;
        self
    }

    pub fn with_route_name(mut self, name: impl Into<String>) -> Self {
        self.route_name = name.into();
        self
    }

    pub async fn prewarm_endpoint(&self, pubkey: &str, addr: SocketAddr) -> u64 {
        let t_start = Instant::now();
        self.quic_router.insert_node(pubkey, addr).await;
        self.quic_router.prewarm_leader(pubkey).await;
        t_start.elapsed().as_nanos() as u64
    }

    pub async fn get_slot_leaders(&self, slot: u64, limit: u64) -> Result<Vec<String>, RouteError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSlotLeaders",
            "params": [slot, limit]
        });

        let resp = self
            .http_client
            .post(&self.rpc_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| RouteError::Transport(e.to_string()))?
            .json::<serde_json::Value>()
            .await
            .map_err(|e| RouteError::Transport(e.to_string()))?;

        if let Some(err) = resp.get("error") {
            return Err(RouteError::Rpc(err.to_string()));
        }

        let leaders = resp
            .get("result")
            .and_then(|r| r.as_array())
            .ok_or_else(|| RouteError::Rpc("Missing result array in getSlotLeaders".into()))?
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();

        Ok(leaders)
    }

    pub async fn update_blockhash(&self, blockhash: String, last_valid: u64) {
        let mut write = self.cached_blockhash.write().await;
        *write = (blockhash, last_valid, unix_now_ms());
    }

    pub fn connection_reuse_count(&self) -> u64 {
        self.connection_reused_count.load(Ordering::Acquire)
    }

    pub fn total_handoffs(&self) -> u64 {
        self.total_handoffs.load(Ordering::Acquire)
    }
}

#[async_trait]
impl ExecutionRoute for DirectLeaderQuicRoute {
    fn route_name(&self) -> &str {
        &self.route_name
    }

    async fn get_latest_blockhash(&self) -> Result<(String, u64), RouteError> {
        if self.is_router_aware {
            if let Some(ref stream) = self.blockhash_stream {
                let (bh, lv) = stream.get_latest_blockhash().await;
                if !bh.is_empty() {
                    return Ok((bh, lv));
                }
            }

            let read = self.cached_blockhash.read().await;
            let now_ms = unix_now_ms();
            if !read.0.is_empty() && now_ms.saturating_sub(read.2) < 8_000 {
                return Ok((read.0.clone(), read.1));
            }
            drop(read);
        }

        let (bh, lv) = self.fallback_route.get_latest_blockhash().await?;
        if self.is_router_aware {
            self.update_blockhash(bh.clone(), lv).await;
        }
        Ok((bh, lv))
    }

    async fn get_current_slot(&self) -> Result<u64, RouteError> {
        self.fallback_route.get_current_slot().await
    }

    async fn submit_transaction(&self, tx_base64: &str) -> Result<SubmissionAck, RouteError> {
        let t_total_start = Instant::now();

        if self.fallback_policy == FallbackPolicy::RpcOnly {
            return self.fallback_route.submit_transaction(tx_base64).await;
        }

        let tx_bytes = base64::engine::general_purpose::STANDARD
            .decode(tx_base64)
            .map_err(|e| RouteError::Other(format!("Base64 decode failed: {}", e)))?;

        let sig = if tx_bytes.len() >= 65 && tx_bytes[0] == 1 {
            bs58::encode(&tx_bytes[1..65]).into_string()
        } else {
            format!(
                "Sig{}",
                bs58::encode(&tx_bytes[..tx_bytes.len().min(32)]).into_string()
            )
        };

        let slot = self.get_current_slot().await.unwrap_or(0);
        let max_fetch = match self.target_policy {
            LeaderTargetPolicy::CurrentLeaderOnly => 2,
            LeaderTargetPolicy::CurrentPlusNext => 3,
            LeaderTargetPolicy::MultiLeaderFanout(n) => (n + 1).max(4) as u64,
            LeaderTargetPolicy::HandoverAware => 4,
        };
        let leaders = self.get_slot_leaders(slot, max_fetch).await.unwrap_or_default();
        let slot_in_window = (slot % 4) + 1;
        let is_dead_zone = slot_in_window == 4;

        let target_leaders: Vec<String> = match self.target_policy {
            LeaderTargetPolicy::CurrentLeaderOnly => leaders.iter().take(1).cloned().collect(),
            LeaderTargetPolicy::CurrentPlusNext => leaders.iter().take(2).cloned().collect(),
            LeaderTargetPolicy::MultiLeaderFanout(n) => leaders.iter().take(n).cloned().collect(),
            LeaderTargetPolicy::HandoverAware => {
                if is_dead_zone {
                    leaders.iter().skip(1).take(2).cloned().collect()
                } else {
                    leaders.iter().take(2).cloned().collect()
                }
            }
        };

        let mut primary_socket = None;
        let mut any_reused = false;
        for (idx, leader_key) in target_leaders.iter().enumerate() {
            if let Some(ep) = self.resolver.get_or_resolve(leader_key).await {
                if let Some(sock) = ep.tpu_quic {
                    if idx == 0 {
                        primary_socket = Some(sock);
                    }
                    if self.quic_router.has_warm_connection(&sock).await {
                        any_reused = true;
                    }
                    self.quic_router.insert_node(leader_key.clone(), sock).await;
                }
            }
        }

        let primary_leader = target_leaders.first().cloned().unwrap_or_default();

        if primary_socket.is_none() && target_leaders.is_empty() {
            if self.fallback_policy == FallbackPolicy::QuicOnly {
                return Err(RouteError::Transport(
                    "No TPU QUIC endpoint found for target leaders".into(),
                ));
            }
            let mut fallback_ack = self.fallback_route.submit_transaction(tx_base64).await?;
            fallback_ack.fallback_triggered = true;
            fallback_ack.route_name = format!("{}_rpc_fallback", self.route_name);
            return Ok(fallback_ack);
        }

        let t_handoff_start = Instant::now();
        let fanout = self
            .quic_router
            .fanout_multi_leader(&target_leaders, &tx_bytes)
            .await;
        let handoff_time_ns = t_handoff_start.elapsed().as_nanos() as u64;

        if fanout.successful_dispatches > 0 {
            self.total_handoffs.fetch_add(1, Ordering::Relaxed);
            if any_reused {
                self.connection_reused_count.fetch_add(1, Ordering::Relaxed);
            }
            let ack_time_ns = t_total_start.elapsed().as_nanos() as u64;
            return Ok(SubmissionAck {
                signature: sig,
                ack_time_ns,
                route_name: self.route_name.clone(),
                handshake_time_ns: if any_reused {
                    Some(0)
                } else {
                    Some(handoff_time_ns / 2)
                },
                handoff_time_ns: Some(handoff_time_ns),
                target_leader: Some(primary_leader),
                tpu_socket: fanout
                    .fastest_tpu
                    .or(primary_socket)
                    .map(|s| s.to_string()),
                connection_reused: any_reused,
                fallback_triggered: false,
            });
        }

        if self.fallback_policy == FallbackPolicy::QuicOnly {
            return Err(RouteError::Transport(
                "Multi-leader QUIC fanout failed to deliver".into(),
            ));
        }

        let mut fallback_ack = self.fallback_route.submit_transaction(tx_base64).await?;
        fallback_ack.fallback_triggered = true;
        fallback_ack.target_leader = Some(primary_leader);
        fallback_ack.route_name = format!("{}_rpc_fallback", self.route_name);
        Ok(fallback_ack)
    }

    async fn poll_status(
        &self,
        signature: &str,
        timeout_ms: u64,
    ) -> Result<(ExecutionStatus, Option<u64>), RouteError> {
        self.fallback_route.poll_status(signature, timeout_ms).await
    }
}
