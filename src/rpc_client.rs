use crate::types::{LeaderId, Slot};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RpcClientError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("JSON RPC error: {0}")]
    JsonRpc(String),
    #[error("Invalid payload format: {0}")]
    InvalidPayload(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochInfo {
    pub absolute_slot: u64,
    pub block_height: u64,
    pub epoch: u64,
    pub slot_index: u64,
    pub slots_in_epoch: u64,
    pub transaction_count: Option<u64>,
}

/// Lightweight HTTP JSON-RPC client for Solana cluster queries.
pub struct SolanaRpcClient {
    client: Client,
    rpc_url: String,
}

impl SolanaRpcClient {
    pub fn new(rpc_url: impl Into<String>) -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(6))
                .build()
                .unwrap_or_default(),
            rpc_url: rpc_url.into(),
        }
    }

    pub fn rpc_url(&self) -> &str {
        &self.rpc_url
    }

    pub async fn get_slot(&self) -> Result<Slot, RpcClientError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSlot",
            "params": [{"commitment": "processed"}]
        });

        let resp = self.send_rpc(&payload).await?;
        let slot = resp
            .as_u64()
            .ok_or_else(|| RpcClientError::InvalidPayload("Expected u64 slot".into()))?;
        Ok(Slot(slot))
    }

    pub async fn get_slot_leader(&self) -> Result<LeaderId, RpcClientError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSlotLeader"
        });

        let resp = self.send_rpc(&payload).await?;
        let leader_str = resp
            .as_str()
            .ok_or_else(|| RpcClientError::InvalidPayload("Expected leader string".into()))?;
        Ok(LeaderId::new(leader_str))
    }

    pub async fn get_slot_leaders(
        &self,
        start_slot: u64,
        limit: u64,
    ) -> Result<Vec<LeaderId>, RpcClientError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSlotLeaders",
            "params": [start_slot, limit]
        });

        let resp = self.send_rpc(&payload).await?;
        let array = resp
            .as_array()
            .ok_or_else(|| RpcClientError::InvalidPayload("Expected array of leaders".into()))?;

        let leaders = array
            .iter()
            .filter_map(|v| v.as_str().map(LeaderId::new))
            .collect();
        Ok(leaders)
    }

    pub async fn get_epoch_info(&self) -> Result<EpochInfo, RpcClientError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getEpochInfo",
            "params": [{"commitment": "processed"}]
        });

        let resp = self.send_rpc(&payload).await?;
        Ok(EpochInfo {
            epoch: resp.get("epoch").and_then(|v| v.as_u64()).unwrap_or(0),
            absolute_slot: resp
                .get("absoluteSlot")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            block_height: resp
                .get("blockHeight")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            slot_index: resp.get("slotIndex").and_then(|v| v.as_u64()).unwrap_or(0),
            slots_in_epoch: resp
                .get("slotsInEpoch")
                .and_then(|v| v.as_u64())
                .unwrap_or(432_000),
            transaction_count: resp.get("transactionCount").and_then(|v| v.as_u64()),
        })
    }

    pub async fn get_latest_blockhash(&self) -> Result<(String, u64), RpcClientError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getLatestBlockhash",
            "params": [{"commitment": "processed"}]
        });

        let resp = self.send_rpc(&payload).await?;
        let blockhash = resp
            .get("value")
            .and_then(|v| v.get("blockhash"))
            .and_then(|b| b.as_str())
            .ok_or_else(|| {
                RpcClientError::InvalidPayload("Missing blockhash in response".into())
            })?;
        let last_valid_block_height = resp
            .get("value")
            .and_then(|v| v.get("lastValidBlockHeight"))
            .and_then(|h| h.as_u64())
            .unwrap_or(0);

        Ok((blockhash.to_string(), last_valid_block_height))
    }

    pub async fn get_leader_schedule(
        &self,
        slot: Slot,
    ) -> Result<HashMap<Slot, LeaderId>, RpcClientError> {
        let epoch_start_slot = match self.get_epoch_info().await {
            Ok(info) => info.absolute_slot.saturating_sub(info.slot_index),
            Err(_) => (slot.as_u64() / 432_000) * 432_000,
        };

        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getLeaderSchedule",
            "params": [slot.as_u64(), {"commitment": "processed"}]
        });

        let resp = self.send_rpc(&payload).await?;
        let mut schedule = HashMap::new();

        if let Some(map) = resp.as_object() {
            for (leader_pubkey, slots) in map {
                if let Some(slot_indices) = slots.as_array() {
                    for idx in slot_indices {
                        if let Some(i) = idx.as_u64() {
                            schedule.insert(
                                Slot(epoch_start_slot + i),
                                LeaderId::new(leader_pubkey),
                            );
                        }
                    }
                }
            }
        }

        Ok(schedule)
    }

    async fn send_rpc(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, RpcClientError> {
        let resp = self
            .client
            .post(&self.rpc_url)
            .json(payload)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        if let Some(err) = resp.get("error") {
            return Err(RpcClientError::JsonRpc(err.to_string()));
        }

        resp.get("result")
            .cloned()
            .ok_or_else(|| {
                RpcClientError::InvalidPayload("Missing result field in RPC response".into())
            })
    }
}
