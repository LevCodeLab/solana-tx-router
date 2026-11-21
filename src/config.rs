use serde::{Deserialize, Serialize};

/// Core runtime configuration for `solana-tx-router`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterConfig {
    pub rpc_url: String,
    pub ws_url: String,
    pub slot_duration_ms: u64,
    pub dead_zone_threshold_ms: u64,
    pub fanout_leaders: usize,
    pub default_compute_units: u32,
    pub base_micro_lamports_per_cu: u64,
    pub max_micro_lamports_per_cu: u64,
    pub dry_run: bool,
}

impl Default for RouterConfig {
    fn default() -> Self {
        let rpc_url = std::env::var("SOLANA_RPC_URL")
            .unwrap_or_else(|_| "https://api.devnet.solana.com".to_string());
        let ws_url = std::env::var("SOLANA_WS_URL")
            .unwrap_or_else(|_| Self::derive_ws_url(&rpc_url));

        Self {
            rpc_url,
            ws_url,
            slot_duration_ms: 400,
            dead_zone_threshold_ms: 120,
            fanout_leaders: 4,
            default_compute_units: 150_000,
            base_micro_lamports_per_cu: 15_000,
            max_micro_lamports_per_cu: 500_000,
            dry_run: true,
        }
    }
}

impl RouterConfig {
    pub fn local_validator() -> Self {
        Self {
            rpc_url: "http://127.0.0.1:8899".to_string(),
            ws_url: "ws://127.0.0.1:8900".to_string(),
            ..Default::default()
        }
    }

    pub fn devnet() -> Self {
        Self {
            rpc_url: "https://api.devnet.solana.com".to_string(),
            ws_url: "wss://api.devnet.solana.com".to_string(),
            ..Default::default()
        }
    }

    pub fn mainnet_public() -> Self {
        Self {
            rpc_url: "https://api.mainnet-beta.solana.com".to_string(),
            ws_url: "wss://api.mainnet-beta.solana.com".to_string(),
            ..Default::default()
        }
    }

    pub fn derive_ws_url(rpc_url: &str) -> String {
        if rpc_url == "http://127.0.0.1:8899" {
            "ws://127.0.0.1:8900".to_string()
        } else if rpc_url.starts_with("https://") {
            rpc_url.replacen("https://", "wss://", 1)
        } else if rpc_url.starts_with("http://") {
            rpc_url.replacen("http://", "ws://", 1)
        } else {
            format!("wss://{}", rpc_url)
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.rpc_url.trim().is_empty() {
            return Err("rpc_url cannot be empty".to_string());
        }
        if self.slot_duration_ms < 50 || self.slot_duration_ms > 2_000 {
            return Err("slot_duration_ms must be between 50ms and 2000ms".to_string());
        }
        if self.fanout_leaders == 0 || self.fanout_leaders > 16 {
            return Err("fanout_leaders must be between 1 and 16".to_string());
        }
        if self.default_compute_units == 0 || self.default_compute_units > 1_400_000 {
            return Err("default_compute_units must be within (0, 1_400_000]".to_string());
        }
        if self.base_micro_lamports_per_cu > self.max_micro_lamports_per_cu {
            return Err("base_micro_lamports_per_cu cannot exceed max_micro_lamports_per_cu".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_router_config_defaults_and_ws_derivation() {
        let cfg = RouterConfig::devnet();
        assert_eq!(cfg.rpc_url, "https://api.devnet.solana.com");
        assert_eq!(cfg.ws_url, "wss://api.devnet.solana.com");
        assert!(cfg.validate().is_ok());

        let local = RouterConfig::local_validator();
        assert_eq!(local.rpc_url, "http://127.0.0.1:8899");
        assert_eq!(local.ws_url, "ws://127.0.0.1:8900");
        assert!(local.validate().is_ok());
    }
}
