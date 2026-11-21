use crate::fee_calculator::{PriorityFeeQuote, LAMPORTS_PER_SIGNATURE};
use base64::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use solana_hash::Hash;
use solana_instruction::{AccountMeta, Instruction};
use solana_keypair::{Keypair, Signer};
use solana_message::Message;
use solana_pubkey::Pubkey;
use solana_transaction::Transaction;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// Official Solana System Program ID (`11111111111111111111111111111111`).
pub const SYSTEM_PROGRAM_ID_STR: &str = "11111111111111111111111111111111";

/// Maximum serialized wire packet size accepted by Solana QUIC TPU (`PACKET_DATA_SIZE`).
pub const MAX_SOLANA_PACKET_BYTES: usize = 1232;

/// Signed and serialized Solana wire transaction ready for QUIC TPU or RPC dispatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuiltTransaction {
    pub raw_bytes: Vec<u8>,
    pub base64_payload: String,
    pub signature_base58: String,
    pub payer_pubkey: String,
    pub instruction_count: usize,
    pub compute_unit_limit: Option<u32>,
    pub micro_lamports_per_cu: Option<u64>,
    pub total_fee_lamports: u64,
    pub packet_bytes_len: usize,
    pub build_time_micros: u64,
}

/// Encodes a standard Solana System Program `Transfer` instruction (index `2u32` LE + `lamports` LE).
pub fn system_transfer_instruction(from: &Pubkey, to: &Pubkey, lamports: u64) -> Instruction {
    let system_program = Pubkey::new_from_array([0u8; 32]);
    let mut data = Vec::with_capacity(12);
    data.extend_from_slice(&2u32.to_le_bytes());
    data.extend_from_slice(&lamports.to_le_bytes());

    Instruction {
        program_id: system_program,
        accounts: vec![AccountMeta::new(*from, true), AccountMeta::new(*to, false)],
        data,
    }
}

/// Generic Solana transaction constructor and Ed25519 signer with cached blockhash decoding.
pub struct TxBuilder {
    decode_cache: Arc<RwLock<(String, Hash)>>,
}

impl Default for TxBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TxBuilder {
    pub fn new() -> Self {
        Self {
            decode_cache: Arc::new(RwLock::new((String::new(), Hash::default()))),
        }
    }

    /// Parses a Base58 blockhash or derives a deterministic 32-byte hash for local fixture labels.
    pub fn parse_or_fixture_blockhash(blockhash_str: &str) -> Result<Hash, String> {
        if let Ok(h) = Hash::from_str(blockhash_str) {
            return Ok(h);
        }
        if blockhash_str.starts_with("Local") {
            let digest = Sha256::digest(blockhash_str.as_bytes());
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&digest);
            return Ok(Hash::new_from_array(arr));
        }
        Err(format!("Invalid base58 recent blockhash: {}", blockhash_str))
    }

    /// Decodes a Base58 blockhash with an in-memory cache hit when the hash has not rotated.
    pub async fn resolve_blockhash(&self, blockhash_str: &str) -> Result<Hash, String> {
        {
            let cached = self.decode_cache.read().await;
            if !cached.0.is_empty() && cached.0 == blockhash_str {
                return Ok(cached.1.clone());
            }
        }
        let parsed = Self::parse_or_fixture_blockhash(blockhash_str)?;
        {
            let mut write = self.decode_cache.write().await;
            *write = (blockhash_str.to_string(), parsed.clone());
        }
        Ok(parsed)
    }

    /// Compiles and signs an arbitrary bundle of Solana instructions, optionally prepending
    /// `ComputeBudget` (`0x02` limit + `0x03` micro-lamports price) instructions.
    pub fn build_signed_bundle(
        signer: &Keypair,
        instructions: &[Instruction],
        recent_blockhash: Hash,
        fee_quote: Option<&PriorityFeeQuote>,
    ) -> Result<BuiltTransaction, String> {
        if instructions.is_empty() {
            return Err("Instruction bundle cannot be empty".to_string());
        }

        let t0 = Instant::now();
        let payer = signer.pubkey();

        let mut all_instructions = Vec::with_capacity(instructions.len() + 2);
        if let Some(quote) = fee_quote {
            let [limit_ix, price_ix] = quote.build_compute_budget_instructions();
            all_instructions.push(limit_ix);
            all_instructions.push(price_ix);
        }
        all_instructions.extend_from_slice(instructions);

        let message = Message::new(&all_instructions, Some(&payer));
        let tx = Transaction::new(&[signer], message, recent_blockhash);

        let raw_bytes = bincode::serialize(&tx)
            .map_err(|e| format!("Failed to serialize wire transaction: {}", e))?;

        if raw_bytes.len() > MAX_SOLANA_PACKET_BYTES {
            return Err(format!(
                "Serialized transaction ({} bytes) exceeds Solana QUIC TPU MTU ({} bytes)",
                raw_bytes.len(),
                MAX_SOLANA_PACKET_BYTES
            ));
        }

        let signature_base58 = tx
            .signatures
            .first()
            .map(|s| bs58::encode(s.as_ref()).into_string())
            .unwrap_or_default();
        let base64_payload = base64::engine::general_purpose::STANDARD.encode(&raw_bytes);
        let build_time_micros = t0.elapsed().as_micros() as u64;

        let total_fee_lamports = fee_quote
            .map(|q| q.total_fee_lamports)
            .unwrap_or(LAMPORTS_PER_SIGNATURE);

        Ok(BuiltTransaction {
            packet_bytes_len: raw_bytes.len(),
            raw_bytes,
            base64_payload,
            signature_base58,
            payer_pubkey: payer.to_string(),
            instruction_count: all_instructions.len(),
            compute_unit_limit: fee_quote.map(|q| q.compute_unit_limit),
            micro_lamports_per_cu: fee_quote.map(|q| q.micro_lamports_per_cu),
            total_fee_lamports,
            build_time_micros,
        })
    }

    /// Builds and signs a System Program SOL transfer with optional ComputeBudget priority fee headers.
    pub fn build_system_transfer(
        signer: &Keypair,
        recipient: &Pubkey,
        lamports: u64,
        recent_blockhash: Hash,
        fee_quote: Option<&PriorityFeeQuote>,
    ) -> Result<BuiltTransaction, String> {
        let payer = signer.pubkey();
        let ix = system_transfer_instruction(&payer, recipient, lamports);
        Self::build_signed_bundle(signer, &[ix], recent_blockhash, fee_quote)
    }

    /// Loads a Solana keypair from a JSON byte-array file (`[u8; 64]` or `[u8; 32]`).
    /// Never prints or logs secret key material.
    pub fn load_keypair_from_file(path: &Path) -> Result<Keypair, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read keypair file {}: {}", path.display(), e))?;
        let bytes: Vec<u8> = serde_json::from_str(&content)
            .map_err(|e| format!("Invalid keypair JSON in {}: {}", path.display(), e))?;

        if bytes.len() != 64 && bytes.len() != 32 {
            return Err(format!(
                "Invalid keypair length in {}: expected 64 or 32 bytes, got {}",
                path.display(),
                bytes.len()
            ));
        }

        let mut seed = [0u8; 32];
        seed.copy_from_slice(&bytes[0..32]);
        Ok(Keypair::new_from_array(seed))
    }

    /// Resolves keypair from explicit CLI flag, `ROUTER_KEYPAIR` / `TESTNET_KEYPAIR` env var,
    /// or generates an ephemeral in-memory `Keypair::new()` fallback.
    pub fn resolve_keypair(explicit_path: Option<&str>) -> Result<(Keypair, String, String), String> {
        if let Some(path_str) = explicit_path {
            let path = Path::new(path_str);
            let kp = Self::load_keypair_from_file(path)?;
            let pubkey = kp.pubkey().to_string();
            return Ok((kp, pubkey, format!("file:{}", path.display())));
        }

        if let Ok(env_path) =
            std::env::var("ROUTER_KEYPAIR").or_else(|_| std::env::var("TESTNET_KEYPAIR"))
        {
            let trimmed = env_path.trim();
            if !trimmed.is_empty() {
                let path = Path::new(trimmed);
                let kp = Self::load_keypair_from_file(path)?;
                let pubkey = kp.pubkey().to_string();
                return Ok((kp, pubkey, format!("env:{}", path.display())));
            }
        }

        let kp = Keypair::new();
        let pubkey = kp.pubkey().to_string();
        Ok((kp, pubkey, "ephemeral-generated".to_string()))
    }

    /// Queries account balance in lamports over HTTP JSON-RPC.
    pub async fn get_balance(rpc_url: &str, pubkey_b58: &str) -> Result<u64, String> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap_or_default();

        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [pubkey_b58, {"commitment": "confirmed"}]
        });

        let resp = client
            .post(rpc_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json::<serde_json::Value>()
            .await
            .map_err(|e| e.to_string())?;

        if let Some(err) = resp.get("error") {
            return Err(err.to_string());
        }

        resp.get("result")
            .and_then(|r| r.get("value"))
            .and_then(|v| v.as_u64())
            .ok_or_else(|| "Missing result.value in getBalance response".to_string())
    }

    /// Requests a bounded devnet/testnet airdrop over HTTP JSON-RPC.
    pub async fn request_airdrop(
        rpc_url: &str,
        pubkey_b58: &str,
        lamports: u64,
    ) -> Result<String, String> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "requestAirdrop",
            "params": [pubkey_b58, lamports, {"commitment": "confirmed"}]
        });

        let resp = client
            .post(rpc_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json::<serde_json::Value>()
            .await
            .map_err(|e| e.to_string())?;

        if let Some(err) = resp.get("error") {
            return Err(err.to_string());
        }

        resp.get("result")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| "Missing result signature in requestAirdrop response".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::SlotProgress;
    use crate::fee_calculator::{
        AccountContentionProfile, PriorityFeeCalculator, UrgencyTier,
    };
    use crate::types::{Slot, SlotDuration};

    #[test]
    fn test_system_transfer_and_compute_budget_bundle_roundtrip() {
        let sender = Keypair::new();
        let recipient = Keypair::new().pubkey();
        let blockhash = Hash::new_from_array([9u8; 32]);

        let calc = PriorityFeeCalculator::default();
        let progress = SlotProgress {
            slot: Slot(200),
            slot_start: Instant::now(),
            duration: SlotDuration::MS_400,
            elapsed_ms: 80,
            remaining_ms: 320,
            progress: 0.20,
            is_measured: true,
        };
        let quote = calc.quote(
            45_000,
            0,
            UrgencyTier::Normal,
            &AccountContentionProfile::default(),
            &progress,
            1,
        );

        let built = TxBuilder::build_system_transfer(
            &sender,
            &recipient,
            100_000,
            blockhash.clone(),
            Some(&quote),
        )
        .expect("System transfer with compute budget should build cleanly");

        assert_eq!(built.instruction_count, 3);
        assert!(built.packet_bytes_len > 150 && built.packet_bytes_len <= MAX_SOLANA_PACKET_BYTES);
        assert!(!built.signature_base58.is_empty());

        // Round-trip deserialize wire bytes via bincode
        let decoded: Transaction =
            bincode::deserialize(&built.raw_bytes).expect("Wire bytes must deserialize");
        assert_eq!(decoded.signatures.len(), 1);
        assert_eq!(decoded.message.instructions.len(), 3);
        assert_eq!(decoded.message.recent_blockhash, blockhash);
        // Instruction 0 = SetComputeUnitLimit (0x02), Instruction 1 = SetComputeUnitPrice (0x03)
        assert_eq!(decoded.message.instructions[0].data[0], 0x02);
        assert_eq!(decoded.message.instructions[1].data[0], 0x03);
    }

    #[test]
    fn test_self_transfer_deduplicates_payer_account() {
        let sender = Keypair::new();
        let payer_pk = sender.pubkey();
        let blockhash = Hash::new_from_array([3u8; 32]);

        let built =
            TxBuilder::build_system_transfer(&sender, &payer_pk, 5_000, blockhash, None).unwrap();
        let decoded: Transaction = bincode::deserialize(&built.raw_bytes).unwrap();
        // Only [payer, system_program] in account_keys when transferring to self
        assert_eq!(decoded.message.account_keys.len(), 2);
    }

    #[test]
    fn test_oversized_packet_rejected_by_mtu_guard() {
        let sender = Keypair::new();
        let prog = Pubkey::new_from_array([5u8; 32]);
        let huge_ix = Instruction {
            program_id: prog,
            accounts: vec![AccountMeta::new(sender.pubkey(), true)],
            data: vec![0xAA; 1_300],
        };
        let err = TxBuilder::build_signed_bundle(&sender, &[huge_ix], Hash::default(), None)
            .expect_err("Oversized packet > 1232 bytes must fail");
        assert!(err.contains("exceeds Solana QUIC TPU MTU"));
    }
}
