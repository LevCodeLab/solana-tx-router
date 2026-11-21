use crate::clock::SlotProgress;
use serde::{Deserialize, Serialize};
use solana_instruction::Instruction;
use solana_pubkey::Pubkey;
use std::str::FromStr;

/// Official Solana ComputeBudget native program address.
pub const COMPUTE_BUDGET_PROGRAM_ID_STR: &str = "ComputeBudget111111111111111111111111111111";

/// Discriminator byte for `ComputeBudgetInstruction::SetComputeUnitLimit(u32)`.
pub const IX_SET_COMPUTE_UNIT_LIMIT: u8 = 0x02;

/// Discriminator byte for `ComputeBudgetInstruction::SetComputeUnitPrice(u64)`.
pub const IX_SET_COMPUTE_UNIT_PRICE: u8 = 0x03;

/// Discriminator byte for `ComputeBudgetInstruction::SetLoadedAccountsDataSizeLimit(u32)`.
pub const IX_SET_LOADED_ACCOUNTS_DATA_SIZE_LIMIT: u8 = 0x04;

/// Maximum per-transaction compute unit cap enforced by Solana runtime.
pub const MAX_TRANSACTION_COMPUTE_UNITS: u32 = 1_400_000;

/// Standard lamports charged per Ed25519 signature.
pub const LAMPORTS_PER_SIGNATURE: u64 = 5_000;

#[inline]
pub fn compute_budget_program_id() -> Pubkey {
    Pubkey::from_str(COMPUTE_BUDGET_PROGRAM_ID_STR).expect("valid ComputeBudget program id")
}

/// Encodes `SetComputeUnitLimit` (`0x02` + 4-byte little-endian `u32`).
pub fn encode_set_compute_unit_limit(units: u32) -> Instruction {
    let clamped = units.clamp(1_000, MAX_TRANSACTION_COMPUTE_UNITS);
    let mut data = Vec::with_capacity(5);
    data.push(IX_SET_COMPUTE_UNIT_LIMIT);
    data.extend_from_slice(&clamped.to_le_bytes());

    Instruction {
        program_id: compute_budget_program_id(),
        accounts: Vec::new(),
        data,
    }
}

/// Encodes `SetComputeUnitPrice` (`0x03` + 8-byte little-endian `u64` micro-lamports per CU).
pub fn encode_set_compute_unit_price(micro_lamports_per_cu: u64) -> Instruction {
    let mut data = Vec::with_capacity(9);
    data.push(IX_SET_COMPUTE_UNIT_PRICE);
    data.extend_from_slice(&micro_lamports_per_cu.to_le_bytes());

    Instruction {
        program_id: compute_budget_program_id(),
        accounts: Vec::new(),
        data,
    }
}

/// Encodes `SetLoadedAccountsDataSizeLimit` (`0x04` + 4-byte little-endian `u32`).
pub fn encode_set_loaded_accounts_data_size_limit(bytes: u32) -> Instruction {
    let mut data = Vec::with_capacity(5);
    data.push(IX_SET_LOADED_ACCOUNTS_DATA_SIZE_LIMIT);
    data.extend_from_slice(&bytes.to_le_bytes());

    Instruction {
        program_id: compute_budget_program_id(),
        accounts: Vec::new(),
        data,
    }
}

/// Execution urgency tier selected by caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UrgencyTier {
    Low,
    Normal,
    High,
    Critical,
}

impl UrgencyTier {
    pub fn multiplier(self) -> f64 {
        match self {
            Self::Low => 0.75,
            Self::Normal => 1.00,
            Self::High => 1.60,
            Self::Critical => 2.50,
        }
    }

    pub fn parse_label(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "low" => Self::Low,
            "high" => Self::High,
            "critical" | "urgent" | "max" => Self::Critical,
            _ => Self::Normal,
        }
    }
}

impl std::fmt::Display for UrgencyTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Low => write!(f, "LOW"),
            Self::Normal => write!(f, "NORMAL"),
            Self::High => write!(f, "HIGH"),
            Self::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// Slot window phase derived from current monotonic slot progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SlotPhase {
    /// Slots 1..2 with <= 60% elapsed: clean banking stage intake.
    EarlyWindow,
    /// Mid-window or Slot 3: normal banking stage queue depth.
    MidWindow,
    /// > 85% elapsed inside Slots 1..3: tail of banking stage window.
    LateSlot,
    /// Late Slot 4 near leader rotation boundary: hold for next leader Slot 1 instead of overpaying.
    Slot4DeadZone,
}

impl std::fmt::Display for SlotPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EarlyWindow => write!(f, "EARLY_WINDOW"),
            Self::MidWindow => write!(f, "MID_WINDOW"),
            Self::LateSlot => write!(f, "LATE_SLOT"),
            Self::Slot4DeadZone => write!(f, "SLOT_4_DEAD_ZONE"),
        }
    }
}

/// Account write-lock contention descriptor for local fee-market estimation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountContentionProfile {
    pub writable_accounts: usize,
    /// Basis points (0..=10_000) representing write-lock contention on shared state accounts.
    pub hot_account_bps: u32,
    /// Optional observed local fee-market p75 in micro-lamports per CU.
    pub observed_p75_micro_lamports: Option<u64>,
}

impl Default for AccountContentionProfile {
    fn default() -> Self {
        Self {
            writable_accounts: 2,
            hot_account_bps: 0,
            observed_p75_micro_lamports: None,
        }
    }
}

/// Output quote from `PriorityFeeCalculator`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityFeeQuote {
    pub compute_unit_limit: u32,
    pub micro_lamports_per_cu: u64,
    pub priority_fee_lamports: u64,
    pub base_signature_fee_lamports: u64,
    pub total_fee_lamports: u64,
    pub urgency: UrgencyTier,
    pub slot_phase: SlotPhase,
    pub slot_in_window: u8,
    pub hold_for_next_leader: bool,
    pub rationale: String,
}

impl PriorityFeeQuote {
    /// Returns `[SetComputeUnitLimit, SetComputeUnitPrice]` ready to prepend to any transaction message.
    pub fn build_compute_budget_instructions(&self) -> [Instruction; 2] {
        [
            encode_set_compute_unit_limit(self.compute_unit_limit),
            encode_set_compute_unit_price(self.micro_lamports_per_cu),
        ]
    }
}

/// Computes exact priority fee lamports charged by Agave runtime:
/// `ceil((compute_unit_limit * micro_lamports_per_cu) / 1_000_000)`.
#[inline]
pub fn calculate_priority_fee_lamports(compute_unit_limit: u32, micro_lamports_per_cu: u64) -> u64 {
    let total_micro = (compute_unit_limit as u128) * (micro_lamports_per_cu as u128);
    total_micro.div_ceil(1_000_000) as u64
}

/// Dynamic Compute-Unit priority fee calculator aware of slot phase, urgency, and account contention.
#[derive(Debug, Clone)]
pub struct PriorityFeeCalculator {
    pub base_micro_lamports_per_cu: u64,
    pub max_micro_lamports_per_cu: u64,
    pub dead_zone_threshold_ms: u64,
}

impl Default for PriorityFeeCalculator {
    fn default() -> Self {
        Self {
            base_micro_lamports_per_cu: 15_000,
            max_micro_lamports_per_cu: 500_000,
            dead_zone_threshold_ms: 120,
        }
    }
}

impl PriorityFeeCalculator {
    pub fn new(
        base_micro_lamports_per_cu: u64,
        max_micro_lamports_per_cu: u64,
        dead_zone_threshold_ms: u64,
    ) -> Self {
        Self {
            base_micro_lamports_per_cu,
            max_micro_lamports_per_cu: max_micro_lamports_per_cu.max(base_micro_lamports_per_cu),
            dead_zone_threshold_ms,
        }
    }

    /// Evaluates optimal CU limit and priority fee price given slot progress, urgency, and contention.
    pub fn quote(
        &self,
        estimated_compute_units: u32,
        cu_headroom_bps: u32,
        urgency: UrgencyTier,
        contention: &AccountContentionProfile,
        progress: &SlotProgress,
        signature_count: u64,
    ) -> PriorityFeeQuote {
        let headroom_units = ((estimated_compute_units as u64) * (cu_headroom_bps as u64)) / 10_000;
        let compute_unit_limit = estimated_compute_units
            .saturating_add(headroom_units as u32)
            .clamp(1_000, MAX_TRANSACTION_COMPUTE_UNITS);

        let slot_u64 = progress.slot.as_u64();
        let slot_in_window = ((slot_u64 % 4) + 1) as u8;

        // Scale dead-zone threshold if slot duration is 200ms vs 400ms
        let effective_dz_ms = if self.dead_zone_threshold_ms >= 100 && progress.duration.as_millis() <= 200 {
            40
        } else {
            self.dead_zone_threshold_ms
        };

        let (slot_phase, phase_multiplier, hold_for_next_leader) =
            if slot_in_window == 4 && progress.remaining_ms <= effective_dz_ms {
                // Do not spike priority fees into a closing Slot 4 bank; hold for next leader Slot 1!
                (SlotPhase::Slot4DeadZone, 1.05, true)
            } else if progress.progress > 0.85 {
                (SlotPhase::LateSlot, 1.35, false)
            } else if slot_in_window <= 2 && progress.progress <= 0.60 {
                (SlotPhase::EarlyWindow, 1.00, false)
            } else {
                (SlotPhase::MidWindow, 1.15, false)
            };

        let anchor_micro_lamports = contention
            .observed_p75_micro_lamports
            .unwrap_or(self.base_micro_lamports_per_cu)
            .max(self.base_micro_lamports_per_cu);

        let extra_writable = contention.writable_accounts.saturating_sub(1) as f64;
        let contention_multiplier = 1.0
            + (extra_writable * 0.08)
            + ((contention.hot_account_bps.min(10_000) as f64) / 10_000.0);

        let raw_price = (anchor_micro_lamports as f64)
            * urgency.multiplier()
            * phase_multiplier
            * contention_multiplier;

        let micro_lamports_per_cu = (raw_price.round() as u64).min(self.max_micro_lamports_per_cu);
        let priority_fee_lamports =
            calculate_priority_fee_lamports(compute_unit_limit, micro_lamports_per_cu);
        let base_signature_fee_lamports = signature_count.max(1) * LAMPORTS_PER_SIGNATURE;
        let total_fee_lamports = base_signature_fee_lamports.saturating_add(priority_fee_lamports);

        let rationale = if hold_for_next_leader {
            format!(
                "Slot 4/4 dead zone ({}ms left <= {}ms threshold): hold tx for next leader Slot 1 and bid clean {} micro-lamports/CU instead of burning panic fees",
                progress.remaining_ms, effective_dz_ms, micro_lamports_per_cu
            )
        } else {
            format!(
                "Slot {}/4 ({}) | Urgency={} ({:.2}x) | Writable={} (contention {:.2}x) -> {} micro-lamports/CU ({} lamports priority fee)",
                slot_in_window,
                slot_phase,
                urgency,
                urgency.multiplier(),
                contention.writable_accounts,
                contention_multiplier,
                micro_lamports_per_cu,
                priority_fee_lamports
            )
        };

        PriorityFeeQuote {
            compute_unit_limit,
            micro_lamports_per_cu,
            priority_fee_lamports,
            base_signature_fee_lamports,
            total_fee_lamports,
            urgency,
            slot_phase,
            slot_in_window,
            hold_for_next_leader,
            rationale,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Slot, SlotDuration};
    use std::time::Instant;

    #[test]
    fn test_compute_budget_instruction_wire_encoding() {
        let cu_ix = encode_set_compute_unit_limit(150_000);
        assert_eq!(cu_ix.program_id.to_string(), COMPUTE_BUDGET_PROGRAM_ID_STR);
        assert!(cu_ix.accounts.is_empty());
        assert_eq!(cu_ix.data.len(), 5);
        assert_eq!(cu_ix.data[0], 0x02);
        assert_eq!(
            u32::from_le_bytes(cu_ix.data[1..5].try_into().unwrap()),
            150_000
        );

        let price_ix = encode_set_compute_unit_price(25_000);
        assert_eq!(price_ix.program_id.to_string(), COMPUTE_BUDGET_PROGRAM_ID_STR);
        assert!(price_ix.accounts.is_empty());
        assert_eq!(price_ix.data.len(), 9);
        assert_eq!(price_ix.data[0], 0x03);
        assert_eq!(
            u64::from_le_bytes(price_ix.data[1..9].try_into().unwrap()),
            25_000
        );
    }

    #[test]
    fn test_priority_fee_ceiling_and_dead_zone_protection() {
        // 150,000 CU * 25,000 micro-lamports = 3,750,000,000 micro-lamports = 3,750 lamports
        assert_eq!(calculate_priority_fee_lamports(150_000, 25_000), 3_750);
        // Ceiling rounding check: 1_000 CU * 1 micro-lamport -> 1 lamport
        assert_eq!(calculate_priority_fee_lamports(1_000, 1), 1);

        let calc = PriorityFeeCalculator::new(20_000, 250_000, 120);
        let contention = AccountContentionProfile {
            writable_accounts: 3,
            hot_account_bps: 2_000,
            observed_p75_micro_lamports: None,
        };

        // Slot 103 is Slot 4 in 4-slot window ((103 % 4) + 1 == 4), with 45ms remaining -> dead zone!
        let dz_progress = SlotProgress {
            slot: Slot(103),
            slot_start: Instant::now(),
            duration: SlotDuration::MS_400,
            elapsed_ms: 355,
            remaining_ms: 45,
            progress: 0.887,
            is_measured: true,
        };

        let quote = calc.quote(
            120_000,
            1_000, // +10% CU headroom -> 132,000 CU
            UrgencyTier::High,
            &contention,
            &dz_progress,
            1,
        );
        assert_eq!(quote.compute_unit_limit, 132_000);
        assert_eq!(quote.slot_phase, SlotPhase::Slot4DeadZone);
        assert!(quote.hold_for_next_leader);
        assert!(quote.micro_lamports_per_cu <= 250_000);
        assert_eq!(
            quote.total_fee_lamports,
            5_000 + quote.priority_fee_lamports
        );
    }
}
