use solana_hash::Hash;
use solana_keypair::{Keypair, Signer};
use solana_tx_router::{
    circuit_breaker::{CircuitBreakerAction, DeadZoneCircuitBreaker},
    clock::SlotClock,
    fee_calculator::{AccountContentionProfile, PriorityFeeCalculator, UrgencyTier},
    tpu_quic::{socket_addr_to_quic_sni, TpuQuicRouter},
    tx_builder::TxBuilder,
    types::{Slot, SlotDuration},
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1. Simulate arriving late in Slot 4 (365ms into a 400ms slot -> 35ms left in window)
    let mut clock = SlotClock::new(SlotDuration::MS_400);
    clock.on_slot_observed_at(Slot(448_160_003), Instant::now() - Duration::from_millis(365));

    let clock_arc = Arc::new(RwLock::new(clock));
    let breaker = DeadZoneCircuitBreaker::new(clock_arc.clone(), 120);

    let decision = breaker.evaluate().await;
    match &decision {
        CircuitBreakerAction::HoldForNextLeader {
            current_slot,
            target_slot,
            hold_duration_ms,
            ..
        } => {
            println!(
                "[breaker] Slot {} (4/4) is in dead zone -> holding {}ms for next leader Slot {}",
                current_slot, hold_duration_ms, target_slot
            );
        }
        CircuitBreakerAction::DispatchImmediately { slot, .. } => {
            println!("[breaker] Slot {} is safe to send immediately", slot);
        }
    }

    // 2. Advance clock to fresh Slot 1 of the next 4-slot leader window
    {
        let mut guard = clock_arc.write().await;
        guard.on_slot_observed_at(Slot(448_160_004), Instant::now() - Duration::from_millis(25));
    }
    let fresh_progress = clock_arc.read().await.progress();

    // 3. Compute dynamic CU limit + priority fee quote
    let fee_calc = PriorityFeeCalculator::new(15_000, 300_000, 120);
    let contention = AccountContentionProfile {
        writable_accounts: 3,
        hot_account_bps: 1_500,
        observed_p75_micro_lamports: Some(20_000),
    };
    let quote = fee_calc.quote(
        120_000,
        1_000,
        UrgencyTier::High,
        &contention,
        &fresh_progress,
        1,
    );

    println!(
        "[fee-calc] CU limit={} | price={} micro-lamports/CU | total fee={} lamports",
        quote.compute_unit_limit, quote.micro_lamports_per_cu, quote.total_fee_lamports
    );

    // 4. Build and sign a Solana transfer bundle with 0x02 + 0x03 ComputeBudget headers
    let payer = Keypair::new();
    let recipient = Keypair::new().pubkey();
    let blockhash = Hash::new_from_array([7u8; 32]);

    let signed_bundle = TxBuilder::build_system_transfer(
        &payer,
        &recipient,
        250_000,
        blockhash,
        Some(&quote),
    )?;

    println!(
        "[tx-builder] Signed by {} | sig={} | wire={} bytes ({}us)",
        payer.pubkey(),
        signed_bundle.signature_base58,
        signed_bundle.packet_bytes_len,
        signed_bundle.build_time_micros
    );

    // 5. Initialize 4-lane striped QUIC TPU router
    let router = TpuQuicRouter::new("https://api.devnet.solana.com")?;
    let leader_tpu: SocketAddr = "127.0.0.1:8009".parse()?;
    println!(
        "[quic-router] Initialized {} striped Ed25519 X.509 lanes | SNI={}",
        router.striped_lane_count(),
        socket_addr_to_quic_sni(leader_tpu)
    );

    Ok(())
}
