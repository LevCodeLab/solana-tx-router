use solana_hash::Hash;
use solana_keypair::{Keypair, Signer};
use solana_tx_router::{
    circuit_breaker::{CircuitBreakerAction, DeadZoneCircuitBreaker},
    clock::{SlotClock, SlotProgress},
    cluster_probe::ClusterProbe,
    decision::{ExecutionDecisionEngine, RoutingAction},
    experiment::{ExecutionStatus, ExperimentConfig, ExperimentGroup},
    explainer::ExecutionExplainer,
    fee_calculator::{AccountContentionProfile, PriorityFeeCalculator, SlotPhase, UrgencyTier},
    freshness::{BankFreshnessTier, FreshnessState, FreshnessTier},
    multi_leader_fanout::MultiLeaderFanoutEngine,
    route::LocalFixtureRoute,
    runner::BenchmarkRunner,
    stats::BenchmarkStats,
    storage::BenchmarkStorage,
    tpu_quic::{
        new_tpu_x509_client_certificate, socket_addr_to_quic_sni, TpuQuicRouter,
        QUIC_STRIPED_ENDPOINTS,
    },
    tx_builder::{TxBuilder, MAX_SOLANA_PACKET_BYTES},
    types::{unix_now_ms, Slot, SlotDuration},
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

#[test]
fn test_freshness_evaluation_matrix() {
    let now_ms = unix_now_ms();

    let fresh = FreshnessState::evaluate(
        50,
        250,
        Some("Leader_1".to_string()),
        Some("Leader_2".to_string()),
        "11111111111111111111111111111111".to_string(),
        now_ms,
        now_ms,
        Some("bank-1".to_string()),
        false,
    );
    assert_eq!(fresh.slot_tier, FreshnessTier::Fresh);
    assert_eq!(fresh.bank_tier, BankFreshnessTier::Canonical);
    assert_eq!(fresh.blockhash_tier, FreshnessTier::Fresh);

    let abandoned = FreshnessState::evaluate(
        150,
        250,
        Some("Leader_1".to_string()),
        Some("Leader_2".to_string()),
        "11111111111111111111111111111111".to_string(),
        now_ms,
        now_ms,
        Some("bank-abandoned".to_string()),
        true,
    );
    assert_eq!(abandoned.bank_tier, BankFreshnessTier::Abandoned);

    let stale_bh = FreshnessState::evaluate(
        50,
        250,
        Some("Leader_1".to_string()),
        Some("Leader_2".to_string()),
        "11111111111111111111111111111111".to_string(),
        1,
        now_ms,
        Some("bank-1".to_string()),
        false,
    );
    assert_eq!(stale_bh.blockhash_tier, FreshnessTier::Stale);
}

#[test]
fn test_decision_engine_rules() {
    let now_ms = unix_now_ms();

    let normal_fresh = FreshnessState::evaluate(
        50,
        250,
        Some("Leader_A".to_string()),
        Some("Leader_B".to_string()),
        "11111111111111111111111111111111".to_string(),
        now_ms,
        now_ms,
        Some("bank-1".to_string()),
        false,
    );
    let ctrl_dec = ExecutionDecisionEngine::evaluate_control(&normal_fresh);
    let router_dec = ExecutionDecisionEngine::evaluate_router_aware(&normal_fresh, true);
    assert_eq!(ctrl_dec.action, RoutingAction::Submit);
    assert_eq!(router_dec.action, RoutingAction::Submit);

    let handoff_imminent = FreshnessState::evaluate(
        230,
        250,
        Some("Leader_A".to_string()),
        Some("Leader_B".to_string()),
        "11111111111111111111111111111111".to_string(),
        now_ms,
        now_ms,
        Some("bank-1".to_string()),
        false,
    );
    let ctrl_handoff = ExecutionDecisionEngine::evaluate_control(&handoff_imminent);
    let router_handoff = ExecutionDecisionEngine::evaluate_router_aware(&handoff_imminent, true);
    assert_eq!(ctrl_handoff.action, RoutingAction::Submit);
    assert!(matches!(router_handoff.action, RoutingAction::Wait { .. }));

    let abandoned_state = FreshnessState::evaluate(
        50,
        250,
        Some("Leader_A".to_string()),
        Some("Leader_B".to_string()),
        "11111111111111111111111111111111".to_string(),
        now_ms,
        now_ms,
        Some("bank-dead".to_string()),
        true,
    );
    let router_abort = ExecutionDecisionEngine::evaluate_router_aware(&abandoned_state, true);
    assert!(matches!(router_abort.action, RoutingAction::Abort { .. }));
}

#[test]
fn test_fee_calculator_and_tx_builder_integration() {
    let sender = Keypair::new();
    let recipient = Keypair::new().pubkey();
    let blockhash = Hash::new_from_array([11u8; 32]);

    let progress = SlotProgress {
        slot: Slot(400),
        slot_start: Instant::now(),
        duration: SlotDuration::MS_400,
        elapsed_ms: 100,
        remaining_ms: 300,
        progress: 0.25,
        is_measured: true,
    };

    let calc = PriorityFeeCalculator::new(20_000, 300_000, 120);
    let contention = AccountContentionProfile {
        writable_accounts: 4,
        hot_account_bps: 2_500,
        observed_p75_micro_lamports: Some(25_000),
    };
    let quote = calc.quote(150_000, 1_000, UrgencyTier::High, &contention, &progress, 1);

    assert_eq!(quote.slot_phase, SlotPhase::EarlyWindow);
    assert!(!quote.hold_for_next_leader);
    assert_eq!(quote.compute_unit_limit, 165_000);
    assert!(quote.micro_lamports_per_cu >= 40_000);

    let signed_bundle =
        TxBuilder::build_system_transfer(&sender, &recipient, 75_000, blockhash, Some(&quote))
            .expect("Must build signed transfer with compute budget");

    assert_eq!(signed_bundle.payer_pubkey, sender.pubkey().to_string());
    assert_eq!(signed_bundle.instruction_count, 3);
    assert!(signed_bundle.packet_bytes_len <= MAX_SOLANA_PACKET_BYTES);
}

#[tokio::test]
async fn test_dead_zone_circuit_breaker_and_quic_router() {
    let mut clock = SlotClock::new(SlotDuration::MS_400);
    let late_slot_4_start = Instant::now() - std::time::Duration::from_millis(360);
    clock.on_slot_observed_at(Slot(103), late_slot_4_start);

    let breaker = DeadZoneCircuitBreaker::new(Arc::new(RwLock::new(clock)), 120);
    let decision = breaker.evaluate().await;
    assert!(matches!(
        decision,
        CircuitBreakerAction::HoldForNextLeader {
            target_slot: 104,
            ..
        }
    ));

    let router = TpuQuicRouter::new("https://api.devnet.solana.com").unwrap();
    assert_eq!(router.striped_lane_count(), QUIC_STRIPED_ENDPOINTS);

    let addr: SocketAddr = "127.0.0.1:8009".parse().unwrap();
    assert_eq!(socket_addr_to_quic_sni(addr), "127.0.0.1.8009.sol");

    let kp = Keypair::new();
    let (cert, _pk) = new_tpu_x509_client_certificate(&kp);
    assert!(!cert.as_ref().is_empty());
}

#[tokio::test]
async fn test_local_fixture_benchmark_runner_and_explainer() {
    let temp_dir = std::env::temp_dir().join(format!("solana-tx-router-test-{}", std::process::id()));
    let storage = BenchmarkStorage::new(&temp_dir);

    let config = ExperimentConfig {
        id: format!("test-exp-{}", std::process::id()),
        cluster: "local".to_string(),
        sample_count: 10,
        max_tps: 1000.0,
        interleave_mode: true,
        ..Default::default()
    };

    let runner = BenchmarkRunner::new(config.clone()).with_storage(BenchmarkStorage::new(&temp_dir));
    let control_route = Arc::new(LocalFixtureRoute::new(0));
    let router_route = Arc::new(LocalFixtureRoute::new(0));

    let (summary, samples) = runner
        .run_experiment(control_route, router_route)
        .await
        .expect("Experiment run should succeed");

    let loaded_summary = storage
        .load_summary(&config.id)
        .expect("Load summary should succeed");
    assert_eq!(loaded_summary.experiment_id, config.id);
    assert_eq!(samples.len(), 10);
    assert_eq!(summary.total_samples, 10);

    let ctrl_count = samples
        .iter()
        .filter(|s| s.group == ExperimentGroup::Control)
        .count();
    let router_count = samples
        .iter()
        .filter(|s| s.group == ExperimentGroup::RouterAware)
        .count();
    assert_eq!(ctrl_count, 5);
    assert_eq!(router_count, 5);
    assert!(samples.iter().all(|s| s.status == ExecutionStatus::Confirmed));

    let explanation = ExecutionExplainer::format_explanation(&samples[0]);
    assert!(explanation.contains("TX ROUTER EXECUTION EXPLAINER"));
    assert!(explanation.contains("Group:             CONTROL"));

    let mut latencies = vec![100.0, 110.0, 120.0, 130.0, 140.0, 150.0, 160.0, 170.0, 180.0, 200.0];
    assert_eq!(BenchmarkStats::percentile(&mut latencies, 0.50), 150.0);

    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn test_offline_probe_and_multi_leader_fanout() {
    let probe = ClusterProbe::devnet_default();
    let probe_res = probe.run_offline(448_160_003).await;
    assert_eq!(probe_res.slot_in_window, 4);
    assert!(probe_res.is_current_dead_zone);
    assert_eq!(probe_res.leader_probes.len(), 5);

    let fanout = MultiLeaderFanoutEngine::devnet_default(4);
    let fanout_res = fanout.simulate_offline(448_160_001).await;
    assert_eq!(fanout_res.fanout_size, 4);
    assert_eq!(fanout_res.leaders.len(), 4);
    assert!(fanout_res.concurrency_speedup > 1.5);
}
