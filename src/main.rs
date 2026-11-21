use solana_keypair::{Keypair, Signer};
use solana_pubkey::Pubkey;
use solana_tx_router::{
    circuit_breaker::{CircuitBreakerAction, DeadZoneCircuitBreaker},
    clock::{SlotClock, SlotProgress},
    cluster_probe::ClusterProbe,
    config::RouterConfig,
    experiment::ExperimentConfig,
    explainer::ExecutionExplainer,
    fee_calculator::{
        encode_set_compute_unit_limit, encode_set_compute_unit_price, AccountContentionProfile,
        PriorityFeeCalculator, UrgencyTier,
    },
    leader_transport::LeaderTransportResolver,
    multi_leader_fanout::MultiLeaderFanoutEngine,
    route::{DirectLeaderQuicRoute, ExecutionRoute, LocalFixtureRoute, StandardRpcRoute},
    runner::BenchmarkRunner,
    tpu_quic::{socket_addr_to_quic_sni, TpuQuicRouter, QUIC_STRIPED_ENDPOINTS},
    tx_builder::TxBuilder,
    types::{Slot, SlotDuration},
    ROUTER_VERSION,
};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

fn print_help() {
    println!(
        r#"solana-tx-router v{version}
Smart 4-lane QUIC TPU transaction router, CU priority fee calculator,
slot-4 dead-zone circuit breaker, and leader latency benchmark CLI.

USAGE:
  solana-tx-router <SUBCOMMAND> [OPTIONS]

SUBCOMMANDS:
  fee-calc   Compute dynamic CU limit + micro-lamports priority fee & hex headers
  route      Build, sign, circuit-check, and route a transaction over 4-lane QUIC
  bench      Run interleaved A/B latency benchmark (Control RPC vs Router-Aware QUIC)
  probe      Inspect cluster validator TPU QUIC sockets and slot-4 dead-zone state
  fanout     Measure parallel multi-leader QUIC fanout across upcoming slot leaders
  explain    Print step-by-step lifecycle trace for a benchmark execution ID
  help       Show this help guide

EXAMPLES:
  solana-tx-router fee-calc --cu 150000 --urgency high --writable 4 --slot 448160003 --elapsed-ms 365
  solana-tx-router route --lamports 100000 --urgency high --dry-run
  solana-tx-router bench --local --samples 20
  solana-tx-router probe --offline
  solana-tx-router fanout --leaders 4 --offline
  solana-tx-router explain --demo"#,
        version = ROUTER_VERSION
    );
}

fn arg_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find(|w| w[0] == flag)
        .map(|w| w[1].clone())
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|a| a == flag)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn run_fee_calc(args: &[String]) {
    let cu: u32 = arg_value(args, "--cu")
        .and_then(|v| v.parse().ok())
        .unwrap_or(150_000);
    let headroom_bps: u32 = arg_value(args, "--headroom-bps")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000);
    let urgency = arg_value(args, "--urgency")
        .map(|s| UrgencyTier::parse_label(&s))
        .unwrap_or(UrgencyTier::High);
    let writable: usize = arg_value(args, "--writable")
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    let hot_bps: u32 = arg_value(args, "--hot-bps")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_500);
    let slot_num: u64 = arg_value(args, "--slot")
        .and_then(|v| v.parse().ok())
        .unwrap_or(448_160_001);
    let slot_ms: u64 = arg_value(args, "--slot-ms")
        .and_then(|v| v.parse().ok())
        .unwrap_or(400);
    let elapsed_ms: u64 = arg_value(args, "--elapsed-ms")
        .and_then(|v| v.parse().ok())
        .unwrap_or(95);

    let remaining_ms = slot_ms.saturating_sub(elapsed_ms);
    let progress = SlotProgress {
        slot: Slot(slot_num),
        slot_start: Instant::now(),
        duration: SlotDuration::from_millis(slot_ms),
        elapsed_ms,
        remaining_ms,
        progress: (elapsed_ms as f64 / slot_ms.max(1) as f64).clamp(0.0, 1.0),
        is_measured: true,
    };

    let calc = PriorityFeeCalculator::default();
    let contention = AccountContentionProfile {
        writable_accounts: writable,
        hot_account_bps: hot_bps,
        observed_p75_micro_lamports: None,
    };
    let quote = calc.quote(cu, headroom_bps, urgency, &contention, &progress, 1);

    let limit_ix = encode_set_compute_unit_limit(quote.compute_unit_limit);
    let price_ix = encode_set_compute_unit_price(quote.micro_lamports_per_cu);

    println!("============================================================");
    println!("COMPUTE-UNIT PRIORITY FEE QUOTE");
    println!("============================================================");
    println!(
        "Slot Position:      {} (Slot {}/4 | {}ms elapsed / {}ms left)",
        slot_num, quote.slot_in_window, elapsed_ms, remaining_ms
    );
    println!("Slot Phase:         {}", quote.slot_phase);
    println!("Urgency Tier:       {}", quote.urgency);
    println!(
        "CU Limit (+{}bps): {} CU",
        headroom_bps, quote.compute_unit_limit
    );
    println!(
        "Priority Price:     {} micro-lamports / CU",
        quote.micro_lamports_per_cu
    );
    println!("Priority Fee:       {} lamports", quote.priority_fee_lamports);
    println!(
        "Total Tx Fee:       {} lamports ({:.9} SOL)",
        quote.total_fee_lamports,
        quote.total_fee_lamports as f64 / 1_000_000_000.0
    );
    println!(
        "Dead-Zone Hold:     {}",
        if quote.hold_for_next_leader {
            "YES (Hold for next leader Slot 1)"
        } else {
            "NO (Safe to dispatch immediately)"
        }
    );
    println!("Ix #0 (0x02 Limit): 0x{}", hex_encode(&limit_ix.data));
    println!("Ix #1 (0x03 Price): 0x{}", hex_encode(&price_ix.data));
    println!("Rationale:          {}", quote.rationale);
    println!("============================================================");
}

async fn run_route(args: &[String]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let rpc_url =
        arg_value(args, "--rpc").unwrap_or_else(|| "https://api.devnet.solana.com".to_string());
    let lamports: u64 = arg_value(args, "--lamports")
        .and_then(|v| v.parse().ok())
        .unwrap_or(50_000);
    let urgency = arg_value(args, "--urgency")
        .map(|s| UrgencyTier::parse_label(&s))
        .unwrap_or(UrgencyTier::High);
    let slot_num: u64 = arg_value(args, "--slot")
        .and_then(|v| v.parse().ok())
        .unwrap_or(448_160_003);
    let elapsed_ms: u64 = arg_value(args, "--elapsed-ms")
        .and_then(|v| v.parse().ok())
        .unwrap_or(365);
    let live_mode = has_flag(args, "--live") && !has_flag(args, "--dry-run");
    let keypair_flag = arg_value(args, "--keypair");

    let (signer, payer_b58, key_source) = TxBuilder::resolve_keypair(keypair_flag.as_deref())?;
    let recipient = match arg_value(args, "--to") {
        Some(pk_str) => Pubkey::from_str(&pk_str).map_err(|e| e.to_string())?,
        None => Keypair::new().pubkey(),
    };

    let mut slot_clock = SlotClock::new(SlotDuration::MS_400);
    let synthetic_start = Instant::now() - std::time::Duration::from_millis(elapsed_ms.min(400));
    slot_clock.on_slot_observed_at(Slot(slot_num), synthetic_start);
    let progress = slot_clock.progress();

    let clock_arc = Arc::new(RwLock::new(slot_clock));
    let breaker = DeadZoneCircuitBreaker::new(clock_arc, 120);
    let breaker_decision = breaker.evaluate().await;

    let fee_calc = PriorityFeeCalculator::default();
    let quote = fee_calc.quote(
        45_000,
        1_000,
        urgency,
        &AccountContentionProfile::default(),
        &progress,
        1,
    );

    let tx_builder = TxBuilder::new();
    let blockhash = tx_builder
        .resolve_blockhash("LocalRouteBlockhash_CandidateTip")
        .await?;
    let signed_bundle = TxBuilder::build_system_transfer(
        &signer,
        &recipient,
        lamports,
        blockhash,
        Some(&quote),
    )?;

    let router = TpuQuicRouter::new(&rpc_url)?;
    let mock_tpu: SocketAddr = "127.0.0.1:8009".parse()?;

    println!("============================================================");
    println!("SOLANA TX ROUTER -- DISPATCH PIPELINE");
    println!("============================================================");
    println!("Mode:               {}", if live_mode { "LIVE" } else { "DRY-RUN" });
    println!("Signer:             {} ({})", payer_b58, key_source);
    println!("Recipient:          {}", recipient);
    println!("QUIC Stripes:       {} independent Ed25519 X.509 UDP lanes", QUIC_STRIPED_ENDPOINTS);
    println!("Example TPU SNI:    {}", socket_addr_to_quic_sni(mock_tpu));
    println!(
        "Wire Packet:        {} / 1232 bytes (built in {} us)",
        signed_bundle.packet_bytes_len, signed_bundle.build_time_micros
    );
    println!("Signature:          {}", signed_bundle.signature_base58);
    println!(
        "Priority Fee:       {} micro-lamports/CU ({} lamports total fee)",
        quote.micro_lamports_per_cu, signed_bundle.total_fee_lamports
    );

    match &breaker_decision {
        CircuitBreakerAction::DispatchImmediately {
            slot,
            slot_in_window,
            remaining_ms,
            ..
        } => {
            println!(
                "Circuit Breaker:    DISPATCH_IMMEDIATELY (Slot {} [{}/4], {}ms left)",
                slot, slot_in_window, remaining_ms
            );
        }
        CircuitBreakerAction::HoldForNextLeader {
            current_slot,
            target_slot,
            hold_duration_ms,
            ..
        } => {
            println!(
                "Circuit Breaker:    HOLD_FOR_NEXT_LEADER (Slot {} [4/4] -> Slot {}, hold {}ms)",
                current_slot, target_slot, hold_duration_ms
            );
        }
    }

    if live_mode {
        let count = router.refresh_cluster_nodes().await.unwrap_or(0);
        println!("Cluster TPU Cache:  {} QUIC validator sockets loaded", count);
        match router.broadcast_rpc_fallback(&signed_bundle.raw_bytes).await {
            Ok(sig) => println!("Dispatched Sig:     {}", sig),
            Err(e) => println!("Live Dispatch Note: {}", e),
        }
    } else {
        println!("Status:             DRY-RUN VERIFIED (pass --live --rpc <url> to broadcast)");
    }
    println!("============================================================");
    Ok(())
}

async fn run_bench(args: &[String]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let samples: usize = arg_value(args, "--samples")
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);
    let max_tps: f64 = arg_value(args, "--tps")
        .and_then(|v| v.parse().ok())
        .unwrap_or(200.0);
    let live_mode = has_flag(args, "--live") && !has_flag(args, "--local");
    let persist = has_flag(args, "--save");
    let rpc_url =
        arg_value(args, "--rpc").unwrap_or_else(|| "https://api.devnet.solana.com".to_string());

    let exp_cfg = ExperimentConfig {
        cluster: if live_mode {
            "devnet".to_string()
        } else {
            "local".to_string()
        },
        route: "quic_vs_rpc".to_string(),
        sample_count: samples,
        max_tps,
        interleave_mode: true,
        leader_window_focus: true,
        ..Default::default()
    };

    let runner = BenchmarkRunner::new(exp_cfg).with_persistence(persist);

    let (control_route, router_route): (Arc<dyn ExecutionRoute>, Arc<dyn ExecutionRoute>) =
        if live_mode {
            let resolver = Arc::new(LeaderTransportResolver::new(rpc_url.clone()));
            let quic_route = DirectLeaderQuicRoute::new(rpc_url.clone(), resolver)?;
            (
                Arc::new(StandardRpcRoute::new(rpc_url)),
                Arc::new(quic_route),
            )
        } else {
            (
                Arc::new(LocalFixtureRoute::new(6).with_label("local_fixture_control_rpc")),
                Arc::new(LocalFixtureRoute::new(1).with_label("local_fixture_router_quic")),
            )
        };

    let (summary, _records) = runner.run_experiment(control_route, router_route).await?;

    println!("============================================================");
    println!("INTERLEAVED A/B LATENCY BENCHMARK ({})", summary.experiment_id);
    println!("============================================================");
    println!(
        "{:<28} {:>14} {:>14}",
        "Metric", "Control RPC", "Router QUIC"
    );
    println!("{:-<60}", "");
    println!(
        "{:<28} {:>13.1}% {:>13.1}%",
        "Confirmed Rate",
        summary.control_summary.success_rate,
        summary.router_summary.success_rate
    );
    println!(
        "{:<28} {:>12.2}ms {:>12.2}ms",
        "Submit -> Ack (p50)",
        summary.control_summary.submit_to_ack_ms_p50,
        summary.router_summary.submit_to_ack_ms_p50
    );
    println!(
        "{:<28} {:>12.1}ms {:>12.1}ms",
        "Submit -> Confirm (p50)",
        summary.control_summary.submit_to_confirmed_ms_p50,
        summary.router_summary.submit_to_confirmed_ms_p50
    );
    println!(
        "{:<28} {:>11.1} sl {:>11.1} sl",
        "Slot Delta Landed (p50)",
        summary.control_summary.slot_delta_landed_p50,
        summary.router_summary.slot_delta_landed_p50
    );
    println!("{:-<60}", "");
    println!(
        "Verdict: {} ({:+.1}ms p50 delta)",
        summary.router_advantage_verdict, summary.delta_confirmation_latency_ms
    );
    println!("============================================================");
    Ok(())
}

async fn run_probe(args: &[String]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let rpc_url =
        arg_value(args, "--rpc").unwrap_or_else(|| "https://api.devnet.solana.com".to_string());
    let ws_url = RouterConfig::derive_ws_url(&rpc_url);
    let offline = has_flag(args, "--offline");

    let probe = ClusterProbe::new(rpc_url, ws_url);
    let probe_data = if offline {
        probe.run_offline(448_160_002).await
    } else {
        match probe.run().await {
            Ok(r) => r,
            Err(_) => probe.run_offline(448_160_002).await,
        }
    };

    println!("============================================================");
    println!("VALIDATOR TPU QUIC TOPOLOGY PROBE ({})", probe_data.cluster);
    println!("============================================================");
    println!(
        "Current Slot:       {} (Window Slot {}/4)",
        probe_data.current_slot, probe_data.slot_in_window
    );
    println!("RAM Blockhash Read: {} ns", probe_data.ram_lookup_ns);
    println!("QUIC Verified TPU:  {} nodes", probe_data.quic_verified_nodes);
    println!("{:-<60}", "");
    for lp in &probe_data.leader_probes {
        println!(
            "Slot {} [{}/4] | {:<18} | {:>4}us | {}",
            lp.slot,
            lp.slot_in_window,
            lp.tpu_quic
                .map(|s| s.to_string())
                .unwrap_or_else(|| "unadvertised".to_string()),
            lp.quic_latency_us,
            lp.routing_verdict
        );
    }
    println!("============================================================");
    Ok(())
}

async fn run_fanout(args: &[String]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let leaders_n: usize = arg_value(args, "--leaders")
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);
    let rpc_url =
        arg_value(args, "--rpc").unwrap_or_else(|| "https://api.devnet.solana.com".to_string());
    let ws_url = RouterConfig::derive_ws_url(&rpc_url);
    let offline = has_flag(args, "--offline");

    let engine = MultiLeaderFanoutEngine::new(rpc_url, ws_url, leaders_n);
    let fanout_data = if offline {
        engine.simulate_offline(448_160_001).await
    } else {
        match engine.execute_fanout().await {
            Ok(r) => r,
            Err(_) => engine.simulate_offline(448_160_001).await,
        }
    };

    println!("============================================================");
    println!("MULTI-LEADER QUIC TPU FANOUT ({} LEADERS)", fanout_data.fanout_size);
    println!("============================================================");
    println!(
        "Parallel Fanout:    {} us (vs {} us sequential -> {:.2}x speedup)",
        fanout_data.parallel_fanout_time_us,
        fanout_data.sequential_estimated_us,
        fanout_data.concurrency_speedup
    );
    println!("RAM Blockhash Read: {} ns", fanout_data.blockhash_ram_lookup_ns);
    println!("Verdict:            {}", fanout_data.handover_resilience_verdict);
    println!("{:-<60}", "");
    for item in &fanout_data.leaders {
        println!(
            "Slot {} [{}/4] | {:>4}us | {} | {}",
            item.slot, item.slot_in_window, item.quic_rtt_us, item.role, item.delivery_guarantee
        );
    }
    println!("============================================================");
    Ok(())
}

async fn run_explain(args: &[String]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if has_flag(args, "--demo") || args.len() < 3 {
        let cfg = ExperimentConfig {
            id: "demo-trace".to_string(),
            sample_count: 2,
            max_tps: 500.0,
            ..Default::default()
        };
        let runner = BenchmarkRunner::new(cfg).with_persistence(false);
        let ctrl = Arc::new(LocalFixtureRoute::new(0));
        let rtr = Arc::new(LocalFixtureRoute::new(0));
        let (_summary, samples) = runner.run_experiment(ctrl, rtr).await?;
        if let Some(sample) = samples.get(1) {
            println!("{}", ExecutionExplainer::format_explanation(sample));
        }
        return Ok(());
    }

    let exec_id = &args[2];
    match ExecutionExplainer::find_and_explain(exec_id) {
        Some(text) => println!("{}", text),
        None => println!(
            "No stored sample found for execution ID '{}'. Run `solana-tx-router explain --demo` for a live preview.",
            exec_id
        ),
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let subcmd = args.get(1).map(|s| s.as_str()).unwrap_or("help");

    let res = match subcmd {
        "fee-calc" => {
            run_fee_calc(&args);
            Ok(())
        }
        "route" => run_route(&args).await,
        "bench" => run_bench(&args).await,
        "probe" => run_probe(&args).await,
        "fanout" => run_fanout(&args).await,
        "explain" => run_explain(&args).await,
        "--version" | "-V" | "version" => {
            println!("solana-tx-router v{}", ROUTER_VERSION);
            Ok(())
        }
        _ => {
            print_help();
            Ok(())
        }
    };

    if let Err(err) = res {
        eprintln!("Error: {}", err);
        std::process::exit(1);
    }
}
