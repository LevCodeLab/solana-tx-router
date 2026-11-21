use crate::experiment::{ExperimentConfig, ExperimentSummary, SampleRecord};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

pub struct BenchmarkStorage {
    base_dir: PathBuf,
}

impl Default for BenchmarkStorage {
    fn default() -> Self {
        Self {
            base_dir: PathBuf::from("artifacts/benchmarks"),
        }
    }
}

impl BenchmarkStorage {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    pub fn experiment_dir(&self, exp_id: &str) -> PathBuf {
        self.base_dir.join(exp_id)
    }

    /// Persists experiment config, raw JSONL samples, summary JSON, and markdown report.
    pub fn save_experiment(
        &self,
        config: &ExperimentConfig,
        samples: &[SampleRecord],
        summary: &ExperimentSummary,
    ) -> Result<PathBuf, std::io::Error> {
        let dir = self.experiment_dir(&config.id);
        fs::create_dir_all(&dir)?;

        let config_file = dir.join("experiment.json");
        fs::write(config_file, serde_json::to_string_pretty(config)?)?;

        let samples_file = dir.join("samples.jsonl");
        let mut file = File::create(samples_file)?;
        for sample in samples {
            writeln!(file, "{}", serde_json::to_string(sample)?)?;
        }

        let summary_file = dir.join("summary.json");
        fs::write(summary_file, serde_json::to_string_pretty(summary)?)?;

        let readme_file = dir.join("README.md");
        let readme_content = format!(
            r#"# Router Benchmark Experiment: {exp_id}

- Cluster: `{cluster}`
- Source: `{source}`
- Route: `{route}`
- Total Samples: `{total_samples}`
- Interleaved: `{interleave}`
- Verdict: **{verdict}** ({confidence})

## Comparative Summary Matrix

| Metric | CONTROL (Baseline) | ROUTER-AWARE | Delta |
|---|---|---|---|
| Success Rate | {ctrl_success:.1}% ({ctrl_conf}/{ctrl_total}) | {rtr_success:.1}% ({rtr_conf}/{rtr_total}) | {delta_success:+.1}% |
| Confirmation Latency (p50) | {ctrl_conf_p50:.1}ms | {rtr_conf_p50:.1}ms | {delta_conf:+.1}ms |
| Confirmation Latency (p95) | {ctrl_conf_p95:.1}ms | {rtr_conf_p95:.1}ms | n/a |
| Landing Latency (p50) | {ctrl_land_p50:.1}ms | {rtr_land_p50:.1}ms | n/a |
| Slot Delta to Landing (p50) | +{ctrl_slot_p50:.1} slots | +{rtr_slot_p50:.1} slots | n/a |
| Stale Blockhash Errors | {ctrl_bh_err} | {rtr_bh_err} | {delta_bh:+} |
| Leader Handoff Misses | {ctrl_miss} | {rtr_miss} | {delta_miss:+} |
| Total Retries | {ctrl_retries} | {rtr_retries} | n/a |

## Notes
{limitations}
"#,
            exp_id = config.id,
            cluster = config.cluster,
            source = config.source,
            route = config.route,
            total_samples = samples.len(),
            interleave = config.interleave_mode,
            verdict = summary.router_advantage_verdict,
            confidence = summary.statistical_confidence,
            ctrl_success = summary.control_summary.success_rate,
            ctrl_conf = summary.control_summary.confirmed_count,
            ctrl_total = summary.control_summary.total_samples,
            rtr_success = summary.router_summary.success_rate,
            rtr_conf = summary.router_summary.confirmed_count,
            rtr_total = summary.router_summary.total_samples,
            delta_success = summary.delta_success_rate_percent,
            ctrl_conf_p50 = summary.control_summary.submit_to_confirmed_ms_p50,
            rtr_conf_p50 = summary.router_summary.submit_to_confirmed_ms_p50,
            delta_conf = summary.delta_confirmation_latency_ms,
            ctrl_conf_p95 = summary.control_summary.submit_to_confirmed_ms_p95,
            rtr_conf_p95 = summary.router_summary.submit_to_confirmed_ms_p95,
            ctrl_land_p50 = summary.control_summary.submit_to_landed_ms_p50,
            rtr_land_p50 = summary.router_summary.submit_to_landed_ms_p50,
            ctrl_slot_p50 = summary.control_summary.slot_delta_landed_p50,
            rtr_slot_p50 = summary.router_summary.slot_delta_landed_p50,
            ctrl_bh_err = summary.control_summary.stale_blockhash_errors,
            rtr_bh_err = summary.router_summary.stale_blockhash_errors,
            delta_bh = (summary.router_summary.stale_blockhash_errors as i64)
                - (summary.control_summary.stale_blockhash_errors as i64),
            ctrl_miss = summary.control_summary.leader_handoff_misses,
            rtr_miss = summary.router_summary.leader_handoff_misses,
            delta_miss = (summary.router_summary.leader_handoff_misses as i64)
                - (summary.control_summary.leader_handoff_misses as i64),
            ctrl_retries = summary.control_summary.retry_count_total,
            rtr_retries = summary.router_summary.retry_count_total,
            limitations = if summary.limitations.is_empty() {
                "- None noted during this run.".to_string()
            } else {
                summary
                    .limitations
                    .iter()
                    .map(|l| format!("- {}", l))
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        );

        fs::write(readme_file, readme_content)?;
        Ok(dir)
    }

    /// Lists all stored experiment IDs in sorted timestamp order.
    pub fn list_experiments(&self) -> Vec<String> {
        let mut list = Vec::new();
        if let Ok(entries) = fs::read_dir(&self.base_dir) {
            for entry in entries.flatten() {
                if entry.path().is_dir() {
                    if let Some(name) = entry.file_name().to_str() {
                        list.push(name.to_string());
                    }
                }
            }
        }
        list.sort();
        list
    }

    pub fn load_summary(&self, exp_id: &str) -> Option<ExperimentSummary> {
        let file = self.experiment_dir(exp_id).join("summary.json");
        let content = fs::read_to_string(file).ok()?;
        serde_json::from_str(&content).ok()
    }

    pub fn load_samples(&self, exp_id: &str) -> Option<Vec<SampleRecord>> {
        let file = self.experiment_dir(exp_id).join("samples.jsonl");
        let f = File::open(file).ok()?;
        let reader = BufReader::new(f);
        let mut samples = Vec::new();
        for line in reader.lines().map_while(Result::ok) {
            if let Ok(s) = serde_json::from_str::<SampleRecord>(&line) {
                samples.push(s);
            }
        }
        Some(samples)
    }
}
