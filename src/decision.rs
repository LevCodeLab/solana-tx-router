use crate::freshness::{BankFreshnessTier, FreshnessState, FreshnessTier};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutingAction {
    Submit,
    Wait { wait_ms: u64, reason: String },
    Retry { attempt: u32, reason: String },
    Abort { reason: String },
    Unknown { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionResult {
    pub action: RoutingAction,
    pub explanation: String,
    pub evidence: Vec<String>,
}

pub struct ExecutionDecisionEngine;

impl ExecutionDecisionEngine {
    /// Evaluates routing decision for the CONTROL group (baseline standard RPC client).
    pub fn evaluate_control(freshness: &FreshnessState) -> DecisionResult {
        if freshness.blockhash_tier == FreshnessTier::Stale {
            DecisionResult {
                action: RoutingAction::Abort {
                    reason: "Blockhash expired (>110 slots old)".to_string(),
                },
                explanation: "Control aborted: blockhash exceeded maximum validity window"
                    .to_string(),
                evidence: vec![format!("Blockhash age: {}ms", freshness.blockhash_age_ms)],
            }
        } else {
            DecisionResult {
                action: RoutingAction::Submit,
                explanation:
                    "Control submitted: baseline client submits blindly without leader or bank timing awareness"
                        .to_string(),
                evidence: vec![format!(
                    "Target blockhash: {} (age: {}ms)",
                    freshness.blockhash, freshness.blockhash_age_ms
                )],
            }
        }
    }

    /// Evaluates routing decision for the ROUTER-AWARE group.
    ///
    /// Uses continuous slot clock, leader transition lookahead, and candidate
    /// bank invalidation checks to avoid dropped packets and wasted priority fees.
    pub fn evaluate_router_aware(
        freshness: &FreshnessState,
        leader_window_focus: bool,
    ) -> DecisionResult {
        if freshness.bank_tier == BankFreshnessTier::Abandoned {
            return DecisionResult {
                action: RoutingAction::Abort {
                    reason: "Target parent candidate bank invalidated by UpdateParent".to_string(),
                },
                explanation:
                    "Router aborted: candidate bank abandoned during leader handoff, preventing dispatch to dead fork"
                        .to_string(),
                evidence: vec![
                    format!("Candidate Bank ID: {:?}", freshness.bank_id),
                    "UpdateParent invalidated parent block".to_string(),
                ],
            };
        }

        if freshness.blockhash_tier == FreshnessTier::Stale {
            return DecisionResult {
                action: RoutingAction::Abort {
                    reason: "Blockhash is stale; requires stream refresh".to_string(),
                },
                explanation:
                    "Router aborted: fresh blockhash required from RAM stream before submission"
                        .to_string(),
                evidence: vec![format!("Blockhash age: {}ms", freshness.blockhash_age_ms)],
            };
        }

        if leader_window_focus && freshness.leader_tier == FreshnessTier::HandoffImminent {
            let wait_ms = freshness.remaining_window_ms.max(15);
            return DecisionResult {
                action: RoutingAction::Wait {
                    wait_ms,
                    reason: format!(
                        "Leader handoff imminent (<40ms remaining in slot). Waiting for next leader: {:?}",
                        freshness.next_leader
                    ),
                },
                explanation: format!(
                    "Router held {}ms: avoided late-slot submission likely to drop during leader rotation",
                    wait_ms
                ),
                evidence: vec![
                    format!(
                        "Slot elapsed: {}ms / {}ms",
                        freshness.slot_elapsed_ms, freshness.slot_target_duration_ms
                    ),
                    format!("Current leader: {:?}", freshness.current_leader),
                    format!("Next leader: {:?}", freshness.next_leader),
                ],
            };
        }

        if freshness.source_tier == FreshnessTier::Stale {
            return DecisionResult {
                action: RoutingAction::Unknown {
                    reason: "Source telemetry stream delayed >2500ms; degraded certainty"
                        .to_string(),
                },
                explanation:
                    "Router flagged degraded source: telemetry delayed beyond confidence threshold"
                        .to_string(),
                evidence: vec![format!(
                    "Last event received {}ms ago",
                    freshness.last_event_received_ago_ms
                )],
            };
        }

        DecisionResult {
            action: RoutingAction::Submit,
            explanation:
                "Router submitted: verified fresh slot window, active leader tenure, and canonical parent lineage"
                    .to_string(),
            evidence: vec![
                format!("Slot: {}ms remaining", freshness.remaining_window_ms),
                format!("Leader: {:?}", freshness.current_leader),
                format!("Blockhash age: {}ms (FRESH)", freshness.blockhash_age_ms),
                format!("Bank state: {:?}", freshness.bank_tier),
            ],
        }
    }
}
