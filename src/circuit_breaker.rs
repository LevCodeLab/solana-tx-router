use crate::clock::{SlotClock, SlotProgress};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::info;

/// Evaluation decision from the Slot-4 Dead-Zone Circuit Breaker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CircuitBreakerAction {
    /// Safe to inject transaction immediately into current slot (Slots 1, 2, 3, or early Slot 4).
    DispatchImmediately {
        slot: u64,
        slot_in_window: u8,
        elapsed_ms: u64,
        remaining_ms: u64,
    },
    /// Inside the critical leader rotation dead zone (late Slot 4).
    /// Hold execution for `hold_duration_ms` to land cleanly in the next leader's Slot 1.
    HoldForNextLeader {
        current_slot: u64,
        target_slot: u64,
        hold_duration_ms: u64,
        reason: String,
    },
}

/// Dead-Zone Circuit Breaker protecting against Slot 4 leader handoff packet drops.
/// Supports both 400ms slots (120ms dead-zone threshold) and 200ms slots (40ms threshold).
pub struct DeadZoneCircuitBreaker {
    clock: Arc<RwLock<SlotClock>>,
    dead_zone_threshold_ms: u64,
}

impl DeadZoneCircuitBreaker {
    pub fn new(clock: Arc<RwLock<SlotClock>>, dead_zone_threshold_ms: u64) -> Self {
        Self {
            clock,
            dead_zone_threshold_ms,
        }
    }

    pub fn new_fast_200ms(clock: Arc<RwLock<SlotClock>>) -> Self {
        Self {
            clock,
            dead_zone_threshold_ms: 40,
        }
    }

    /// Evaluates current slot progress and decides whether immediate dispatch is safe
    /// or if the router should hold for the next leader's Slot 1.
    pub async fn evaluate(&self) -> CircuitBreakerAction {
        let progress = self.clock.read().await.progress();
        let effective_threshold =
            if self.dead_zone_threshold_ms == 40 && progress.duration.as_millis() >= 400 {
                120
            } else if self.dead_zone_threshold_ms == 120 && progress.duration.as_millis() <= 200 {
                40
            } else {
                self.dead_zone_threshold_ms
            };
        Self::evaluate_progress(&progress, effective_threshold)
    }

    /// Pure evaluation function given a `SlotProgress` snapshot.
    pub fn evaluate_progress(progress: &SlotProgress, threshold_ms: u64) -> CircuitBreakerAction {
        let slot_u64 = progress.slot.as_u64();
        let slot_in_window = ((slot_u64 % 4) + 1) as u8;

        if slot_in_window == 4 && progress.remaining_ms <= threshold_ms {
            let hold_ms = progress.remaining_ms.saturating_add(4);
            CircuitBreakerAction::HoldForNextLeader {
                current_slot: slot_u64,
                target_slot: slot_u64.saturating_add(1),
                hold_duration_ms: hold_ms,
                reason: format!(
                    "Leader handover dead zone active: Slot 4/4 with only {}ms remaining. Holding {}ms for next leader Slot 1",
                    progress.remaining_ms, hold_ms
                ),
            }
        } else {
            CircuitBreakerAction::DispatchImmediately {
                slot: slot_u64,
                slot_in_window,
                elapsed_ms: progress.elapsed_ms,
                remaining_ms: progress.remaining_ms,
            }
        }
    }

    /// Returns `true` when current leader has consumed >= 90% of its slot budget or is in Slot 4 dead zone,
    /// signaling the router to pre-warm and include the next leader (`L_next`) in multi-leader fanout.
    pub async fn is_handover_imminent(&self) -> bool {
        let progress = self.clock.read().await.progress();
        let slot_in_window = ((progress.slot.as_u64() % 4) + 1) as u8;
        (slot_in_window == 4 && progress.remaining_ms <= self.dead_zone_threshold_ms)
            || progress.progress >= 0.90
            || progress.remaining_ms == 0
    }

    /// Coordinates transaction dispatch: sleeps across the Slot 4 dead zone if active, then returns Slot 1 status.
    pub async fn wait_for_safe_window(&self) -> CircuitBreakerAction {
        let decision = self.evaluate().await;
        match decision {
            CircuitBreakerAction::DispatchImmediately { .. } => decision,
            CircuitBreakerAction::HoldForNextLeader {
                hold_duration_ms,
                target_slot,
                ..
            } => {
                info!(
                    "Dead Zone active: holding {}ms to inject directly into next leader Slot 1 ({})",
                    hold_duration_ms, target_slot
                );
                tokio::time::sleep(Duration::from_millis(hold_duration_ms)).await;

                let new_progress = self.clock.read().await.progress();
                let slot_u64 = new_progress.slot.as_u64();
                let slot_in_window = ((slot_u64 % 4) + 1) as u8;

                CircuitBreakerAction::DispatchImmediately {
                    slot: slot_u64,
                    slot_in_window,
                    elapsed_ms: new_progress.elapsed_ms,
                    remaining_ms: new_progress.remaining_ms,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Slot, SlotDuration};
    use std::time::Instant;

    #[test]
    fn test_circuit_breaker_slot_4_dead_zone_hold() {
        let progress_slot_3 = SlotProgress {
            slot: Slot(102),
            slot_start: Instant::now(),
            duration: SlotDuration::MS_400,
            elapsed_ms: 200,
            remaining_ms: 200,
            progress: 0.50,
            is_measured: true,
        };
        let action = DeadZoneCircuitBreaker::evaluate_progress(&progress_slot_3, 120);
        assert!(matches!(
            action,
            CircuitBreakerAction::DispatchImmediately {
                slot_in_window: 3,
                ..
            }
        ));

        // Slot 4 (103 % 4 + 1 == 4) with 50ms remaining <= 120ms threshold -> Hold 54ms for Slot 104!
        let progress_dead_zone = SlotProgress {
            slot: Slot(103),
            slot_start: Instant::now(),
            duration: SlotDuration::MS_400,
            elapsed_ms: 350,
            remaining_ms: 50,
            progress: 0.875,
            is_measured: true,
        };
        let action = DeadZoneCircuitBreaker::evaluate_progress(&progress_dead_zone, 120);
        assert!(matches!(
            action,
            CircuitBreakerAction::HoldForNextLeader {
                hold_duration_ms: 54,
                target_slot: 104,
                ..
            }
        ));

        // 200ms slot: Slot 4 with 25ms remaining <= 40ms threshold -> Hold 29ms for Slot 104!
        let progress_200ms_dead_zone = SlotProgress {
            slot: Slot(103),
            slot_start: Instant::now(),
            duration: SlotDuration::MS_200,
            elapsed_ms: 175,
            remaining_ms: 25,
            progress: 0.875,
            is_measured: true,
        };
        let action_200 = DeadZoneCircuitBreaker::evaluate_progress(&progress_200ms_dead_zone, 40);
        assert!(matches!(
            action_200,
            CircuitBreakerAction::HoldForNextLeader {
                hold_duration_ms: 29,
                target_slot: 104,
                ..
            }
        ));
    }
}
