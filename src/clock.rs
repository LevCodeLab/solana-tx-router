use crate::types::{LeaderId, Slot, SlotDuration};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// High-resolution snapshot of slot progression.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotProgress {
    pub slot: Slot,
    #[serde(skip, default = "Instant::now")]
    pub slot_start: Instant,
    pub duration: SlotDuration,
    pub elapsed_ms: u64,
    pub remaining_ms: u64,
    /// Progress ratio clamped strictly between 0.0 and 1.0.
    pub progress: f64,
    pub is_measured: bool,
}

/// Monotonic hardware slot clock.
///
/// Uses `std::time::Instant` for all elapsed timing so NTP jumps never skew
/// slot-4 dead-zone circuit-breaker thresholds.
pub struct SlotClock {
    current_slot: Slot,
    slot_start: Instant,
    effective_duration: SlotDuration,
    is_measured: bool,
    last_measured_duration: Option<Duration>,
}

impl SlotClock {
    pub fn new(initial_duration: SlotDuration) -> Self {
        Self {
            current_slot: Slot::ZERO,
            slot_start: Instant::now(),
            effective_duration: initial_duration,
            is_measured: false,
            last_measured_duration: None,
        }
    }

    pub fn set_effective_duration(&mut self, duration: SlotDuration, is_measured: bool) {
        self.effective_duration = duration;
        self.is_measured = is_measured;
    }

    pub fn on_slot_observed(&mut self, slot: Slot) {
        self.on_slot_observed_at(slot, Instant::now());
    }

    pub fn tick(&mut self, slot: Slot) {
        self.on_slot_observed(slot);
    }

    pub fn on_slot_observed_at(&mut self, slot: Slot, at: Instant) {
        if slot > self.current_slot {
            if self.current_slot != Slot::ZERO && slot.as_u64() == self.current_slot.as_u64() + 1 {
                let actual_elapsed = at.duration_since(self.slot_start);
                if actual_elapsed >= Duration::from_millis(50)
                    && actual_elapsed <= Duration::from_secs(3)
                {
                    self.last_measured_duration = Some(actual_elapsed);
                }
            }
            self.current_slot = slot;
            self.slot_start = at;
        }
    }

    pub fn progress(&self) -> SlotProgress {
        self.progress_at(Instant::now())
    }

    pub fn progress_at(&self, at: Instant) -> SlotProgress {
        let elapsed = if at >= self.slot_start {
            at.duration_since(self.slot_start)
        } else {
            Duration::ZERO
        };

        let target_duration = self.effective_duration.as_duration();
        let target_ms = self.effective_duration.as_millis();
        let elapsed_ms = elapsed.as_millis() as u64;

        let ratio = if target_ms > 0 {
            (elapsed.as_secs_f64() / target_duration.as_secs_f64()).clamp(0.0, 1.0)
        } else {
            1.0
        };

        let remaining = if elapsed < target_duration {
            target_duration - elapsed
        } else {
            Duration::ZERO
        };

        SlotProgress {
            slot: self.current_slot,
            slot_start: self.slot_start,
            duration: self.effective_duration,
            elapsed_ms,
            remaining_ms: remaining.as_millis() as u64,
            progress: (ratio * 1000.0).round() / 1000.0,
            is_measured: self.is_measured,
        }
    }

    pub fn current_slot(&self) -> Slot {
        self.current_slot
    }

    pub fn duration(&self) -> SlotDuration {
        self.effective_duration
    }

    pub fn elapsed(&self) -> Duration {
        Instant::now().saturating_duration_since(self.slot_start)
    }

    pub fn remaining(&self) -> Duration {
        let target = self.effective_duration.as_duration();
        let elapsed = self.elapsed();
        if elapsed < target {
            target - elapsed
        } else {
            Duration::ZERO
        }
    }

    pub fn last_measured_duration(&self) -> Option<Duration> {
        self.last_measured_duration
    }
}

impl Default for SlotClock {
    fn default() -> Self {
        Self::new(SlotDuration::MS_200)
    }
}

/// In-memory cache of slot leader assignments for zero-I/O leader lookahead.
#[derive(Debug, Default, Clone)]
pub struct LeaderScheduleCache {
    schedule: BTreeMap<Slot, LeaderId>,
    current_leader: Option<(Slot, LeaderId)>,
}

impl LeaderScheduleCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_leader(&mut self, slot: Slot, leader: LeaderId) {
        self.schedule.insert(slot, leader.clone());
        self.current_leader = Some((slot, leader));
    }

    pub fn populate_schedule(&mut self, starting_slot: Slot, leaders: Vec<LeaderId>) {
        for (idx, leader) in leaders.into_iter().enumerate() {
            let slot = Slot(starting_slot.as_u64().saturating_add(idx as u64));
            self.schedule.insert(slot, leader);
        }
    }

    pub fn leader_for(&self, slot: Slot) -> Option<LeaderId> {
        self.schedule.get(&slot).cloned()
    }

    pub fn current_leader(&self) -> Option<LeaderId> {
        self.current_leader.as_ref().map(|(_, l)| l.clone())
    }

    pub fn next_leader(&self, current_slot: Slot) -> Option<LeaderId> {
        self.schedule.get(&current_slot.next()).cloned()
    }

    pub fn is_leader_transition(&self, slot_a: Slot, slot_b: Slot) -> bool {
        match (self.leader_for(slot_a), self.leader_for(slot_b)) {
            (Some(a), Some(b)) => a != b,
            _ => false,
        }
    }

    pub fn prune_older_than(&mut self, min_slot: Slot) {
        self.schedule.retain(|s, _| *s >= min_slot);
    }

    pub fn cached_slots_count(&self) -> usize {
        self.schedule.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_clock_boundaries_0_50_99_100_percent() {
        let mut clock = SlotClock::new(SlotDuration::MS_200);
        let start = Instant::now();
        clock.on_slot_observed_at(Slot(100), start);

        let p0 = clock.progress_at(start);
        assert_eq!(p0.slot, Slot(100));
        assert_eq!(p0.elapsed_ms, 0);
        assert_eq!(p0.remaining_ms, 200);
        assert_eq!(p0.progress, 0.0);

        let p50 = clock.progress_at(start + Duration::from_millis(100));
        assert_eq!(p50.elapsed_ms, 100);
        assert_eq!(p50.remaining_ms, 100);
        assert_eq!(p50.progress, 0.5);

        let p99 = clock.progress_at(start + Duration::from_millis(198));
        assert_eq!(p99.elapsed_ms, 198);
        assert_eq!(p99.remaining_ms, 2);
        assert_eq!(p99.progress, 0.99);

        let p100 = clock.progress_at(start + Duration::from_millis(200));
        assert_eq!(p100.elapsed_ms, 200);
        assert_eq!(p100.remaining_ms, 0);
        assert_eq!(p100.progress, 1.0);

        let pover = clock.progress_at(start + Duration::from_millis(250));
        assert_eq!(pover.progress, 1.0);
        assert_eq!(pover.remaining_ms, 0);
    }

    #[test]
    fn test_slot_transitions_and_leader_cache() {
        let mut clock = SlotClock::new(SlotDuration::MS_250);
        let t0 = Instant::now();
        clock.on_slot_observed_at(Slot(10), t0);

        let t1 = t0 + Duration::from_millis(248);
        clock.on_slot_observed_at(Slot(11), t1);
        assert_eq!(clock.current_slot(), Slot(11));
        assert_eq!(
            clock.last_measured_duration(),
            Some(Duration::from_millis(248))
        );

        let mut leaders = LeaderScheduleCache::new();
        let l1 = LeaderId::new("LeaderAlpha11111111111111111111111111111111");
        let l2 = LeaderId::new("LeaderBeta111111111111111111111111111111111");
        leaders.set_leader(Slot(100), l1.clone());
        leaders.set_leader(Slot(101), l2.clone());
        assert_eq!(leaders.leader_for(Slot(100)), Some(l1));
        assert!(leaders.is_leader_transition(Slot(100), Slot(101)));
        leaders.prune_older_than(Slot(101));
        assert!(leaders.leader_for(Slot(100)).is_none());
    }
}
