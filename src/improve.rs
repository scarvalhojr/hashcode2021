use super::*;
use crate::sched::Schedule;
use log::{info, warn};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub trait Improver {
    fn improve<'a>(
        &self,
        abort_flag: Arc<AtomicBool>,
        schedule: Schedule<'a>,
    ) -> Option<(Schedule<'a>, Score)>;
}

pub struct IncrementalImprover {
    max_rounds: Option<u32>,
    abort_flag: Arc<AtomicBool>,
}

impl IncrementalImprover {
    pub fn new(abort_flag: Arc<AtomicBool>) -> Self {
        Self {
            max_rounds: None,
            abort_flag,
        }
    }
}

impl IncrementalImprover {
    pub fn set_max_rounds(&mut self, rounds: u32) {
        self.max_rounds = Some(rounds);
    }

    pub fn improve<'a>(
        &self,
        initial_schedule: &'a Schedule,
        improver: &dyn Improver,
    ) -> Schedule<'a> {
        if let Some(rounds) = self.max_rounds {
            info!("Incremental improver: max {} rounds", rounds);
        } else {
            info!("Incremental improver: continuous rounds");
        };

        let mut schedule = initial_schedule.clone();
        for round in 1.. {
            if self.max_rounds.map(|max| round > max).unwrap_or(false) {
                break;
            }

            if let Some((new_schedule, new_score)) =
                improver.improve(self.abort_flag.clone(), schedule.clone())
            {
                schedule = new_schedule;
                info!("Round {}, new score {}", round, new_score);
            } else {
                info!("Round {}, no improvement", round);
                break;
            }

            if self.abort_flag.load(Ordering::SeqCst) {
                warn!("Termination request received after {} rounds", round);
                break;
            }
        }
        schedule
    }
}
