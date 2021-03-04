use super::*;
use crate::sched::Schedule;

pub trait Improver {
    fn improve<'a>(
        &self,
        schedule: Schedule<'a>,
    ) -> Option<(Schedule<'a>, Score)>;
}

pub struct IncrementalImprover {
    rounds: u32,
}

impl Default for IncrementalImprover {
    fn default() -> Self {
        Self { rounds: 5 }
    }
}

impl IncrementalImprover {
    pub fn set_rounds(&mut self, rounds: u32) {
        self.rounds = rounds;
    }

    pub fn improve<'a>(
        &self,
        initial_schedule: &'a Schedule,
        improver: &dyn Improver,
    ) -> Schedule<'a> {
        println!("Incremental Improver: {} rounds", self.rounds);

        let mut schedule = initial_schedule.clone();
        for round in 1..=self.rounds {
            if let Some((new_schedule, new_score)) =
                improver.improve(schedule.clone())
            {
                schedule = new_schedule;
                println!("Round {}, new score: {}", round, new_score);
            } else {
                println!("Round {}, no improvement", round);
                break;
            }
        }
        schedule
    }
}
