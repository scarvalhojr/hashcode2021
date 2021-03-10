use super::*;
use crate::improve::Improver;
use crate::sched::Schedule;
use log::info;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct ShuffleImprover {
    min_wait_time: Time,
    max_streets: usize,
    max_shuffles: usize,
}

impl Default for ShuffleImprover {
    fn default() -> Self {
        Self {
            min_wait_time: 10,
            max_streets: 10,
            max_shuffles: 10,
        }
    }
}

impl ShuffleImprover {
    pub fn set_min_wait_time(&mut self, min_wait_time: u32) {
        self.min_wait_time = min_wait_time;
    }

    pub fn set_max_streets(&mut self, max_streets: usize) {
        self.max_streets = max_streets;
    }

    pub fn set_max_shuffles(&mut self, max_shuffles: usize) {
        self.max_shuffles = max_shuffles;
    }
}

impl Improver for ShuffleImprover {
    fn improve<'a>(
        &self,
        abort_flag: Arc<AtomicBool>,
        schedule: Schedule<'a>,
    ) -> Option<(Schedule<'a>, Score)> {
        info!(
            "Shuffle improver: {} min wait time, {} max streets per round, \
            {} max shuffles per street",
            self.min_wait_time, self.max_streets, self.max_shuffles,
        );

        // Sort streets by total wait time
        let stats = schedule.stats().unwrap();
        let mut wait_times: Vec<(StreetId, Time)> = stats
            .total_wait_time
            .into_iter()
            .filter(|&(_, time)| time >= self.min_wait_time)
            .collect();
        wait_times.sort_unstable_by(|a, b| b.1.cmp(&a.1));

        let mut best_count = 0;
        let mut best_score = stats.score;
        let mut best_sched = None;
        'outer: for &(street_id, wait_time) in
            wait_times.iter().take(self.max_streets)
        {
            if schedule.is_street_always_green(street_id) {
                // This street is always green so can't be improved
                continue;
            }

            let shuffles = bounded_factorial(
                schedule.num_streets_in_intersection(street_id),
                self.max_shuffles,
            );
            info!(
                "Street {}: {} total wait time, \
                {} streets in the intersection, {} shuffles",
                street_id,
                wait_time,
                schedule.num_streets_in_intersection(street_id),
                shuffles,
            );
            for add_time in 0..=2 {
                let mut new_schedule = schedule.clone();
                new_schedule.add_street_time(street_id, add_time);
                for _ in 0..=shuffles {
                    if abort_flag.load(Ordering::SeqCst) {
                        break 'outer;
                    }

                    let new_stats = new_schedule.stats().unwrap();
                    if new_stats.score <= best_score {
                        continue;
                    }
                    info!(
                        "=> New best score by adding {} to street \
                        {}: {}",
                        add_time, street_id, new_stats.score
                    );
                    best_count += 1;
                    best_score = new_stats.score;
                    best_sched = Some(new_schedule.clone());
                    new_schedule.shuffle_intersection(street_id);
                }
            }

            if best_count >= 5 {
                break;
            }
        }

        best_sched.map(|sched| (sched, best_score))
    }
}

fn bounded_factorial(num: usize, max: usize) -> usize {
    let mut fact = 1;
    for n in (2..=num).rev() {
        fact *= n;
        if fact > max {
            return max;
        }
    }
    fact
}
