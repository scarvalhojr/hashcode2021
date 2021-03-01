use super::*;
use crate::sched::{Schedule, ScheduleStats, Scheduler};
use crate::naive::NaiveScheduler;

pub struct IncrementalScheduler {
    rounds: u32,
    streets_per_round: u32,
    max_shuffles_per_street: u32,
}

impl Default for IncrementalScheduler {
    fn default() -> Self {
        Self {
            rounds: 20,
            streets_per_round: 5,
            max_shuffles_per_street: 5,
        }
    }
}

impl IncrementalScheduler {
    pub fn set_rounds(&mut self, rounds: u32) {
        self.rounds = rounds;
    }

    pub fn set_streets_per_round(&mut self, streets_per_round: u32) {
        self.streets_per_round = streets_per_round;
    }

    pub fn set_max_shuffles_per_street(&mut self, max_shuffles: u32) {
        self.max_shuffles_per_street = max_shuffles;
    }
}

impl Scheduler for IncrementalScheduler {
    fn schedule<'a>(&self, simulation: &'a Simulation) -> Schedule<'a> {
        let mut schedule = NaiveScheduler::default().schedule(simulation);
        let mut stats = schedule.stats().unwrap();

        println!(
            "\n\
            Incremental scheduler\n\
            ---------------------",
        );

        for round in 1..=self.rounds {
            println!("Round {}, current score: {}", round, stats.score);

            // Sort streets by total wait time
            let mut wait_times: Vec<(StreetId, Time)> = stats
                .total_wait_time
                .into_iter()
                .collect();
            wait_times.sort_unstable_by(|a, b| b.1.cmp(&a.1));

            let mut streets_tried = 0;
            let mut best_score = stats.score;
            let mut best_change: Option<(Schedule, ScheduleStats)> = None;
            for &(street_id, wait_time) in wait_times.iter() {
                if schedule.is_street_always_green(street_id) {
                    // This street is always green so can't be improved
                    continue;
                }

                println!(
                    "Adding time to street {}: {} total wait time, \
                    {} streets in the intersection",
                    street_id, wait_time,
                    schedule.num_streets_in_intersection(street_id),
                );

                for _ in 1..=self.max_shuffles_per_street {
                    let mut new_schedule = schedule.clone();
                    new_schedule.add_street_time(street_id, 1);
                    new_schedule.shuffle_intersection(street_id);
                    let new_stats = new_schedule.stats().unwrap();

                    if new_stats.score > best_score {
                        println!("  => New best score: {} ***", new_stats.score);
                        best_score = new_stats.score;
                        best_change = Some((new_schedule, new_stats));
                    } else {
                        println!("  => New score: {}", new_stats.score);
                    }
                }

                streets_tried += 1;
                if streets_tried == self.streets_per_round {
                    break;
                }
            }

            if let Some((new_schedule, new_stats)) = best_change {
                schedule = new_schedule;
                stats = new_stats;
            } else {
                println!("No further improvements could be made");
                break;
            }
        }

        schedule
    }
}
