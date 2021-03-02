use super::*;
use crate::naive::NaiveScheduler;
use crate::sched::{Schedule, ScheduleStats, Scheduler};

pub struct IncrementalScheduler {
    rounds: u32,
    min_wait_time: Time,
    max_streets_per_round: usize,
    max_shuffles_per_street: usize,
}

impl Default for IncrementalScheduler {
    fn default() -> Self {
        Self {
            rounds: 10,
            min_wait_time: 10,
            max_streets_per_round: 10,
            max_shuffles_per_street: 10,
        }
    }
}

impl IncrementalScheduler {
    pub fn set_rounds(&mut self, rounds: u32) {
        self.rounds = rounds;
    }

    pub fn set_min_wait_time(&mut self, min_wait_time: u32) {
        self.min_wait_time = min_wait_time;
    }

    pub fn set_max_streets_per_round(&mut self, max_streets_per_round: usize) {
        self.max_streets_per_round = max_streets_per_round;
    }

    pub fn set_max_shuffles_per_street(&mut self, max_shuffles: usize) {
        self.max_shuffles_per_street = max_shuffles;
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

impl Scheduler for IncrementalScheduler {
    fn schedule<'a>(&self, simulation: &'a Simulation) -> Schedule<'a> {
        let mut schedule = NaiveScheduler::default().schedule(simulation);
        let mut stats = schedule.stats().unwrap();

        println!(
            "\n\
            Incremental scheduler\n\
            ------------------------\n\
            Rounds                 : {}\n\
            Min wait time          : {}\n\
            Max streets per round  : {}\n\
            Max shuffles per street: {}",
            self.rounds,
            self.min_wait_time,
            self.max_streets_per_round,
            self.max_shuffles_per_street,
        );

        for round in 1..=self.rounds {
            println!("Round {}, current score: {}", round, stats.score);

            // Sort streets by total wait time
            let mut wait_times: Vec<(StreetId, Time)> = stats
                .total_wait_time
                .into_iter()
                .filter(|&(_, time)| time >= self.min_wait_time)
                .collect();
            wait_times.sort_unstable_by(|a, b| b.1.cmp(&a.1));

            let mut best_count = 0;
            let mut best_score = stats.score;
            let mut best_change: Option<(Schedule, ScheduleStats)> = None;
            for &(street_id, wait_time) in
                wait_times.iter().take(self.max_streets_per_round)
            {
                if schedule.is_street_always_green(street_id) {
                    // This street is always green so can't be improved
                    continue;
                }

                let shuffles = bounded_factorial(
                    schedule.num_streets_in_intersection(street_id),
                    self.max_shuffles_per_street,
                );
                println!(
                    "Street {}: {} total wait time, \
                    {} streets in the intersection, {} shuffles",
                    street_id,
                    wait_time,
                    schedule.num_streets_in_intersection(street_id),
                    shuffles,
                );
                for add_time in 0..=2 {
                    println!(
                        "  Adding {} time to street {}",
                        add_time, street_id,
                    );

                    let mut new_schedule = schedule.clone();
                    new_schedule.add_street_time(street_id, add_time);
                    for _ in 0..=shuffles {
                        let new_stats = new_schedule.stats().unwrap();
                        if new_stats.score > best_score {
                            println!(
                                "  => New best score: {} ***",
                                new_stats.score
                            );
                            best_count += 1;
                            best_score = new_stats.score;
                            best_change =
                                Some((new_schedule.clone(), new_stats));
                        }
                        new_schedule.shuffle_intersection(street_id);
                    }
                }

                if best_count >= 5 {
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
