use super::*;
use crate::improve::Improver;
use crate::intersect::reorder_intersection;
use crate::sched::Schedule;
use log::info;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct GreedyImprover {
    min_wait_time: Time,
    max_streets: usize,
    max_add_time: Time,
}

impl Default for GreedyImprover {
    fn default() -> Self {
        Self {
            min_wait_time: 10,
            max_streets: 10,
            max_add_time: 1,
        }
    }
}

impl GreedyImprover {
    pub fn set_min_wait_time(&mut self, min_wait_time: Time) {
        self.min_wait_time = min_wait_time;
    }

    pub fn set_max_streets(&mut self, max_streets: usize) {
        self.max_streets = max_streets;
    }

    pub fn set_max_add_time(&mut self, max_add_time: Time) {
        self.max_add_time = max_add_time;
    }
}

impl Improver for GreedyImprover {
    fn improve<'a>(
        &self,
        abort_flag: Arc<AtomicBool>,
        schedule: Schedule<'a>,
    ) -> Option<(Schedule<'a>, Score)> {
        // Sort streets by total wait time
        let stats = schedule.stats().unwrap();
        let mut wait_times: Vec<(StreetId, Time)> = stats
            .total_wait_time
            .into_iter()
            .filter(|&(street_id, time)| {
                time >= self.min_wait_time
                    && !schedule.is_street_always_green(street_id)
            })
            .collect();
        wait_times.sort_unstable_by(|a, b| b.1.cmp(&a.1));
        wait_times.truncate(self.max_streets);

        // Collect IDs of all intersections
        let inter_ids: HashSet<IntersectionId> = wait_times
            .iter()
            .map(|&(street_id, _)| {
                schedule.get_intersection_id(street_id).unwrap()
            })
            .collect();

        info!(
            "Greedy improver: {} minimum wait time, {} max additional time, \
            {} max streets per round, {} streets selected, {} intersections",
            self.min_wait_time,
            self.max_add_time,
            self.max_streets,
            wait_times.len(),
            inter_ids.len(),
        );

        let mut best_count = 0;
        let mut best_score = stats.score;
        let mut best_sched = None;

        // First, try to improve each intersection by reordering streets
        // without changing their times
        for &inter_id in inter_ids.iter() {
            if abort_flag.load(Ordering::SeqCst) {
                break;
            }
            let mut new_schedule = schedule.clone();
            let new_score = reorder_intersection(&mut new_schedule, inter_id);
            if new_score <= best_score {
                continue;
            }
            info!(
                "=> New best score after updating intersection {}: {}",
                inter_id, new_score,
            );
            best_count += 1;
            best_score = new_score;
            best_sched = Some(new_schedule.clone());
            if best_count >= 5 {
                break;
            }
        }

        if let Some(sched) = best_sched {
            // If a better schedule was found, return it
            return Some((sched, best_score));
        }
        if abort_flag.load(Ordering::SeqCst) {
            return None;
        }

        // Try to improve schedule by adding time to busy streets
        'outer: for add_time in 1..=self.max_add_time {
            for &(street_id, wait_time) in wait_times.iter() {
                if abort_flag.load(Ordering::SeqCst) {
                    break 'outer;
                }
                let intersection_id =
                    schedule.get_intersection_id(street_id).unwrap();
                let mut new_schedule = schedule.clone();
                new_schedule.add_street_time(street_id, add_time);
                let new_score =
                    reorder_intersection(&mut new_schedule, intersection_id);
                if new_score <= best_score {
                    continue;
                }
                info!(
                    "=> New best score after adding {} to street {}, \
                    intersection {} ({} streets in the intersection), {} wait \
                    time: {}",
                    add_time,
                    street_id,
                    intersection_id,
                    schedule.num_streets_in_intersection(street_id),
                    wait_time,
                    new_score,
                );
                best_count += 1;
                best_score = new_score;
                best_sched = Some(new_schedule.clone());
                if best_count >= 5 {
                    break 'outer;
                }
            }
            if best_count > 0 {
                // If a better schedule was found, return it
                break;
            }
        }

        best_sched.map(|sched| (sched, best_score))
    }
}
