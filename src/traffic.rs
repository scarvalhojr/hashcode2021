use super::*;
use crate::intersect::reorder_all_intersections;
use crate::sched::{Schedule, Scheduler};
use log::info;
use rand::{thread_rng, Rng};
use std::collections::HashMap;

pub struct TrafficScheduler {
    min_base: f32,
    max_base: f32,
    max_streets_per_inter: usize,
}

impl Default for TrafficScheduler {
    fn default() -> Self {
        Self {
            min_base: 1.5_f32,
            max_base: 3.5_f32,
            max_streets_per_inter: 20,
        }
    }
}

impl TrafficScheduler {
    pub fn set_min_base(&mut self, min_base: f32) {
        self.min_base = min_base;
    }

    pub fn set_max_base(&mut self, max_base: f32) {
        self.max_base = max_base;
    }
}

impl Scheduler for TrafficScheduler {
    fn schedule<'a>(&self, simulation: &'a Simulation) -> Schedule<'a> {
        let mut schedule = Schedule::new(simulation);

        // For each intersection, count how many cars need to cross each street
        let mut traffic: HashMap<IntersectionId, HashMap<StreetId, usize>> =
            HashMap::new();
        for car_path in simulation.car_paths.iter() {
            let path_len = car_path.len();
            for &street_id in car_path.iter().take(path_len - 1) {
                let inter_id = simulation.streets[street_id].end_intersection;
                traffic
                    .entry(inter_id)
                    .and_modify(|counters| {
                        counters
                            .entry(street_id)
                            .and_modify(|counter| *counter += 1)
                            .or_insert_with(|| 1);
                    })
                    .or_insert_with(|| {
                        vec![(street_id, 1)].into_iter().collect()
                    });
            }
        }

        let log_base = thread_rng().gen_range(self.min_base..=self.max_base);
        info!("Traffic scheduler: log base {}", log_base);

        for (&inter_id, counters) in traffic.iter() {
            if counters.len() > self.max_streets_per_inter {
                // Too many streets: give each street 1 sec
                for (&street_id, _) in counters.iter() {
                    schedule.add_street(inter_id, street_id, 1);
                }
                continue;
            }
            let min_traffic = *counters.values().min().unwrap() as f32;
            for (&street_id, &counter) in counters.iter() {
                // Normalize the time each street gets based on the total
                // number of cars that need to cross it
                let time = ((counter as f32) / min_traffic)
                    .log(log_base)
                    .round()
                    .max(1_f32) as Time;
                assert!(time > 0);
                schedule.add_street(inter_id, street_id, time);
            }
        }

        reorder_all_intersections(&mut schedule);

        schedule
    }
}
