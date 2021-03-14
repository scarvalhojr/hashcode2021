use super::*;
use crate::sched::{Schedule, Scheduler};
use crate::intersect::reorder_all_intersections;
use log::{debug, info};
use std::collections::HashMap;
use std::f32::consts::E;

pub struct TrafficScheduler {
    log_base: f32,
}

impl Default for TrafficScheduler {
    fn default() -> Self {
        Self { log_base: E }
    }
}

impl TrafficScheduler {
    pub fn new(log_base: f32) -> Self {
        Self { log_base }
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

        info!("Traffic scheduler: log {}", self.log_base);

        // Find the largest difference in number of cars between the quitest
        // and the busiest streets among all intersections (for informational
        // purposes only)
        let mut max_delta = 0;
        for (&inter_id, counters) in traffic.iter() {
            let min_traffic = counters.values().min().unwrap();
            let max_traffic = counters.values().max().unwrap();
            let traffic_delta = max_traffic - min_traffic;
            if traffic_delta > max_delta {
                debug!(
                    "Intersection {}: {} min traffic, {} max traffic, {} delta",
                    inter_id, min_traffic, max_traffic, traffic_delta,
                );
                max_delta = traffic_delta;
            }
        }

        for (&inter_id, counters) in traffic.iter() {
            let min_traffic = *counters.values().min().unwrap() as f32;
            for (&street_id, &counter) in counters.iter() {
                // Normalize the time each street gets based on the total
                // number of cars that need to cross it
                let time = ((counter as f32) / min_traffic)
                    .log(self.log_base)
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
