use super::*;
use crate::sched::{Schedule, Scheduler};
use log::info;
use std::collections::HashMap;

#[derive(Default)]
pub struct TrafficScheduler {}

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

        info!("Traffic scheduler (log 10)");

        // List intersections that have large difference between its quitests
        // and busiest streets (for informational purposes only)
        let mut max_delta = 0;
        for (&inter_id, counters) in traffic.iter() {
            let min_traffic = counters.values().min().unwrap();
            let max_traffic = counters.values().max().unwrap();
            let traffic_delta = max_traffic - min_traffic;
            if traffic_delta > max_delta {
                info!(
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
                let time =
                    ((counter as f32) / min_traffic).log10().round().max(1_f32)
                        as Time;
                assert!(time > 0);
                schedule.add_street(inter_id, street_id, time);
            }
        }

        schedule
    }
}
