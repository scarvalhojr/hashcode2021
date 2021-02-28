use super::*;
use crate::sched::{Schedule, Scheduler};
use std::collections::HashSet;

#[derive(Default)]
pub struct Naive {}

impl Scheduler for Naive {
    fn schedule<'a>(&self, simulation: &'a Simulation) -> Schedule<'a> {
        let mut schedule = Schedule::new(simulation);

        let mut crossed_streets: HashSet<StreetId> = HashSet::new();
        for car_path in simulation.car_paths.iter() {
            let path_len = car_path.len();
            crossed_streets.extend(car_path.iter().take(path_len - 1).copied());
        }

        for &street_id in crossed_streets.iter() {
            let inter_id = simulation.streets[street_id].end_intersection;
            schedule.add_street(inter_id, street_id, 1);
        }

        schedule
    }
}
