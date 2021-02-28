use super::*;
use crate::sched::{Schedule, Scheduler};
use std::collections::HashSet;

#[derive(Default)]
pub struct Naive {}

impl Scheduler for Naive {
    fn schedule<'a>(&self, simulation: &'a Simulation) -> Schedule<'a> {
        let mut schedule = Schedule::new(simulation);

        // Number of cars whose minimum travel time exceeds the simulation's
        // duration; the schedule will not directly include the streets crossed
        // by these cars (although they may be included by other crossing cars)
        let mut ignored_cars = 0;

        let mut crossed_streets: HashSet<StreetId> = HashSet::new();
        for car_path in simulation.car_paths.iter() {
            let travel_time: Time = car_path
                .iter()
                .map(|&street_id| simulation.streets[street_id].travel_time)
                .sum();
            if travel_time > simulation.duration {
                ignored_cars += 1;
            } else {
                let path_len = car_path.len();
                crossed_streets
                    .extend(car_path.iter().take(path_len - 1).copied());
            }
        }

        println!(
            "\n\
            Naive scheduler\n\
            ---------------\n\
            Ignored cars: {}",
            ignored_cars,
        );

        for &street_id in crossed_streets.iter() {
            let inter_id = simulation.streets[street_id].end_intersection;
            schedule.add_street(inter_id, street_id, 1);
        }

        schedule
    }
}
