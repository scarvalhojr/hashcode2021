use super::*;
use crate::sched::{Schedule, Scheduler};

#[derive(Default)]
pub struct Naive {}

impl Scheduler for Naive {
    fn schedule<'a>(&self, simulation: &'a Simulation) -> Schedule<'a> {
        let mut schedule = Schedule::new(simulation);

        for (street_id, street) in simulation.streets.iter().enumerate() {
            let inter_id = street.end_intersection;
            schedule.add_light(inter_id, street_id, 1);
        }

        schedule
    }
}
