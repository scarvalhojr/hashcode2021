use super::*;
use crate::sched::{Car, CarState, Schedule, Scheduler};
use std::collections::{HashSet, VecDeque};

#[derive(Default)]
pub struct AdaptiveScheduler {}

impl Scheduler for AdaptiveScheduler {
    fn schedule<'a>(&self, simulation: &'a Simulation) -> Schedule<'a> {
        // Cars whose minimum travel time exceeds the simulation's duration;
        // these cars will not be used to build the schedule (although the
        // streets they cross  may be included by other crossing cars)
        let mut ignored_cars: HashSet<CarId> = HashSet::new();

        let mut crossed_streets: HashMap<IntersectionId, HashSet<StreetId>> =
            HashMap::new();
        for (car_id, car_path) in simulation.car_paths.iter().enumerate() {
            let travel_time: Time = car_path
                .iter()
                .skip(1)
                .map(|&street_id| simulation.streets[street_id].travel_time)
                .sum();
            if travel_time > simulation.duration {
                ignored_cars.insert(car_id);
            } else {
                let path_len = car_path.len();
                for &street_id in car_path.iter().take(path_len - 1) {
                    let inter_id =
                        simulation.streets[street_id].end_intersection;
                    crossed_streets
                        .entry(inter_id)
                        .and_modify(|streets| {
                            streets.insert(street_id);
                        })
                        .or_insert_with(|| {
                            vec![street_id].into_iter().collect()
                        });
                }
            }
        }

        let mut inter_order: HashMap<IntersectionId, Vec<Option<StreetId>>> =
            HashMap::new();
        for (&inter_id, streets) in crossed_streets.iter() {
            inter_order
                .entry(inter_id)
                .or_insert_with(|| vec![None; streets.len()]);
        }

        // All cars that haven't reached their end yet
        let mut moving_cars: HashMap<CarId, Car> = simulation
            .car_paths
            .iter()
            .map(|path| Car::new(path))
            .enumerate()
            .collect();

        // Queues of cars at the end of streets
        let mut queues: HashMap<StreetId, VecDeque<CarId>> = HashMap::new();

        // Add cars to the queues of their starting street (in order of car ID)
        for car_id in 0..simulation.car_paths.len() {
            let street_id = moving_cars.get_mut(&car_id).unwrap().start();
            queues
                .entry(street_id)
                .and_modify(|cars| cars.push_back(car_id))
                .or_insert_with(|| vec![car_id].into_iter().collect());
        }

        for time in 0..=simulation.duration {
            // Let cars move if possible
            for (&car_id, car) in moving_cars.iter_mut() {
                if car.state == CarState::Ready {
                    if let Some(next_street_id) = car.move_forward() {
                        queues
                            .entry(next_street_id)
                            .and_modify(|cars| cars.push_back(car_id))
                            .or_insert_with(|| {
                                vec![car_id].into_iter().collect()
                            });
                    }
                }
            }

            // Let cars at the top of the queue cross intersections if possible
            for (&street_id, cars) in queues.iter_mut() {
                let inter_id = simulation.streets[street_id].end_intersection;
                let order = inter_order.get_mut(&inter_id).unwrap();
                let slot_pos = usize::try_from(time).unwrap() % order.len();
                let curr_slot = order.get_mut(slot_pos).unwrap();
                if let Some(open_street_id) = curr_slot {
                    // The current slot is already assigned to a street
                    if street_id != *open_street_id {
                        // But the intersection is not open for the street the
                        // car is waiting at, so it will have to wait
                        continue;
                    }
                } else {
                    // Current slot is not yet assigned to any street
                    if !crossed_streets
                        .get_mut(&inter_id)
                        .unwrap()
                        .remove(&street_id)
                    {
                        // The street the car is waiting on has either already
                        // been assigned a different slot, or it does not need
                        // to be open (the car will never finish in time)
                        continue;
                    }
                    // Assign the current slot to the street
                    *curr_slot = Some(street_id);
                }

                // Let the car go through
                let car_id = cars.pop_front().unwrap();
                moving_cars
                    .get_mut(&car_id)
                    .unwrap()
                    .cross_intersection(simulation);
            }

            // Drop empty traffic light queues
            queues.retain(|_, cars| !cars.is_empty());

            // Remove cars that reached their end
            moving_cars.retain(|_, car| car.state != CarState::Arrived);
        }

        let unused_count: usize =
            crossed_streets.values().map(|streets| streets.len()).sum();
        println!(
            "\n\
            Adaptive scheduler\n\
            ------------------\n\
            Ignored cars  : {}\n\
            Unused streets: {}",
            ignored_cars.len(),
            unused_count,
        );

        let mut schedule = Schedule::new(simulation);
        for (&inter_id, streets) in inter_order.iter() {
            for street in streets.iter() {
                if let Some(street_id) = street {
                    schedule.add_street(inter_id, *street_id, 1);
                } else {
                    // Some streets were never crosseed!
                    let unused_streets =
                        crossed_streets.get_mut(&inter_id).unwrap();
                    let unused_street_id =
                        unused_streets.iter().copied().next().unwrap();
                    schedule.add_street(inter_id, unused_street_id, 1);
                    unused_streets.remove(&unused_street_id);
                }
            }
            assert!(crossed_streets.get(&inter_id).unwrap().is_empty());
        }
        schedule
    }
}
