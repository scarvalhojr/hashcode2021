use super::*;
use crate::improve::Improver;
use crate::sched::{Car, CarState, Schedule};
use std::collections::{HashSet, VecDeque};

pub struct GreedyImprover {
    min_wait_time: Time,
    max_streets: usize,
}

impl Default for GreedyImprover {
    fn default() -> Self {
        Self {
            min_wait_time: 10,
            max_streets: 10,
        }
    }
}

impl GreedyImprover {
    pub fn set_min_wait_time(&mut self, min_wait_time: u32) {
        self.min_wait_time = min_wait_time;
    }

    pub fn set_max_streets(&mut self, max_streets: usize) {
        self.max_streets = max_streets;
    }

    fn update_intersection(schedule: &mut Schedule, inter_id: IntersectionId) {
        let mut intersection = Intersection::from(schedule, inter_id);

        // All cars that haven't reached their end yet
        let mut moving_cars: HashMap<CarId, Car> = schedule
            .simulation
            .car_paths
            .iter()
            .map(|path| Car::new(path))
            .enumerate()
            .collect();

        // Queues of cars at the end of streets
        let mut queues: HashMap<StreetId, VecDeque<CarId>> = HashMap::new();

        // Add cars to the queues of their starting street (in order of car ID)
        for car_id in 0..schedule.simulation.car_paths.len() {
            let street_id = moving_cars.get_mut(&car_id).unwrap().start();
            queues
                .entry(street_id)
                .and_modify(|cars| cars.push_back(car_id))
                .or_insert_with(|| vec![car_id].into_iter().collect());
        }

        for time in 0..=schedule.simulation.duration {
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

            // Let cars at the top of the queue cross intersections
            for (&street_id, cars) in queues.iter_mut() {
                let stree_inter_id =
                    schedule.simulation.streets[street_id].end_intersection;
                let is_green = if stree_inter_id == inter_id {
                    intersection.is_or_set_green(street_id, time)
                } else {
                    schedule.is_green(stree_inter_id, street_id, time)
                };
                if is_green {
                    let car_id = cars.pop_front().unwrap();
                    moving_cars
                        .get_mut(&car_id)
                        .unwrap()
                        .cross_intersection(schedule.simulation);
                }
            }

            if intersection.is_done() {
                // No need to continue further with simulation
                break;
            }

            // Drop empty traffic light queues
            queues.retain(|_, cars| !cars.is_empty());

            // Remove cars that reached their end
            moving_cars.retain(|_, car| car.state != CarState::Arrived);
        }

        intersection.assign_remaining();
        intersection.rebuild_intersection(schedule, inter_id);
    }
}

impl Improver for GreedyImprover {
    fn improve<'a>(
        &self,
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

        println!(
            "Greedy improver: {} minimum wait time, {} max streets per round, \
            {} streets selected, {} intersections",
            self.min_wait_time,
            self.max_streets,
            wait_times.len(),
            inter_ids.len(),
        );

        let mut best_count = 0;
        let mut best_score = stats.score;
        let mut best_sched = None;

        // First, try to improve each intersection without changing times
        for &inter_id in inter_ids.iter() {
            let mut new_schedule = schedule.clone();
            Self::update_intersection(&mut new_schedule, inter_id);
            let new_stats = new_schedule.stats().unwrap();
            if new_stats.score <= best_score {
                continue;
            }
            println!(
                "  => New best score after updating intersection {}: {}",
                inter_id, new_stats.score
            );
            best_count += 1;
            best_score = new_stats.score;
            best_sched = Some(new_schedule.clone());
            if best_count >= 5 {
                break;
            }
        }

        if let Some(sched) = best_sched {
            // If a better schedule was found, return it
            return Some((sched, best_score));
        }

        // Try to improve schedule by adding time to busy streets
        'outer: for add_time in 1..=2 {
            for &(street_id, wait_time) in wait_times.iter() {
                let intersection_id =
                    schedule.get_intersection_id(street_id).unwrap();
                println!(
                    "Adding {} to street {}, intersection {}: {} total wait \
                    time, {} streets in the intersection",
                    add_time,
                    street_id,
                    intersection_id,
                    wait_time,
                    schedule.num_streets_in_intersection(street_id),
                );

                let mut new_schedule = schedule.clone();
                new_schedule.add_street_time(street_id, add_time);
                Self::update_intersection(&mut new_schedule, intersection_id);
                let new_stats = new_schedule.stats().unwrap();
                if new_stats.score <= best_score {
                    continue;
                }
                println!("  => New best score: {}", new_stats.score);
                best_count += 1;
                best_score = new_stats.score;
                best_sched = Some(new_schedule.clone());
                if best_count >= 5 {
                    break 'outer;
                }
            }
        }

        best_sched.map(|sched| (sched, best_score))
    }
}

struct Intersection {
    streets: HashMap<StreetId, Time>,
    slots: Vec<(Option<StreetId>, Time)>,
    cycle: Time,
}

impl Intersection {
    fn from(schedule: &Schedule, intersection_id: IntersectionId) -> Self {
        let turns =
            &schedule.intersections.get(&intersection_id).unwrap().turns;

        let mut streets = HashMap::new();
        let mut slots = Vec::with_capacity(turns.len());
        let mut cycle = 0;
        for &(street_id, time) in turns.iter() {
            streets.insert(street_id, time);
            slots.push((None, time));
            cycle += time;
        }

        Self {
            streets,
            slots,
            cycle,
        }
    }

    fn is_or_set_green(&mut self, street_id: StreetId, at_time: Time) -> bool {
        let mod_time = at_time % self.cycle;
        let mut slot_idx = None;
        let mut slot_street = None;
        let mut slot_time = 0;
        let mut acc_time = 0;
        for (idx, &(street, time)) in self.slots.iter().enumerate() {
            acc_time += time;
            if mod_time < acc_time {
                slot_idx = Some(idx);
                slot_street = street;
                slot_time = time;
                break;
            }
        }

        let slot_idx = slot_idx.unwrap();
        if let Some(id) = slot_street {
            // Slot is already assigned
            return id == street_id;
        }

        if let Some(&street_time) = self.streets.get(&street_id) {
            // Street is not yet assigned: check if times match
            if street_time == slot_time {
                self.slots[slot_idx] = (Some(street_id), street_time);
                self.streets.remove(&street_id);
                true
            } else {
                // Times do not match
                false
            }
        } else {
            // Street is already assigned to another slot
            false
        }
    }

    fn is_done(&self) -> bool {
        self.slots.iter().all(|(street, _)| street.is_some())
    }

    fn assign_remaining(&mut self) {
        for (slot_street, slot_time) in self.slots.iter_mut() {
            if slot_street.is_some() {
                continue;
            }
            let street_id: StreetId = self
                .streets
                .iter()
                .find(|(_, &time)| time == *slot_time)
                .map(|(&id, _)| id)
                .unwrap();
            self.streets.remove(&street_id);
            *slot_street = Some(street_id);
        }
    }

    fn rebuild_intersection(
        &self,
        schedule: &mut Schedule,
        inter_id: IntersectionId,
    ) {
        schedule.reset_intersection(inter_id);
        for &(street_id, time) in self.slots.iter() {
            schedule.add_street(inter_id, street_id.unwrap(), time);
        }
    }
}
