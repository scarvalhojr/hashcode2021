use super::*;
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};

pub trait Scheduler {
    fn schedule<'a>(&self, simulation: &'a Simulation) -> Schedule<'a>;
}

pub struct Schedule<'a> {
    simulation: &'a Simulation,
    intersections: HashMap<IntersectionId, TrafficLight>,
}

pub struct TrafficLight {
    order: Vec<(StreetId, Time)>,
    cycle: Time,
}

impl TrafficLight {
    pub fn new(street_id: StreetId, time: Time) -> Self {
        let order = vec![(street_id, time)];
        let cycle = time;
        Self { order, cycle }
    }

    pub fn add(&mut self, street_id: StreetId, time: Time) {
        self.order.push((street_id, time));
        self.cycle += time;
    }

    pub fn is_green(&self, street_id: StreetId, time: Time) -> bool {
        let time = time % self.cycle;
        let mut acc = 0;
        for &(cycle_street_id, cycle_street_time) in &self.order {
            acc += cycle_street_time;
            if time < acc {
                return street_id == cycle_street_id;
            }
        }
        unreachable!();
    }
}

impl<'a> Schedule<'a> {
    pub fn new(simulation: &'a Simulation) -> Self {
        let intersections = HashMap::new();
        Self {
            simulation,
            intersections,
        }
    }

    pub fn add_light(
        &mut self,
        inter_id: IntersectionId,
        street_id: StreetId,
        time: Time,
    ) {
        self.intersections
            .entry(inter_id)
            .and_modify(|light| light.add(street_id, time))
            .or_insert_with(|| TrafficLight::new(street_id, time));
    }

    pub fn num_intersections(&self) -> usize {
        self.intersections.len()
    }

    pub fn is_green(
        &self,
        inter_id: IntersectionId,
        street_id: StreetId,
        time: Time,
    ) -> bool {
        self.intersections
            .get(&inter_id)
            .map(|inter| inter.is_green(street_id, time))
            .unwrap_or(false)
    }

    pub fn score(&self) -> Result<u32, String> {
        let mut score = 0;

        // All cars that haven't reached their end yet
        let mut moving_cars: HashMap<CarId, Car> = self
            .simulation
            .car_paths
            .iter()
            .map(|path| Car::new(path))
            .enumerate()
            .collect();

        // Traffic light queues of cars at the end of streets
        let mut queues: HashMap<StreetId, VecDeque<CarId>> = HashMap::new();

        // Add all cars to the queues of their starting street (in order of car ID)
        for car_id in 0..self.simulation.car_paths.len() {
            let street_id = moving_cars.get_mut(&car_id).unwrap().start();
            queues
                .entry(street_id)
                .and_modify(|cars| cars.push_back(car_id))
                .or_insert_with(|| vec![car_id].into_iter().collect());
        }

        for time in 0..self.simulation.duration {

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
                let inter_id =
                    self.simulation.streets[street_id].end_intersection;
                if self.is_green(inter_id, street_id, time) {
                    let car_id = cars.pop_front().unwrap();
                    moving_cars
                        .get_mut(&car_id)
                        .unwrap()
                        .cross_intersection(self.simulation);
                }
            }

            // Drop empty traffic light queues
            queues.retain(|_, cars| !cars.is_empty());

            // Update score for cars that reached their end
            score += (self.simulation.bonus
                + (self.simulation.duration - time))
                * u32::try_from(
                    moving_cars
                        .iter()
                        .filter(|(_, car)| car.state == CarState::Finished)
                        .count(),
                )
                .unwrap();

            // Remove cars that reached their end
            moving_cars.retain(|_, car| car.state != CarState::Finished);
        }

        Ok(score)
    }
}

#[derive(PartialEq)]
enum CarState {
    Ready,
    Waiting,
    Finished,
}

struct Car {
    // Remaining path in reverse order (last element is current street)
    remain_path: Vec<StreetId>,
    // Remaining time in current street
    remain_time: Time,
    state: CarState,
}

impl Car {
    fn new(full_path: &[StreetId]) -> Self {
        Self {
            remain_path: full_path.iter().copied().rev().collect(),
            remain_time: 0,
            state: CarState::Waiting,
        }
    }

    fn start(&mut self) -> StreetId {
        self.remain_path.pop().unwrap()
    }

    fn cross_intersection(&mut self, simul: &Simulation) {
        assert_eq!(self.remain_time, 0);

        let street_id = self.remain_path.last().copied().unwrap();
        self.remain_time = simul.streets[street_id].travel_time;
        self.state = CarState::Ready;
    }

    fn move_forward(&mut self) -> Option<StreetId> {
        // Move forward on the current street
        self.remain_time -= 1;

        if self.remain_time == 0 {
            let street_id = self.remain_path.pop().unwrap();
            if self.remain_path.is_empty() {
                // Reached the end of its journey
                self.state = CarState::Finished;
            } else {
                // Join traffic light queue
                self.state = CarState::Waiting;
                return Some(street_id);
            }
        }

        None
    }
}

impl Display for Schedule<'_> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        writeln!(f, "{}", self.intersections.len())?;
        for (inter_id, light) in &self.intersections {
            writeln!(f, "{}\n{}", inter_id, light.order.len())?;
            for &(street_id, time) in &light.order {
                let street_name =
                    &self.simulation.streets.get(street_id).unwrap().name;
                writeln!(f, "{} {}", street_name, time)?;
            }
        }
        Ok(())
    }
}
