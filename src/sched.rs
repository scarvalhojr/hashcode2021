use super::*;
use rand::{seq::SliceRandom, thread_rng};
use std::collections::{HashSet, VecDeque};
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};

pub trait Scheduler {
    fn schedule<'a>(&self, simulation: &'a Simulation) -> Schedule<'a>;
}

#[derive(Clone)]
pub struct Schedule<'a> {
    simulation: &'a Simulation,
    intersections: HashMap<IntersectionId, Intersection>,
}

#[derive(Clone)]
pub struct Intersection {
    turns: Vec<(StreetId, Time)>,
    cycle: Time,
}

pub struct ScheduleStats {
    pub num_intersections: usize,
    pub num_streets: usize,
    pub num_arrived_cars: usize,
    pub earliest_arrival: Time,
    pub latest_arrival: Time,
    pub crossed_streets: HashSet<StreetId>,
    pub total_wait_time: HashMap<StreetId, Time>,
    pub score: u32,
}

impl ScheduleStats {
    fn new(schedule: &Schedule) -> Self {
        let num_intersections = schedule.intersections.len();
        let num_streets = schedule
            .intersections
            .values()
            .map(|intersection| intersection.turns.len())
            .sum();

        Self {
            num_intersections,
            num_streets,
            num_arrived_cars: 0,
            earliest_arrival: 0,
            latest_arrival: 0,
            crossed_streets: HashSet::new(),
            total_wait_time: HashMap::new(),
            score: 0,
        }
    }
}

impl Intersection {
    pub fn new(street_id: StreetId, time: Time) -> Self {
        let turns = vec![(street_id, time)];
        let cycle = time;
        Self { turns, cycle }
    }

    pub fn add_street(&mut self, street_id: StreetId, time: Time) {
        self.turns.push((street_id, time));
        self.cycle += time;
    }

    pub fn add_street_time(&mut self, street_id: StreetId, add_time: Time) {
        for (id, time) in self.turns.iter_mut() {
            if *id == street_id {
                *time += add_time;
                return;
            }
        }
        panic!("Failed to add time to street {} in intersection", street_id);
    }

    pub fn shuffle(&mut self) {
        let mut rng = thread_rng();
        self.turns.shuffle(&mut rng);
    }

    pub fn is_green(&self, street_id: StreetId, at_time: Time) -> bool {
        let time = at_time % self.cycle;
        let mut acc_time = 0;
        for &(turn_street_id, turn_street_time) in &self.turns {
            acc_time += turn_street_time;
            if time < acc_time {
                return turn_street_id == street_id;
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

    pub fn add_street(
        &mut self,
        inter_id: IntersectionId,
        street_id: StreetId,
        time: Time,
    ) {
        self.intersections
            .entry(inter_id)
            .and_modify(|intersection| intersection.add_street(street_id, time))
            .or_insert_with(|| Intersection::new(street_id, time));
    }

    pub fn add_street_time(&mut self, street_id: StreetId, add_time: Time) {
        let inter_id = self.simulation.streets[street_id].end_intersection;
        self.intersections
            .entry(inter_id)
            .and_modify(|inter| inter.add_street_time(street_id, add_time));
    }

    pub fn shuffle_intersection(&mut self, street_id: StreetId) {
        let inter_id = self.simulation.streets[street_id].end_intersection;
        self.intersections
            .entry(inter_id)
            .and_modify(|inter| inter.shuffle());
    }

    pub fn num_streets_in_intersection(&self, street_id: StreetId) -> usize {
        let inter_id = self.simulation.streets[street_id].end_intersection;
        self.intersections.get(&inter_id).unwrap().turns.len()
    }

    pub fn is_green(
        &self,
        inter_id: IntersectionId,
        street_id: StreetId,
        at_time: Time,
    ) -> bool {
        self.intersections
            .get(&inter_id)
            .map(|inter| inter.is_green(street_id, at_time))
            .unwrap_or(false)
    }

    pub fn is_street_always_green(&self, street_id: StreetId) -> bool {
        let inter_id = self.simulation.streets[street_id].end_intersection;
        let turns = &self.intersections.get(&inter_id).unwrap().turns;
        turns.len() == 1 && turns[0].0 == street_id
    }

    pub fn stats(&self) -> Result<ScheduleStats, String> {
        let mut stats = ScheduleStats::new(self);

        // All cars that haven't reached their end yet
        let mut moving_cars: HashMap<CarId, Car> = self
            .simulation
            .car_paths
            .iter()
            .map(|path| Car::new(path))
            .enumerate()
            .collect();

        // Queues of cars at the end of streets
        let mut queues: HashMap<StreetId, VecDeque<CarId>> = HashMap::new();

        // Add cars to the queues of their starting street (in order of car ID)
        for car_id in 0..self.simulation.car_paths.len() {
            let street_id = moving_cars.get_mut(&car_id).unwrap().start();
            queues
                .entry(street_id)
                .and_modify(|cars| cars.push_back(car_id))
                .or_insert_with(|| vec![car_id].into_iter().collect());
        }

        for time in 0..=self.simulation.duration {
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
                    stats.crossed_streets.insert(street_id);
                    let car_id = cars.pop_front().unwrap();
                    moving_cars
                        .get_mut(&car_id)
                        .unwrap()
                        .cross_intersection(self.simulation);
                }
            }

            // Drop empty traffic light queues
            queues.retain(|_, cars| !cars.is_empty());

            // Add 1 second to the total wait time of each street with a queue
            for &street_id in queues.keys() {
                stats
                    .total_wait_time
                    .entry(street_id)
                    .and_modify(|wait_time| *wait_time += 1)
                    .or_insert_with(|| 1);
            }

            // Update score for cars that reached their end
            let arrived_cars = moving_cars
                .iter()
                .filter(|(_, car)| car.state == CarState::Arrived)
                .count();
            stats.num_arrived_cars += arrived_cars;
            stats.score += (self.simulation.bonus
                + (self.simulation.duration - time))
                * u32::try_from(arrived_cars).unwrap();

            if arrived_cars > 0 {
                stats.latest_arrival = time;
                if stats.earliest_arrival == 0 {
                    stats.earliest_arrival = time;
                }
            }

            // Remove cars that reached their end
            moving_cars.retain(|_, car| car.state != CarState::Arrived);
        }

        Ok(stats)
    }
}

#[derive(PartialEq)]
pub enum CarState {
    Ready,
    Waiting,
    Arrived,
}

pub struct Car {
    // Remaining path in reverse order (last element is current street)
    remain_path: Vec<StreetId>,
    // Remaining time in current street
    remain_time: Time,
    pub state: CarState,
}

impl Car {
    pub fn new(full_path: &[StreetId]) -> Self {
        Self {
            remain_path: full_path.iter().copied().rev().collect(),
            remain_time: 0,
            state: CarState::Waiting,
        }
    }

    pub fn start(&mut self) -> StreetId {
        self.remain_path.pop().unwrap()
    }

    pub fn cross_intersection(&mut self, simul: &Simulation) {
        assert_eq!(self.remain_time, 0);

        let street_id = self.remain_path.last().copied().unwrap();
        self.remain_time = simul.streets[street_id].travel_time;
        self.state = CarState::Ready;
    }

    pub fn move_forward(&mut self) -> Option<StreetId> {
        // Move forward on the current street
        self.remain_time -= 1;

        if self.remain_time == 0 {
            let street_id = self.remain_path.pop().unwrap();
            if self.remain_path.is_empty() {
                // Reached the end of its journey
                self.state = CarState::Arrived;
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
            writeln!(f, "{}\n{}", inter_id, light.turns.len())?;
            for &(street_id, time) in &light.turns {
                let street_name =
                    &self.simulation.streets.get(street_id).unwrap().name;
                writeln!(f, "{} {}", street_name, time)?;
            }
        }
        Ok(())
    }
}

impl Display for ScheduleStats {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "\
            Intersections   : {}\n\
            Street lights   : {}\n\
            Arrived cars    : {}\n\
            Earliest arrival: {}\n\
            Latest arrival  : {}\n\
            Crossed streets : {}\n\
            Total wait time : {}\n\
            Schedule score  : {}",
            self.num_intersections,
            self.num_streets,
            self.num_arrived_cars,
            self.earliest_arrival,
            self.latest_arrival,
            self.crossed_streets.len(),
            self.total_wait_time.values().sum::<Time>(),
            self.score,
        )
    }
}
