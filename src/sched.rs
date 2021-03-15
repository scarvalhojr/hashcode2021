use super::*;
use image::{ImageBuffer, Rgb, RgbImage};
use rand::{seq::SliceRandom, thread_rng};
use std::collections::{HashSet, VecDeque};
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};

const GREEN: Rgb<u8> = Rgb([0, 128, 0]);
const LIGHT_GRAY: Rgb<u8> = Rgb([211, 211, 211]);
const LIGHT_GREEN: Rgb<u8> = Rgb([144, 238, 144]);
const RED: Rgb<u8> = Rgb([255, 0, 0]);
const WHITE: Rgb<u8> = Rgb([255, 255, 255]);

pub trait Scheduler {
    fn schedule<'a>(&self, simulation: &'a Simulation) -> Schedule<'a>;
}

#[derive(Clone)]
pub struct Schedule<'a> {
    pub simulation: &'a Simulation,
    pub intersections: HashMap<IntersectionId, Intersection>,
}

#[derive(Clone, Default)]
pub struct Intersection {
    pub turns: Vec<(StreetId, Time)>,
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
    pub image: RgbImage,
    pub score: Score,
}

impl ScheduleStats {
    fn new(schedule: &Schedule) -> Self {
        let num_intersections = schedule.intersections.len();
        let num_streets = schedule
            .intersections
            .values()
            .map(|intersection| intersection.turns.len())
            .sum();
        let image_width = u32::try_from(schedule.intersections.len()).unwrap()
            - 1
            + schedule
                .intersections
                .values()
                .map(|inter| inter.cycle)
                .sum::<u32>();
        let image_height = 1 + schedule.simulation.duration;

        Self {
            num_intersections,
            num_streets,
            num_arrived_cars: 0,
            earliest_arrival: 0,
            latest_arrival: 0,
            crossed_streets: HashSet::new(),
            total_wait_time: HashMap::new(),
            image: ImageBuffer::new(image_width, image_height),
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
                self.cycle += add_time;
                return;
            }
        }
        // Street was not in the interesection yet: add it
        self.turns.push((street_id, add_time));
        self.cycle += add_time;
    }

    pub fn sub_street_time(&mut self, street_id: StreetId, sub_time: Time) {
        let mut remove_idx = None;
        for (idx, (id, time)) in self.turns.iter_mut().enumerate() {
            if *id == street_id {
                if *time > sub_time {
                    *time -= sub_time;
                    self.cycle -= sub_time;
                    return;
                }
                self.cycle -= *time;
                remove_idx = Some(idx);
            }
        }
        self.turns.remove(remove_idx.unwrap());
    }

    pub fn shuffle(&mut self) {
        let mut rng = thread_rng();
        self.turns.shuffle(&mut rng);
    }

    pub fn is_green(&self, street_id: StreetId, at_time: Time) -> bool {
        if self.cycle == 0 {
            return false;
        }
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

    pub fn get_street_time(&self, street_id: StreetId) -> Option<Time> {
        self.turns
            .iter()
            .find(|&(id, _)| *id == street_id)
            .map(|&(_, time)| time)
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

    pub fn sub_street_time(&mut self, street_id: StreetId, sub_time: Time) {
        let inter_id = self.simulation.streets[street_id].end_intersection;
        self.intersections
            .entry(inter_id)
            .and_modify(|inter| inter.sub_street_time(street_id, sub_time));
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

    pub fn get_intersection_id(
        &self,
        street_id: StreetId,
    ) -> Option<IntersectionId> {
        self.simulation
            .streets
            .get(street_id)
            .map(|street| street.end_intersection)
    }

    pub fn reset_intersection(&mut self, inter_id: IntersectionId) {
        self.intersections.remove(&inter_id);
    }

    pub fn stats(&self) -> Result<ScheduleStats, String> {
        let mut stats = ScheduleStats::new(self);

        let mut inter_start_col: HashMap<IntersectionId, u32> = HashMap::new();
        let mut next_start_col = 0;

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
            for col in 0..stats.image.width() {
                stats.image.put_pixel(col, time, WHITE);
            }

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
                let inter_id = self.get_intersection_id(street_id).unwrap();
                let is_green = self.is_green(inter_id, street_id, time);
                if is_green {
                    stats.crossed_streets.insert(street_id);
                    let car_id = cars.pop_front().unwrap();
                    moving_cars
                        .get_mut(&car_id)
                        .unwrap()
                        .cross_intersection(self.simulation);
                }

                let intersection;
                if let Some(inter) = self.intersections.get(&inter_id) {
                    intersection = inter;
                } else {
                    assert_eq!(is_green, false);
                    continue;
                }

                let inter_col;
                if let Some(&col) = inter_start_col.get(&inter_id) {
                    inter_col = col;
                } else {
                    inter_col = next_start_col;
                    inter_start_col.insert(inter_id, inter_col);
                    next_start_col += 1 + intersection.cycle;
                }

                let street_time =
                    intersection.get_street_time(street_id).unwrap_or(0);
                if street_time == 0 {
                    continue;
                }

                let street_col = inter_col
                    + intersection
                        .turns
                        .iter()
                        .take_while(|(id, _)| *id != street_id)
                        .map(|(_, t)| t)
                        .sum::<u32>();

                let color = if is_green { LIGHT_GREEN } else { RED };
                for col in street_col..(street_col + street_time) {
                    assert_eq!(stats.image.get_pixel(col, time), &WHITE);
                    stats.image.put_pixel(col, time, color);
                }

                if is_green {
                    let col = inter_col + (time % intersection.cycle);
                    assert_eq!(stats.image.get_pixel(col, time), &LIGHT_GREEN);
                    stats.image.put_pixel(col, time, GREEN);
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
                * Score::try_from(arrived_cars).unwrap();

            if arrived_cars > 0 {
                stats.latest_arrival = time;
                if stats.earliest_arrival == 0 {
                    stats.earliest_arrival = time;
                }
            }

            // Remove cars that reached their end
            moving_cars.retain(|_, car| car.state != CarState::Arrived);
        }

        for &col in inter_start_col.values().filter(|&col| *col > 0) {
            for row in 0..stats.image.height() {
                assert_eq!(stats.image.get_pixel(col - 1, row), &WHITE);
                stats.image.put_pixel(col - 1, row, LIGHT_GRAY);
            }
        }

        Ok(stats)
    }

    pub fn load_from_str(&mut self, s: &str) -> Result<(), String> {
        let mut lines = s.lines().zip(1..);
        let mut intersections = HashMap::new();

        let num_intersections: usize = lines
            .next()
            .ok_or_else(|| "Missing first line".to_string())?
            .0
            .parse()
            .map_err(|err: ParseIntError| {
                format!("Line 1: Invalid number: {}", err)
            })?;

        for _ in 0..num_intersections {
            let (line, line_num) = lines
                .next()
                .ok_or_else(|| "Missing intersections".to_string())?;
            let inter_id: IntersectionId =
                line.parse().map_err(|err: ParseIntError| {
                    format!("Line {}: Invalid number: {}", line_num, err)
                })?;
            if inter_id >= self.simulation.num_intersections {
                return Err(format!(
                    "Intersection ID {} is out of bounds",
                    inter_id
                ));
            }

            let mut intersection = Intersection::default();

            let (line, line_num) = lines.next().ok_or_else(|| {
                format!("Incomplete intersection {}", inter_id)
            })?;
            let num_streets: usize =
                line.parse().map_err(|err: ParseIntError| {
                    format!("Line {}: Invalid number: {}", line_num, err)
                })?;
            let mut added_streets = HashSet::new();

            for _ in 0..num_streets {
                let (line, line_num) = lines.next().ok_or_else(|| {
                    format!("Incomplete intersection {}", inter_id)
                })?;
                let mut fields = line.split_whitespace();
                let street_name = fields.next().ok_or_else(|| {
                    format!("Line {}: missing street name", line_num)
                })?;
                let street_id = self
                    .simulation
                    .streets
                    .iter()
                    .enumerate()
                    .find(|(_, street)| street.name == street_name)
                    .map(|(street_id, _)| street_id)
                    .ok_or_else(|| {
                        format!(
                            "Line {}: unknown street: {}",
                            line_num, street_name
                        )
                    })?;
                if !added_streets.insert(street_id) {
                    return Err(format!(
                        "Line {}: street {} appears multiple times",
                        line_num, street_name
                    ));
                }
                let time = fields
                    .next()
                    .ok_or_else(|| {
                        format!(
                            "Line {}: missing duration of green light at {}",
                            line_num, street_name
                        )
                    })?
                    .parse()
                    .map_err(|err: ParseIntError| {
                        format!("Line {}: Invalid number: {}", line_num, err)
                    })?;

                intersection.add_street(street_id, time);
            }

            intersections.insert(inter_id, intersection);
        }

        self.intersections = intersections;
        Ok(())
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
