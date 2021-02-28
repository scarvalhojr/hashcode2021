use std::collections::HashMap;
use std::convert::TryInto;
use std::num::ParseIntError;
use std::str::FromStr;

pub mod naive;
pub mod sched;

type Time = u32;
type CarId = usize;
type StreetId = usize;
type IntersectionId = u32;

pub struct Simulation {
    pub duration: Time,
    pub num_intersections: u32,
    pub streets: Vec<Street>,
    pub car_paths: Vec<Vec<StreetId>>,
    pub bonus: u32,
}

pub struct Street {
    pub name: String,
    pub start_insersection: IntersectionId,
    pub end_intersection: IntersectionId,
    pub travel_time: Time,
}

impl FromStr for Simulation {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut lines = s.lines().zip(1..);

        let fields: Vec<u32> = lines
            .next()
            .ok_or_else(|| "Missing first line".to_string())?
            .0
            .split_whitespace()
            .map(|num| num.parse())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err: ParseIntError| {
                format!("Line 1: Invalid number: {}", err)
            })?;

        if fields.len() != 5 {
            return Err("Line 1: Line must have exactly 5 fields".to_string());
        }
        let duration = fields[0];
        let num_intersections = fields[1];
        let num_streets = fields[2].try_into().unwrap();
        let num_cars = fields[3].try_into().unwrap();
        let bonus = fields[4];

        let mut street_index = HashMap::new();
        let mut streets = Vec::with_capacity(num_streets);
        for street_id in 0..num_streets {
            let (line, line_num) = lines
                .next()
                .ok_or_else(|| "Missing street lines".to_string())?;
            let street: Street = line.parse().map_err(|err| {
                format!("Line {}: Invalid street line: {}", line_num, err)
            })?;
            street_index.insert(street.name.clone(), street_id);
            streets.push(street);
        }

        let mut car_paths = Vec::with_capacity(num_cars);
        for car_id in 0..num_cars {
            let (line, line_num) = lines
                .next()
                .ok_or_else(|| "Missing car lines".to_string())?;
            let path = line
                .split_whitespace()
                .skip(1)
                .map(|name| {
                    street_index.get(name).copied().ok_or_else(|| {
                        format!("Line {}: Unknown street: {}", line_num, name)
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            car_paths.insert(car_id, path);
        }

        Ok(Simulation {
            duration,
            num_intersections,
            streets,
            car_paths,
            bonus,
        })
    }
}

impl FromStr for Street {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut fields = s.split_whitespace().collect::<Vec<_>>();
        if fields.len() != 4 {
            return Err("Street line must have exactly 4 fields".to_string());
        }
        let name = fields.remove(2).to_string();
        let numbers = fields
            .iter_mut()
            .map(|num| num.parse())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err: ParseIntError| format!("Invalid number: {}", err))?;
        let start_insersection = numbers[0];
        let end_intersection = numbers[1];
        let travel_time = numbers[2];
        Ok(Street {
            name,
            start_insersection,
            end_intersection,
            travel_time,
        })
    }
}
