use super::*;
use crate::sched::{Car, CarState, Schedule};
use crate::sums::AllSums;
use std::collections::VecDeque;
use std::iter::once;
use std::ops::{RangeBounds, RangeInclusive};

pub fn reorder_intersection(
    schedule: &mut Schedule,
    inter_id: IntersectionId,
) -> Score {
    let mut intersection = OpenIntersection::from(schedule, inter_id);
    let mut score = 0;

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
                        .or_insert_with(|| vec![car_id].into_iter().collect());
                }
            }
        }

        // Sort queues by number of cars waiting
        let mut queue_order: Vec<(StreetId, usize)> = queues
            .iter()
            .map(|(&street_id, cars)| (street_id, cars.len()))
            .collect();
        queue_order.sort_unstable_by(|a, b| b.1.cmp(&a.1));

        // Let cars at the top of the queue cross intersections
        for (street_id, _) in queue_order.into_iter() {
            let street_inter_id =
                schedule.simulation.streets[street_id].end_intersection;
            let is_green = if street_inter_id == inter_id {
                intersection.is_or_set_green(street_id, time)
            } else {
                schedule.is_green(street_inter_id, street_id, time)
            };
            if is_green {
                let car_id =
                    queues.get_mut(&street_id).unwrap().pop_front().unwrap();
                moving_cars
                    .get_mut(&car_id)
                    .unwrap()
                    .cross_intersection(schedule.simulation);
            }
        }

        // Drop empty traffic light queues
        queues.retain(|_, cars| !cars.is_empty());

        // Update score for cars that reached their end
        let arrived_cars = moving_cars
            .iter()
            .filter(|(_, car)| car.state == CarState::Arrived)
            .count();
        score += (schedule.simulation.bonus
            + (schedule.simulation.duration - time))
            * Score::try_from(arrived_cars).unwrap();

        // Remove cars that reached their end
        moving_cars.retain(|_, car| car.state != CarState::Arrived);
    }

    intersection.assign_remaining_streets();
    intersection.update_schedule(schedule, inter_id);
    score
}

struct OpenIntersection {
    streets: HashMap<StreetId, Time>,
    slots: Vec<(Option<StreetId>, Time)>,
    cycle: Time,
}

impl OpenIntersection {
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
        if self.cycle == 0 {
            return false;
        }
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

        let mut slot_idx = slot_idx.unwrap();
        if let Some(id) = slot_street {
            // Slot is already assigned
            return id == street_id;
        }

        if let Some(&street_time) = self.streets.get(&street_id) {
            // Street is not yet assigned: check if times match
            if street_time != slot_time {
                // Times do not match: try swapping it with other unused slots
                let offset = mod_time - (acc_time - slot_time);
                if let Some(new_idx) =
                    self.swap_slot(slot_idx, street_time, offset)
                {
                    // Slot swapped to accommodate street
                    slot_idx = new_idx;
                    // Check swap didn't mess up order of streets
                    let updated_time =
                        self.slots[0..slot_idx].iter().map(|(_, t)| *t).sum();
                    assert!(mod_time >= updated_time);
                    assert!(mod_time < updated_time + self.slots[slot_idx].1);
                } else {
                    // Failed to swap slot
                    return false;
                }
            }

            // Assign street to slot
            assert!(self.slots[slot_idx].0.is_none());
            assert_eq!(self.slots[slot_idx].1, street_time);
            self.slots[slot_idx] = (Some(street_id), street_time);
            self.streets.remove(&street_id);
            true
        } else {
            // Street is already assigned to another slot
            false
        }
    }

    fn swap_slot(
        &mut self,
        slot_idx: usize,
        target_time: Time,
        target_offset: Time,
    ) -> Option<usize> {
        // Try an inner swap first
        let exclude_range;
        match self.inner_swap(slot_idx, target_time, target_offset) {
            Ok(target_idx) => {
                return Some(target_idx);
            }
            Err(range) => {
                exclude_range = range;
            }
        }

        // Now try an outer swap
        let target_idx = self.outer_swap(
            slot_idx,
            &exclude_range,
            target_time,
            target_offset,
        );
        if target_idx.is_some() {
            return target_idx;
        }

        // No viable swap found
        None
    }

    fn inner_swap(
        &mut self,
        slot_idx: usize,
        target_time: Time,
        target_offset: Time,
    ) -> Result<usize, RangeInclusive<usize>> {
        let mut target_found = false;
        let mut all_sums = AllSums::default();

        let mut max_offset = target_offset;
        let mut min_offset = if max_offset >= target_time {
            max_offset - target_time + 1
        } else {
            0
        };

        // Expand region to the right
        let mut end_idx = slot_idx;
        for idx in slot_idx..self.slots.len() {
            if target_found && all_sums.contains_any(min_offset..=max_offset) {
                // Viable swap found
                break;
            }
            if self.slots[idx].0.is_some() {
                // Can't expand any further to the right
                break;
            }

            end_idx = idx;
            let curr_time = self.slots[idx].1;
            if !target_found && curr_time == target_time {
                target_found = true;
            } else {
                all_sums.add(curr_time);
            }
        }

        // Expand region to the left if necessary
        let mut start_idx = slot_idx;
        for idx in (0..slot_idx).rev() {
            if target_found && all_sums.contains_any(min_offset..=max_offset) {
                // Viable swap found
                break;
            }
            if self.slots[idx].0.is_some() {
                // Can't expand any further to the left
                break;
            }

            start_idx = idx;
            let curr_time = self.slots[idx].1;
            max_offset += curr_time;
            min_offset = if max_offset >= target_time {
                max_offset - target_time + 1
            } else {
                0
            };
            if !target_found && curr_time == target_time {
                target_found = true;
            } else {
                all_sums.add(curr_time);
            }
        }

        if !target_found || !all_sums.contains_any(min_offset..=max_offset) {
            // No viable swap found: return the examined region
            return Err(start_idx..=end_idx);
        }

        let mut slots_copy = self.slots[start_idx..=end_idx].to_vec();

        for time in all_sums
            .get_min_sum_values(min_offset..=max_offset)
            .unwrap()
            .into_iter()
            .chain(once(target_time))
        {
            let remove_idx = slots_copy
                .iter()
                .position(|(s, t)| s.is_none() && *t == time)
                .unwrap();
            slots_copy.swap_remove(remove_idx);
            self.slots[start_idx] = (None, time);
            start_idx += 1;
        }

        self.slots[start_idx..start_idx + slots_copy.len()]
            .copy_from_slice(&slots_copy);

        Ok(start_idx - 1)
    }

    fn outer_swap<T: RangeBounds<usize>>(
        &mut self,
        slot_idx: usize,
        exclude_range: &T,
        target_time: Time,
        target_offset: Time,
    ) -> Option<usize> {
        // Try to find a range of unassigned slots that can be swapped by
        // another range containing a slot with target_time time and a suitable
        // offset, whose combined times are equal
        let mut range_offset = target_offset;
        for range_start in (0..=slot_idx).rev() {
            if self.slots[range_start].0.is_some() {
                break;
            }
            if range_start < slot_idx {
                range_offset += self.slots[range_start].1;
            }
            let mut total_time = self.slots[range_start..slot_idx]
                .iter()
                .map(|(_, time)| *time)
                .sum();
            for range_end in slot_idx..self.slots.len() {
                if self.slots[range_end].0.is_some() {
                    break;
                }
                total_time += self.slots[range_end].1;
                if total_time < target_time {
                    continue;
                }

                let target_idx = self.range_swap(
                    range_start,
                    range_end,
                    exclude_range,
                    total_time,
                    target_time,
                    range_offset,
                );
                if target_idx.is_some() {
                    return target_idx;
                }
            }
        }

        // No viable swap found
        None
    }

    fn range_swap<T: RangeBounds<usize>>(
        &mut self,
        range_start: usize,
        range_end: usize,
        exclude_range: &T,
        total_time: Time,
        target_time: Time,
        target_offset: Time,
    ) -> Option<usize> {
        let max_offset = target_offset;
        let min_offset = if target_offset >= target_time {
            target_offset - target_time + 1
        } else {
            0
        };

        for start_idx in 0..self.slots.len() {
            let mut acc_time = 0;
            let mut target_found = false;
            let mut all_sums = AllSums::default();

            // Skip if start_idx is inside exclude range
            if exclude_range.contains(&start_idx) {
                continue;
            }

            for end_idx in start_idx..self.slots.len() {
                if self.slots[end_idx].0.is_some() {
                    break;
                }

                // Skip if end_idx is inside exclude range
                if exclude_range.contains(&end_idx) {
                    break;
                }

                if end_idx >= range_start && end_idx <= range_end {
                    // Overlap
                    break;
                }

                let curr_time = self.slots[end_idx].1;

                acc_time += curr_time;
                if acc_time > total_time {
                    break;
                }

                if !target_found && curr_time == target_time {
                    target_found = true;
                } else {
                    all_sums.add(curr_time);
                }

                if acc_time == total_time {
                    if !target_found
                        || !all_sums.contains_any(min_offset..=max_offset)
                    {
                        break;
                    }

                    let offset_slots = all_sums
                        .get_min_sum_values(min_offset..=max_offset)
                        .unwrap();
                    let target_delta = offset_slots.len();
                    self.rearrange_slots(
                        start_idx,
                        end_idx,
                        offset_slots,
                        target_time,
                    );

                    let target_idx = if start_idx < range_start {
                        self.reorder_ranges(
                            start_idx,
                            end_idx,
                            range_start,
                            range_end,
                        );
                        range_end - (end_idx - start_idx) + target_delta
                    } else {
                        self.reorder_ranges(
                            range_start,
                            range_end,
                            start_idx,
                            end_idx,
                        );
                        range_start + target_delta
                    };
                    return Some(target_idx);
                }
            }
        }

        // No viable swap found
        None
    }

    fn rearrange_slots(
        &mut self,
        start_idx: usize,
        end_idx: usize,
        offset_slots: Vec<Time>,
        target_time: Time,
    ) {
        let mut slots_copy = self.slots[start_idx..=end_idx].to_vec();

        let mut idx = start_idx;
        for time in offset_slots.into_iter().chain(once(target_time)) {
            let remove_idx = slots_copy
                .iter()
                .position(|(s, t)| s.is_none() && *t == time)
                .unwrap();
            slots_copy.swap_remove(remove_idx);
            self.slots[idx] = (None, time);
            idx += 1;
        }

        self.slots[idx..idx + slots_copy.len()].copy_from_slice(&slots_copy);
    }

    fn reorder_ranges(
        &mut self,
        left_start: usize,
        left_end: usize,
        right_start: usize,
        right_end: usize,
    ) {
        let mut slots_copy = self.slots[left_start..=right_end].to_vec();
        let (left, rest) = slots_copy.split_at_mut(left_end - left_start + 1);
        let (middle, right) = rest.split_at_mut(right_start - left_end - 1);

        let mid_start = left_start + right.len();
        let new_right_start = mid_start + middle.len();

        self.slots[left_start..mid_start].copy_from_slice(&right);
        self.slots[mid_start..new_right_start].copy_from_slice(&middle);
        self.slots[new_right_start..=right_end].copy_from_slice(&left);
    }

    fn assign_remaining_streets(&mut self) {
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

    fn update_schedule(
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
