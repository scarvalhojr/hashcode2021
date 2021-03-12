use super::*;
use crate::improve::Improver;
use crate::intersect::reorder_intersection;
use crate::sched::{Schedule, ScheduleStats};
use log::{debug, info};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct PhasedImprover {
    max_add_time: Time,
    max_sub_time: Time,
    add_new_streets: bool,
    max_streets_per_inter: usize,
}

impl Default for PhasedImprover {
    fn default() -> Self {
        Self {
            max_add_time: 2,
            max_sub_time: 1,
            max_streets_per_inter: 30,
            add_new_streets: true,
        }
    }
}

impl PhasedImprover {
    pub fn set_max_add_time(&mut self, max_add_time: Time) {
        self.max_add_time = max_add_time;
    }

    pub fn set_max_sub_time(&mut self, max_sub_time: Time) {
        self.max_sub_time = max_sub_time;
    }

    pub fn set_add_new_streets(&mut self, add_new_streets: bool) {
        self.add_new_streets = add_new_streets;
    }
}

impl Improver for PhasedImprover {
    fn improve<'a>(
        &self,
        abort_flag: Arc<AtomicBool>,
        schedule: Schedule<'a>,
    ) -> Option<(Schedule<'a>, Score)> {
        let stats = schedule.stats().unwrap();

        // Sum up total wait time by intersection
        let mut inter_wait: HashMap<IntersectionId, Time> = HashMap::new();
        for (&street_id, &time) in
            stats.total_wait_time.iter().filter(|&(&street_id, _)| {
                !schedule.is_street_always_green(street_id)
            })
        {
            let inter_id = schedule.get_intersection_id(street_id).unwrap();
            inter_wait
                .entry(inter_id)
                .and_modify(|total_time| *total_time += time)
                .or_insert_with(|| time);
        }

        let mut intersections: Vec<(IntersectionId, Time)> =
            inter_wait.into_iter().collect();

        // Sort intersections by total wait time
        intersections.sort_unstable_by(|a, b| b.1.cmp(&a.1));

        // Phase 1
        let result1 = self.phase1(
            abort_flag.clone(),
            schedule.clone(),
            stats.score,
            &intersections,
        );
        if result1.is_some() || abort_flag.load(Ordering::SeqCst) {
            return result1;
        }

        // Phase 2
        let result2 = self.phase2(abort_flag.clone(), schedule.clone(), &stats);
        if result2.is_some() || abort_flag.load(Ordering::SeqCst) {
            return result2;
        }

        // Phase 3
        let result3 = self.phase3(
            abort_flag.clone(),
            schedule.clone(),
            &stats,
            &intersections,
        );
        if result3.is_some() || abort_flag.load(Ordering::SeqCst) {
            return result3;
        }

        // No improvement found
        None
    }
}

impl PhasedImprover {
    fn phase1<'a>(
        &self,
        abort_flag: Arc<AtomicBool>,
        schedule: Schedule<'a>,
        curr_score: Score,
        intersections: &[(IntersectionId, Time)],
    ) -> Option<(Schedule<'a>, Score)> {
        info!(
            "Phased improver, phase 1: reordering intersections with non-zero \
            wait times, {} intersections selected",
            intersections.len()
        );

        // Loop thought all intersections in decreasing order of total wait
        // times, reordering them; return as soon as an improvement is found
        for (&(inter_id, inter_wait), count) in intersections.iter().zip(1..) {
            if abort_flag.load(Ordering::SeqCst) {
                return None;
            }

            let turns = &schedule.intersections.get(&inter_id).unwrap().turns;
            debug!(
                "Phase 1: intersection {} ({}/{}), {} total wait, {} streets",
                inter_id,
                count,
                intersections.len(),
                inter_wait,
                turns.len(),
            );

            // Try to improve intersection by reordering streets
            // without changing their times
            let mut new_schedule = schedule.clone();
            let new_score = reorder_intersection(&mut new_schedule, inter_id);
            if new_score > curr_score {
                info!(
                    "New best score {} after reordering intersection {} (\
                    previous total wait time {}, {} streets), {} \
                    intersection(s) examined",
                    new_score,
                    inter_id,
                    inter_wait,
                    turns.len(),
                    count,
                );
                return Some((new_schedule, new_score));
            }
        }

        // No improvement found
        None
    }

    fn phase2<'a>(
        &self,
        abort_flag: Arc<AtomicBool>,
        schedule: Schedule<'a>,
        curr_stats: &ScheduleStats,
    ) -> Option<(Schedule<'a>, Score)> {
        if self.max_add_time == 0 {
            info!("Phased improver, phase 2: skipping since max_add_time is 0");
            return None;
        }

        // Collect all streets with non-zero wait times whose traffic lights
        // are not always green
        let mut streets: Vec<(StreetId, Time)> = curr_stats
            .total_wait_time
            .iter()
            .filter(|&(&street_id, _)| {
                !schedule.is_street_always_green(street_id)
            })
            .map(|(&street_id, &time)| (street_id, time))
            .collect();

        // Sort streets by wait time
        streets.sort_unstable_by(|a, b| b.1.cmp(&a.1));

        info!(
            "Phased improver, phase 2: adding 1 sec to traffic lights of \
            streets with non-zero wait times, {} streets selected, {} max \
            streets per intersection",
            streets.len(),
            self.max_streets_per_inter,
        );

        // Loop through all streets in decreasing order of wait times; add 1 to
        // the street's traffic light and reorder the intersection; return as
        // soon as an improvement is found
        for (&(street_id, street_wait), count) in streets.iter().zip(1..) {
            if abort_flag.load(Ordering::SeqCst) {
                break;
            }

            let inter_id = schedule.get_intersection_id(street_id).unwrap();
            let turns = &schedule.intersections.get(&inter_id).unwrap().turns;
            if turns.len() > self.max_streets_per_inter {
                debug!(
                    "Phase 2: skipping street {} ({}/{}), {} sec wait, \
                    intersection {}, {} streets in the intersection",
                    inter_id,
                    count,
                    streets.len(),
                    street_wait,
                    inter_id,
                    turns.len(),
                );
                continue;
            }

            debug!(
                "Phase 2: street {} ({}/{}), {} sec wait, intersection {}, \
                {} streets in the intersection",
                inter_id,
                count,
                streets.len(),
                street_wait,
                inter_id,
                turns.len(),
            );

            let mut new_schedule = schedule.clone();
            new_schedule.add_street_time(street_id, 1);
            let new_score = reorder_intersection(&mut new_schedule, inter_id);
            if new_score > curr_stats.score {
                info!(
                    "New best score {} after adding 1 sec to traffic lights \
                    of street {} (previous wait time {}), intersection {}, {} \
                    streets in the intersection, {} street(s) examined",
                    new_score,
                    street_id,
                    street_wait,
                    inter_id,
                    turns.len(),
                    count,
                );
                return Some((new_schedule, new_score));
            }
        }

        // No improvement found
        None
    }

    fn phase3<'a>(
        &self,
        abort_flag: Arc<AtomicBool>,
        schedule: Schedule<'a>,
        curr_stats: &ScheduleStats,
        intersections: &[(IntersectionId, Time)],
    ) -> Option<(Schedule<'a>, Score)> {
        if self.max_add_time >= 2 && self.max_sub_time >= 1 {
            info!(
                "Phased improver, phase 3: subtracting 1 sec from, or adding 2 \
                sec to streets of intersections with non-zero wait times, \
                {} intersections selected",
                intersections.len()
            );
        } else if self.max_add_time >= 2 {
            info!(
                "Phased improver, phase 3: adding 2 sec to streets of \
                intersections with non-zero wait times, {} intersections \
                selected",
                intersections.len()
            );
        } else if self.max_sub_time >= 1 {
            info!(
                "Phased improver, phase 3: subtracting 1 sec from streets of \
                intersections with non-zero wait times, {} intersections \
                selected",
                intersections.len()
            );
        } else {
            info!(
                "Phased improver, phase 3: skipping since max_add_time is {} \
                and max_sub_time is {}",
                self.max_add_time, self.max_sub_time,
            );
            return None;
        }

        let mut best_score = curr_stats.score;
        let mut best_sched = None;

        // Loop thought all intersections in decreasing order of total wait
        for (&(inter_id, inter_wait), count) in intersections.iter().zip(1..) {
            let turns = &schedule.intersections.get(&inter_id).unwrap().turns;

            debug!(
                "Phase 3: intersection {} ({}/{}), {} total wait, {} streets",
                inter_id,
                count,
                intersections.len(),
                inter_wait,
                turns.len(),
            );

            // Loop through all streets in the intersection
            for &(street_id, _) in turns.iter() {
                if abort_flag.load(Ordering::SeqCst) {
                    return None;
                }

                let wait_time =
                    *curr_stats.total_wait_time.get(&street_id).unwrap_or(&0);

                if wait_time > 0 {
                    if self.max_add_time < 2
                        || turns.len() > self.max_streets_per_inter
                    {
                        // Can't add more time
                        continue;
                    }
                } else if self.max_sub_time < 1 {
                    // Can't substract time
                    continue;
                }

                let mut new_schedule = schedule.clone();
                if wait_time > 0 {
                    new_schedule.add_street_time(street_id, 2);
                } else {
                    new_schedule.sub_street_time(street_id, 1);
                }
                let new_score =
                    reorder_intersection(&mut new_schedule, inter_id);
                if new_score > best_score {
                    best_score = new_score;
                    best_sched = Some(new_schedule);
                }
            }

            if let Some(best_schedule) = best_sched {
                info!(
                    "New best score {} after updating intersection {} (\
                    previous total wait time {}, {} streets), {} \
                    intersection(s) examined",
                    best_score,
                    inter_id,
                    inter_wait,
                    turns.len(),
                    count,
                );
                return Some((best_schedule, best_score));
            }
        }

        // No improvement found
        None
    }
}
