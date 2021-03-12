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
            max_add_time: 1,
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
        // Loop thought all intersections in decreasing order of total wait
        // times, reordering them; return as soon as an improvement is found
        info!(
            "Phased improver, phase 1: reordering intersections with non-zero \
            wait times, {} intersections selected",
            intersections.len()
        );

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
                turns.len(),
                inter_wait
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
            "Phased improver, phase 2: adding 1 second to traffic lights of \
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
                    "Phase 2: skipping street {} ({}/{}), {} seconds wait, \
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
                "Phase 2: street {} ({}/{}), {} seconds wait, intersection {}, \
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
                    "New best score {} after adding 1 second to traffic lights \
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
}
