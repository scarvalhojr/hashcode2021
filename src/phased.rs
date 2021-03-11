use super::*;
use crate::improve::Improver;
use crate::intersect::reorder_intersection;
use crate::sched::Schedule;
use log::{debug, info};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct PhasedImprover {
    max_add_time: Time,
    max_sub_time: Time,
    re_add_streets: bool,
}

impl Default for PhasedImprover {
    fn default() -> Self {
        Self {
            max_add_time: 1,
            max_sub_time: 1,
            re_add_streets: true,
        }
    }
}

impl PhasedImprover {
    pub fn set_max_add_time(&mut self, max_add_time: Time) {
        self.max_add_time = max_add_time;
    }

    pub fn set_max_sub_time(&mut self, max_sub_time: Time) {
        self.max_sub_time = max_sub_time;
        self.re_add_streets = max_sub_time > 0;
    }
}

impl Improver for PhasedImprover {
    fn improve<'a>(
        &self,
        abort_flag: Arc<AtomicBool>,
        schedule: Schedule<'a>,
    ) -> Option<(Schedule<'a>, Score)> {

        let stats = schedule.stats().unwrap();

        // Collect all streets with non-zero wait times whose traffic lights
        // are not always green
        let mut streets: Vec<(StreetId, Time)> = stats
            .total_wait_time
            .into_iter()
            .filter(|&(street_id, _)| {
                !schedule.is_street_always_green(street_id)
            })
            .collect();

        // Sum up total wait time by intersection
        let mut inter_wait: HashMap<IntersectionId, Time> = HashMap::new();
        for &(street_id, time) in streets.iter() {
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

        if self.max_add_time >= 1 {
            // Sort streets by wait time
            streets.sort_unstable_by(|a, b| b.1.cmp(&a.1));

            // Phase 2
            let result2 = self.phase2(
                abort_flag.clone(),
                schedule.clone(),
                stats.score,
                &streets,
            );
            if result2.is_some() || abort_flag.load(Ordering::SeqCst) {
                return result2;
            }
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
        score: Score,
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
            reorder_intersection(&mut new_schedule, inter_id);
            let new_stats = new_schedule.stats().unwrap();
            if new_stats.score > score {
                info!(
                    "New best score {} after reordering intersection {} (\
                    previous total wait time {}, {} streets), {} \
                    intersection(s) examined",
                    new_stats.score,
                    inter_id,
                    inter_wait,
                    turns.len(),
                    count,
                );
                return Some((new_schedule, new_stats.score));
            }
        }

        // No improvement found
        None
    }

    fn phase2<'a>(
        &self,
        abort_flag: Arc<AtomicBool>,
        schedule: Schedule<'a>,
        score: Score,
        streets: &[(StreetId, Time)],
    ) -> Option<(Schedule<'a>, Score)> {
        // Loop through all streets in decreasing order of wait times; add 1 to
        // the street's traffic light and reorder the intersection; return as
        // soon as an improvement is found
        info!(
            "Phased improver, phase 2: adding 1 second to traffic lights of \
            streets with non-zero wait times, {} streets selected",
            streets.len()
        );

        for (&(street_id, street_wait), count) in streets.iter().zip(1..) {
            if abort_flag.load(Ordering::SeqCst) {
                break;
            }

            let inter_id = schedule.get_intersection_id(street_id).unwrap();
            debug!(
                "Phase 2: street {} ({}/{}), {} seconds wait, intersection {}",
                inter_id, count, streets.len(), street_wait, inter_id,
            );

            let mut new_schedule = schedule.clone();
            new_schedule.add_street_time(street_id, 1);
            reorder_intersection(&mut new_schedule, inter_id);
            let new_stats = new_schedule.stats().unwrap();
            if new_stats.score > score {
                info!(
                    "New best score {} after adding 1 second to traffic lights \
                    of street {} (previous wait time {}), intersection {}, {} \
                    street(s) examined",
                    new_stats.score, street_id, street_wait, inter_id, count,
                );
                return Some((new_schedule, new_stats.score));
            }
        }

        // No improvement found
        None
    }
}
