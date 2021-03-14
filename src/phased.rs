use super::*;
use crate::improve::Improver;
use crate::intersect::{reorder_intersection, reorder_intersections};
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
            max_add_time: 6,
            max_sub_time: 3,
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

        // // Collect all streets with non-zero wait times whose traffic lights
        // // are not always green
        // let mut streets: Vec<(StreetId, Time)> = stats
        //     .total_wait_time
        //     .iter()
        //     .filter(|&(&street_id, _)| {
        //         !schedule.is_street_always_green(street_id)
        //     })
        //     .map(|(&street_id, &time)| (street_id, time))
        //     .collect();

        // // Sort streets by wait time
        // streets.sort_unstable_by(|a, b| b.1.cmp(&a.1));

        // // Phase 2
        // let result2 = self.phase2(
        //     abort_flag.clone(),
        //     schedule.clone(),
        //     stats.score,
        //     &streets,
        // );
        // if result2.is_some() || abort_flag.load(Ordering::SeqCst) {
        //     return result2;
        // }

        // // Phase 3
        // let result3 = self.phase3(
        //     abort_flag.clone(),
        //     schedule.clone(),
        //     stats.score,
        //     &streets,
        // );
        // if result3.is_some() || abort_flag.load(Ordering::SeqCst) {
        //     return result3;
        // }

        // // Phase 4
        // let result4 = self.phase4(
        //     abort_flag.clone(),
        //     schedule.clone(),
        //     &stats,
        //     &intersections,
        // );
        // if result4.is_some() || abort_flag.load(Ordering::SeqCst) {
        //     return result4;
        // }

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
            "Phased improver, phase 1: reordering {} intersections",
            intersections.len(),
        );

        // Try to improve intersection by reordering all intersections
        // without changing their times
        for count in 1..=intersections.len() {
            for window in intersections.windows(count) {
                if abort_flag.load(Ordering::SeqCst) {
                    break;
                }

                debug!(
                    "Reordering {} intersections starting with intersection {}",
                    count, window[0].0,
                );

                let mut new_schedule = schedule.clone();
                let new_score = reorder_intersections(
                    &mut new_schedule,
                    window.iter().map(|(inter_id, _)| *inter_id),
                );
                let new_stats = new_schedule.stats().unwrap();
                assert_eq!(new_score, new_stats.score);
                if new_score > curr_score {
                    info!(
                        "New best score {} after reordering {} intersections",
                        new_score, count,
                    );
                    return Some((new_schedule, new_score));
                }
                debug!("Score after reordering intersections: {}", new_score);
            }
        }

        // No improvement found
        None
    }

    fn phase2<'a>(
        &self,
        abort_flag: Arc<AtomicBool>,
        schedule: Schedule<'a>,
        curr_score: Score,
        streets: &[(StreetId, Time)],
    ) -> Option<(Schedule<'a>, Score)> {
        if self.max_add_time == 0 {
            info!("Phased improver, phase 2: skipping since max_add_time is 0");
            return None;
        }

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
                    street_id,
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
                street_id,
                count,
                streets.len(),
                street_wait,
                inter_id,
                turns.len(),
            );

            let mut new_schedule = schedule.clone();
            new_schedule.add_street_time(street_id, 1);
            let new_score = reorder_intersection(&mut new_schedule, inter_id);
            if new_score > curr_score {
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
        curr_score: Score,
        streets: &[(StreetId, Time)],
    ) -> Option<(Schedule<'a>, Score)> {
        if !self.add_new_streets {
            info!(
                "Phased improver, phase 3: skipping (add_new_streets is false)"
            );
            return None;
        }

        info!(
            "Phased improver, phase 3: adding streets with non-zero wait times \
            that are not in the schedule"
        );

        // Loop through all streets in decreasing order of wait times and if
        // they are not in the schedule, add them; return as soon as an
        // improvement is found
        for &(street_id, street_wait) in streets.iter() {
            if abort_flag.load(Ordering::SeqCst) {
                break;
            }

            let inter_id = schedule.get_intersection_id(street_id).unwrap();
            let turns = &schedule.intersections.get(&inter_id).unwrap().turns;
            if turns.iter().any(|&(id, _)| id == street_id) {
                continue;
            }

            debug!(
                "Phase 3: adding new street {}, {} sec wait, to intersection \
                {}, {} streets in the intersection",
                street_id,
                street_wait,
                inter_id,
                turns.len(),
            );

            let mut new_schedule = schedule.clone();
            new_schedule.add_street_time(street_id, 1);
            let new_score = reorder_intersection(&mut new_schedule, inter_id);
            if new_score > curr_score {
                info!(
                    "New best score {} after adding new street {} with time 1 \
                    (previous wait time {}) to intersection {}, {} streets in \
                    the intersection",
                    new_score,
                    street_id,
                    street_wait,
                    inter_id,
                    turns.len(),
                );
                return Some((new_schedule, new_score));
            }
        }

        None
    }

    fn phase4<'a>(
        &self,
        abort_flag: Arc<AtomicBool>,
        schedule: Schedule<'a>,
        curr_stats: &ScheduleStats,
        intersections: &[(IntersectionId, Time)],
    ) -> Option<(Schedule<'a>, Score)> {
        for time in 1.. {
            let add_time = if time < self.max_add_time {
                time + 1
            } else {
                0
            };
            let sub_time = if time <= self.max_sub_time { time } else { 0 };
            if add_time == 0 && sub_time == 0 {
                break;
            }
            let result = self.phase4_loop(
                abort_flag.clone(),
                schedule.clone(),
                curr_stats,
                intersections,
                add_time,
                sub_time,
            );
            if result.is_some() {
                return result;
            }
        }

        // No improvement found
        None
    }

    fn phase4_loop<'a>(
        &self,
        abort_flag: Arc<AtomicBool>,
        schedule: Schedule<'a>,
        curr_stats: &ScheduleStats,
        intersections: &[(IntersectionId, Time)],
        add_time: Time,
        sub_time: Time,
    ) -> Option<(Schedule<'a>, Score)> {
        assert!(add_time > 0 || sub_time > 0);
        assert!(add_time <= self.max_add_time);
        assert!(sub_time <= self.max_sub_time);

        if add_time > 0 && sub_time > 0 {
            info!(
                "Phased improver, phase 4: subtracting {} sec from, or adding \
                {} sec to streets of intersections with non-zero wait times, \
                {} intersections selected",
                sub_time,
                add_time,
                intersections.len()
            );
        } else if add_time > 0 {
            info!(
                "Phased improver, phase 4: adding {} sec to streets of \
                intersections with non-zero wait times, {} intersections \
                selected",
                add_time,
                intersections.len()
            );
        } else {
            info!(
                "Phased improver, phase 4: subtracting {} sec from streets of \
                intersections with non-zero wait times, {} intersections \
                selected",
                sub_time,
                intersections.len()
            );
        }

        let mut best_score = curr_stats.score;
        let mut best_sched = None;

        // Loop thought all intersections in decreasing order of total wait
        for (&(inter_id, inter_wait), count) in intersections.iter().zip(1..) {
            let turns = &schedule.intersections.get(&inter_id).unwrap().turns;

            debug!(
                "Phase 4: intersection {} ({}/{}), {} total wait, {} streets",
                inter_id,
                count,
                intersections.len(),
                inter_wait,
                turns.len(),
            );

            // Loop through all streets in the intersection
            for &(street_id, street_time) in turns.iter() {
                if abort_flag.load(Ordering::SeqCst) {
                    return None;
                }

                let wait_time =
                    *curr_stats.total_wait_time.get(&street_id).unwrap_or(&0);

                if wait_time > 0 {
                    if add_time == 0 || turns.len() > self.max_streets_per_inter
                    {
                        // Can't add time
                        continue;
                    }
                } else if sub_time == 0 || street_time < sub_time {
                    // Can't substract time
                    continue;
                }

                let mut new_schedule = schedule.clone();
                if wait_time > 0 {
                    new_schedule.add_street_time(street_id, add_time);
                } else {
                    new_schedule.sub_street_time(street_id, sub_time);
                }
                let new_score =
                    reorder_intersection(&mut new_schedule, inter_id);
                if new_score > best_score {
                    best_score = new_score;
                    best_sched = Some(new_schedule);
                    if wait_time > 0 {
                        info!(
                            "New best score {} after adding {} sec to street \
                            {} ({} time, {} wait time), intersection {}",
                            best_score,
                            add_time,
                            street_id,
                            street_time,
                            wait_time,
                            inter_id,
                        );
                    } else {
                        info!(
                            "New best score {} after subtracting {} sec from \
                            street {} ({} time, {} wait time), intersection {}",
                            best_score,
                            sub_time,
                            street_id,
                            street_time,
                            wait_time,
                            inter_id,
                        );
                    }
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
