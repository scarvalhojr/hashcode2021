use super::*;
use crate::improve::Improver;
use crate::intersect::{reorder_intersection, reorder_intersections};
use crate::sched::{Schedule, ScheduleStats};
use crate::shuffle::bounded_factorial;
use log::{debug, info};
use rand::thread_rng;
use rayon::prelude::*;
use std::collections::HashSet;
use std::iter::{once, repeat};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct PhasedImprover {
    max_add_time: Time,
    max_sub_time: Time,
    max_streets_per_inter: usize,
    max_shuffles_per_inter: usize,
    max_shuffles_per_thread: usize,
}

impl Default for PhasedImprover {
    fn default() -> Self {
        Self {
            max_add_time: 10,
            max_sub_time: 5,
            max_streets_per_inter: 30,
            // Ideally max_shuffles_per_inter should not be a multiple of
            // max_shuffles_per_thread to avoid spanwing a thread to do zero
            // shuffles
            max_shuffles_per_inter: 259,
            max_shuffles_per_thread: 26,
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

    pub fn set_max_shuffles(&mut self, max_shuffles_per_inter: usize) {
        self.max_shuffles_per_inter = max_shuffles_per_inter;
    }

    pub fn set_max_streets_per_inter(&mut self, max_streets_per_inter: usize) {
        self.max_streets_per_inter = max_streets_per_inter;
    }
}

impl Improver for PhasedImprover {
    fn improve<'a>(
        &self,
        abort_flag: Arc<AtomicBool>,
        schedule: Schedule<'a>,
    ) -> Option<(Schedule<'a>, Score)> {
        let stats = schedule.stats(false).unwrap();

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
            &stats,
            &intersections,
        );
        if result1.is_some() || abort_flag.load(Ordering::SeqCst) {
            return result1;
        }

        // Collect all streets with non-zero wait times whose traffic lights
        // are not always green
        let mut streets: Vec<(StreetId, Time)> = stats
            .total_wait_time
            .iter()
            .filter(|&(&street_id, _)| {
                !schedule.is_street_always_green(street_id)
            })
            .map(|(&street_id, &time)| (street_id, time))
            .collect();

        // Sort streets by wait time
        streets.sort_unstable_by(|a, b| b.1.cmp(&a.1));

        // Phase 2
        let result2 =
            self.phase2(abort_flag.clone(), schedule.clone(), &stats, &streets);
        if result2.is_some() || abort_flag.load(Ordering::SeqCst) {
            return result2;
        }

        // Phase 3
        let result3 = self.phase3(
            abort_flag.clone(),
            schedule.clone(),
            stats.score,
            &intersections,
        );
        if result3.is_some() || abort_flag.load(Ordering::SeqCst) {
            return result3;
        }

        // Phase 4
        let result4 = self.phase4(
            abort_flag.clone(),
            schedule.clone(),
            stats.score,
            &streets,
        );
        if result4.is_some() || abort_flag.load(Ordering::SeqCst) {
            return result4;
        }

        // Phase 5
        let result5 = self.phase5(
            abort_flag.clone(),
            schedule.clone(),
            &stats,
            &intersections,
        );
        if result5.is_some() || abort_flag.load(Ordering::SeqCst) {
            return result5;
        }

        // Phase 6
        let result6 = self.phase6(
            abort_flag.clone(),
            schedule.clone(),
            stats.score,
            &intersections,
        );
        if result6.is_some() || abort_flag.load(Ordering::SeqCst) {
            return result6;
        }

        // Phase 7
        let result7 = self.phase7(
            abort_flag.clone(),
            schedule.clone(),
            &stats,
            &intersections,
        );
        if result7.is_some() || abort_flag.load(Ordering::SeqCst) {
            return result7;
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
        curr_stats: &ScheduleStats,
        intersections: &[(IntersectionId, Time)],
    ) -> Option<(Schedule<'a>, Score)> {
        info!(
            "Phased improver, phase 1: removing all streets that were never \
            crossed, examining {} intersections with wait times",
            intersections.len(),
        );

        let mut new_sched = schedule.clone();
        let mut modified_inter = HashSet::new();
        let mut removed = Vec::new();

        // Loop thought all intersections and remove any street has has not
        // been crossed by a car
        for &(inter_id, _) in intersections.iter() {
            // Loop through all streets in the intersection
            let turns = &schedule.intersections.get(&inter_id).unwrap().turns;
            for &(street_id, street_time) in turns.iter() {
                if curr_stats.crossed_streets.contains(&street_id) {
                    // Street was crossed by a car
                    continue;
                }
                let inter = new_sched.intersections.get_mut(&inter_id).unwrap();
                let removed_time = inter.remove_street(street_id);
                assert_eq!(removed_time, Some(street_time));
                removed.push((inter_id, street_id));
                modified_inter.insert(inter_id);
            }
        }

        if removed.is_empty() {
            return None;
        }

        let new_score =
            reorder_intersections(&mut new_sched, modified_inter.into_iter());
        if new_score > curr_stats.score {
            info!(
                "New best score {} after removing {} streets that were never \
                crossed by any car",
                new_score,
                removed.len(),
            );
            return Some((new_sched, new_score));
        }

        debug!(
            "Removing {} streets that were never crossed by any car \
            produced worse or same score: {}",
            removed.len(),
            new_score,
        );

        // Try to remove streets individually, return as soon as an improvement
        // is found
        for (inter_id, street_id) in removed.into_iter() {
            if abort_flag.load(Ordering::SeqCst) {
                return None;
            }

            let street_wait =
                *curr_stats.total_wait_time.get(&street_id).unwrap_or(&0);
            let mut new_sched = schedule.clone();
            let inter = new_sched.intersections.get_mut(&inter_id).unwrap();
            let street_time = inter.remove_street(street_id).unwrap();
            let new_score = reorder_intersection(&mut new_sched, inter_id);
            if new_score > curr_stats.score {
                info!(
                    "New best score {} after removing street {} (time {}, \
                    wait {}) from intersection {}, since it was never crossed \
                    by any car",
                    new_score, street_id, street_time, street_wait, inter_id,
                );
                return Some((new_sched, new_score));
            }

            debug!(
                "Removing street {} (time {}, wait {}) from intersection {} \
                (since it was never crossed by any car) produced worse or \
                same score: {}",
                street_id, street_time, street_wait, inter_id, new_score,
            );
        }

        // No improvement found
        None
    }

    fn phase2<'a>(
        &self,
        abort_flag: Arc<AtomicBool>,
        schedule: Schedule<'a>,
        curr_stats: &ScheduleStats,
        streets: &[(StreetId, Time)],
    ) -> Option<(Schedule<'a>, Score)> {
        info!(
            "Phased improver, phase 2: adding streets with non-zero wait times \
            that are not in the schedule, examining {} streets with wait times",
            streets.len(),
        );

        // Loop through all streets in decreasing order of wait times, add it
        // with time 1 if it's not yet in the schedule, and reorder the
        // intersection; return as soon as an improvement is found
        for (&(street_id, street_wait), count) in streets.iter().zip(1..) {
            if abort_flag.load(Ordering::SeqCst) {
                break;
            }

            if curr_stats.crossed_streets.contains(&street_id) {
                continue;
            }

            if schedule.get_street_time(street_id).is_some() {
                continue;
            }

            let inter_id = schedule.get_intersection_id(street_id).unwrap();
            let mut new_schedule = schedule.clone();
            new_schedule.add_street(inter_id, street_id, 1);

            let new_score = reorder_intersection(&mut new_schedule, inter_id);
            if new_score > curr_stats.score {
                info!(
                    "New best score {} after adding new street {} (previous \
                    wait time {}) to intersection {}, {} street(s) examined",
                    new_score, street_id, street_wait, inter_id, count,
                );
                return Some((new_schedule, new_score));
            }

            debug!(
                "Adding new street {} (previous wait time {}) to \
                intersection {} resulted in same or worse score: {}",
                street_id, street_wait, inter_id, new_score,
            );
        }

        // No improvement found
        None
    }

    fn phase3<'a>(
        &self,
        abort_flag: Arc<AtomicBool>,
        schedule: Schedule<'a>,
        curr_score: Score,
        intersections: &[(IntersectionId, Time)],
    ) -> Option<(Schedule<'a>, Score)> {
        info!(
            "Phased improver, phase 3: reordering intersections with non-zero \
            wait times, {} intersections selected",
            intersections.len()
        );

        // Loop thought all intersections in decreasing order of total wait
        // times, reordering them; return as soon as an improvement is found
        intersections
            .par_iter()
            .find_map_any(|&(inter_id, inter_wait)| {
                if abort_flag.load(Ordering::SeqCst) {
                    return None;
                }
                self.reorder_intersection(
                    schedule.clone(),
                    curr_score,
                    inter_id,
                    inter_wait,
                )
            })
    }

    fn reorder_intersection<'a>(
        &self,
        mut schedule: Schedule<'a>,
        curr_score: Score,
        inter_id: IntersectionId,
        inter_wait: Time,
    ) -> Option<(Schedule<'a>, Score)> {
        debug!(
            "Phase 3: reordering intersection {}, {} total wait, {} streets",
            inter_id,
            inter_wait,
            schedule.num_streets_in_intersection(inter_id),
        );

        let new_score = reorder_intersection(&mut schedule, inter_id);
        if new_score > curr_score {
            info!(
                "New best score {} after reordering intersection {} (\
                previous total wait time {}, {} streets)",
                new_score,
                inter_id,
                inter_wait,
                schedule.num_streets_in_intersection(inter_id),
            );
            Some((schedule, new_score))
        } else {
            // No improvement found
            None
        }
    }

    fn phase4<'a>(
        &self,
        abort_flag: Arc<AtomicBool>,
        schedule: Schedule<'a>,
        curr_score: Score,
        streets: &[(StreetId, Time)],
    ) -> Option<(Schedule<'a>, Score)> {
        if self.max_add_time == 0 {
            info!("Phased improver, phase 4: skipping since max_add_time is 0");
            return None;
        }

        info!(
            "Phased improver, phase 4: adding 1 sec to streets with non-zero \
            wait times, {} streets selected, {} max streets per intersection",
            streets.len(),
            self.max_streets_per_inter,
        );

        // Loop through all streets in decreasing order of wait times; add 1 to
        // the street's traffic light and reorder the intersection; return as
        // soon as an improvement is found
        streets
            .par_iter()
            .find_map_any(|&(street_id, street_wait)| {
                if abort_flag.load(Ordering::SeqCst) {
                    return None;
                }
                self.add_street_time(
                    schedule.clone(),
                    curr_score,
                    street_id,
                    street_wait,
                )
            })
    }

    fn add_street_time<'a>(
        &self,
        mut schedule: Schedule<'a>,
        curr_score: Score,
        street_id: StreetId,
        street_wait: Time,
    ) -> Option<(Schedule<'a>, Score)> {
        let inter_id = schedule.get_intersection_id(street_id).unwrap();
        let num_streets = schedule.num_streets_in_intersection(inter_id);

        if num_streets > self.max_streets_per_inter {
            debug!(
                "Phase 4: skipping street {} ({} sec wait), as intersection {} \
                has {} streets in the intersection (max allowed: {})",
                street_id,
                street_wait,
                inter_id,
                num_streets,
                self.max_streets_per_inter,
            );
            return None;
        }

        debug!(
            "Phase 4: street {} ({} sec wait), intersection {}, {} streets in \
            the intersection",
            street_id, street_wait, inter_id, num_streets,
        );

        schedule.add_street_time(street_id, 1);
        let new_score = reorder_intersection(&mut schedule, inter_id);
        if new_score > curr_score {
            info!(
                "New best score {} after adding 1 sec to street {} (previous \
                wait time {}), intersection {}, {} streets in the intersection",
                new_score, street_id, street_wait, inter_id, num_streets,
            );
            Some((schedule, new_score))
        } else {
            // No improvement found
            None
        }
    }

    fn phase5<'a>(
        &self,
        abort_flag: Arc<AtomicBool>,
        schedule: Schedule<'a>,
        curr_stats: &ScheduleStats,
        intersections: &[(IntersectionId, Time)],
    ) -> Option<(Schedule<'a>, Score)> {
        self.add_or_sub_time_range(
            5,
            1..=2,
            abort_flag,
            schedule,
            curr_stats,
            intersections,
        )
    }

    fn phase6<'a>(
        &self,
        abort_flag: Arc<AtomicBool>,
        schedule: Schedule<'a>,
        curr_score: Score,
        intersections: &[(IntersectionId, Time)],
    ) -> Option<(Schedule<'a>, Score)> {
        info!(
            "Phased improver, phase 6: shuffling intersections with non-zero \
            wait times, {} intersections selected",
            intersections.len(),
        );

        // Loop thought all intersections in decreasing order of total wait
        // times, shuffling them; return as soon as an improvement is found
        intersections
            .par_iter()
            .flat_map_iter(|&(inter_id, inter_wait)| {
                let num_streets =
                    schedule.num_streets_in_intersection(inter_id);
                let shuffles =
                    bounded_factorial(num_streets, self.max_shuffles_per_inter);
                debug!(
                    "Phase 6: intersection {}, {} total wait, {} streets, {} \
                    shuffles",
                    inter_id, inter_wait, num_streets, shuffles,
                );

                let full = shuffles / self.max_shuffles_per_thread;
                let remain = shuffles - full * self.max_shuffles_per_thread;

                repeat((inter_id, inter_wait, self.max_shuffles_per_thread))
                    .take(full)
                    .chain(once((inter_id, inter_wait, remain)))
            })
            .find_map_any(|(inter_id, inter_wait, shuffles)| {
                if abort_flag.load(Ordering::SeqCst) {
                    return None;
                }
                self.shuffle_intersection(
                    schedule.clone(),
                    curr_score,
                    inter_id,
                    inter_wait,
                    shuffles,
                )
            })
    }

    fn shuffle_intersection<'a>(
        &self,
        mut schedule: Schedule<'a>,
        curr_score: Score,
        inter_id: IntersectionId,
        inter_wait: Time,
        shuffles: usize,
    ) -> Option<(Schedule<'a>, Score)> {
        let mut rng = thread_rng();

        // Try to improve intersection by randomly shuffling streets without
        // changing their times, return as soon as improvement is found
        for _ in 1..=shuffles {
            schedule.shuffle_intersection(inter_id, &mut rng);
            let new_score = schedule.score().unwrap();
            if new_score > curr_score {
                let num_streets =
                    schedule.num_streets_in_intersection(inter_id);
                info!(
                    "New best score {} after shuffling intersection {} (\
                    previous total wait time {}, {} streets)",
                    new_score, inter_id, inter_wait, num_streets,
                );
                return Some((schedule, new_score));
            }
        }

        // No improvement found
        None
    }

    fn phase7<'a>(
        &self,
        abort_flag: Arc<AtomicBool>,
        schedule: Schedule<'a>,
        curr_stats: &ScheduleStats,
        intersections: &[(IntersectionId, Time)],
    ) -> Option<(Schedule<'a>, Score)> {
        self.add_or_sub_time_range(
            7,
            3..,
            abort_flag,
            schedule,
            curr_stats,
            intersections,
        )
    }

    fn add_or_sub_time_range<'a, I>(
        &self,
        phase: u32,
        time_range: I,
        abort_flag: Arc<AtomicBool>,
        schedule: Schedule<'a>,
        curr_stats: &ScheduleStats,
        intersections: &[(IntersectionId, Time)],
    ) -> Option<(Schedule<'a>, Score)>
    where
        I: IntoIterator<Item = Time>,
    {
        for time in time_range {
            let add_time = if time < self.max_add_time {
                time + 1
            } else {
                0
            };
            let sub_time = if time <= self.max_sub_time { time } else { 0 };
            if add_time == 0 && sub_time == 0 {
                break;
            }
            let result = self.add_or_sub_loop(
                phase,
                abort_flag.clone(),
                schedule.clone(),
                curr_stats,
                intersections,
                add_time,
                sub_time,
            );
            if result.is_some() || abort_flag.load(Ordering::SeqCst) {
                return result;
            }
        }

        // No improvement found
        None
    }

    fn add_or_sub_loop<'a>(
        &self,
        phase: u32,
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
                "Phased improver, phase {}: subtracting {} sec from, or adding \
                {} sec to streets of intersections with non-zero wait times, \
                {} intersections selected",
                phase,
                sub_time,
                add_time,
                intersections.len()
            );
        } else if add_time > 0 {
            info!(
                "Phased improver, phase {}: adding {} sec to streets of \
                intersections with non-zero wait times, {} intersections \
                selected",
                phase,
                add_time,
                intersections.len()
            );
        } else {
            info!(
                "Phased improver, phase {}: subtracting {} sec from streets of \
                intersections with non-zero wait times, {} intersections \
                selected",
                phase,
                sub_time,
                intersections.len()
            );
        }

        // Loop thought all intersections in decreasing order of total wait
        intersections
            .par_iter()
            .find_map_any(|&(inter_id, inter_wait)| {
                if abort_flag.load(Ordering::SeqCst) {
                    return None;
                }
                self.add_or_sub_inter_time(
                    phase,
                    schedule.clone(),
                    curr_stats,
                    inter_id,
                    inter_wait,
                    add_time,
                    sub_time,
                )
            })
    }

    fn add_or_sub_inter_time<'a>(
        &self,
        phase: u32,
        schedule: Schedule<'a>,
        curr_stats: &ScheduleStats,
        inter_id: IntersectionId,
        inter_wait: Time,
        mut add_time: Time,
        sub_time: Time,
    ) -> Option<(Schedule<'a>, Score)> {
        let streets = &schedule.intersections.get(&inter_id).unwrap().turns;
        let num_streets = streets.len();
        if num_streets > self.max_streets_per_inter {
            // Can't add time to streets of this intersection
            add_time = 0;

            if sub_time == 0 {
                // If we're not subtracting time, then there's nothing to do
                return None;
            }
        }

        debug!(
            "Phase {}: intersection {}, {} total wait, {} streets",
            phase, inter_id, inter_wait, num_streets,
        );

        let mut best_score = curr_stats.score;
        let mut best_sched = None;

        // Loop through all streets in the intersection, pick the best
        // improvement (if any is found)
        for &(street_id, street_time) in streets.iter() {
            let wait_time =
                *curr_stats.total_wait_time.get(&street_id).unwrap_or(&0);

            if wait_time > 0 {
                if add_time == 0 {
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
            let new_score = reorder_intersection(&mut new_schedule, inter_id);
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
            Some((best_schedule, best_score))
        } else {
            // No improvement found
            None
        }
    }
}
