use clap::{crate_description, value_t, App, Arg};
use ctrlc::set_handler;
use hashcode2021::adapt::AdaptiveScheduler;
use hashcode2021::greedy::GreedyImprover;
use hashcode2021::improve::IncrementalImprover;
use hashcode2021::naive::NaiveScheduler;
use hashcode2021::phased::PhasedImprover;
use hashcode2021::sched::{Schedule, Scheduler};
use hashcode2021::shuffle::ShuffleImprover;
use hashcode2021::traffic::TrafficScheduler;
use hashcode2021::{Simulation, Time};
use log::info;
use std::fs::{read_to_string, write};
use std::process::exit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn main() {
    let args = App::new(crate_description!())
        .arg(
            Arg::with_name("input")
                .value_name("simulation file")
                .help("File with simulation input")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("scheduler")
                .help("Load schedule from file or run scheduler algorithm")
                .required(true)
                .possible_values(&["load", "naive", "adaptive", "traffic"])
                .index(2),
        )
        .arg(
            Arg::with_name("improver")
                .value_name("incremental improver")
                .help("Incremental improver algorithm")
                .possible_values(&["shuffle", "phased", "greedy"])
                .index(3),
        )
        .arg(
            Arg::with_name("schedule")
                .value_name("schedule file")
                .help("Schedule file to load as starting solution")
                .required_if("scheduler", "load")
                .short("l")
                .long("schedule-file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("output")
                .value_name("output file")
                .help("File to save schedule solution")
                .short("o")
                .long("output")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("best-of")
                .help("Run scheduler multiple times, keep best schedule")
                .short("b")
                .long("best-of")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("incremental-rounds")
                .help("Number of incremental rounds")
                .short("r")
                .long("incremental-rounds")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("min-wait-time")
                .help("Minimum waiting time per street on incremental rounds")
                .short("w")
                .long("min-wait-time")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("max-add-time")
                .help("Maximum time added to a street on incremental rounds")
                .short("a")
                .long("max-add-time")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("max-sub-time")
                .help("Maximum time subtracted from a street on incremental rounds")
                .short("m")
                .long("max-sub-time")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("no-add-new-streets")
                .help("Do not add new streets to traffic lights")
                .short("n")
                .long("no-add-new-streets")
        )
        .arg(
            Arg::with_name("max-streets-per-round")
                .help("Maximum number of streets per round on incremental rounds")
                .short("s")
                .long("max-streets-per-round")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("max-shuffles-per-street")
                .help("Maximum number of shuffles per street on incremental rounds")
                .short("x")
                .long("max-shuffles-per-street")
                .takes_value(true),
        )
        .get_matches();

    let best_of = if args.is_present("best-of") {
        let value = value_t!(args.value_of("best-of"), u32)
            .unwrap_or_else(|e| e.exit());
        value
    } else {
        1
    };

    let incremental_rounds = if args.is_present("incremental-rounds") {
        let value = value_t!(args.value_of("incremental-rounds"), u32)
            .unwrap_or_else(|e| e.exit());
        Some(value)
    } else {
        None
    };

    let min_wait_time = if args.is_present("min-wait-time") {
        let value = value_t!(args.value_of("min-wait-time"), Time)
            .unwrap_or_else(|e| e.exit());
        Some(value)
    } else {
        None
    };

    let max_add_time = if args.is_present("max-add-time") {
        let value = value_t!(args.value_of("max-add-time"), Time)
            .unwrap_or_else(|e| e.exit());
        Some(value)
    } else {
        None
    };

    let max_sub_time = if args.is_present("max-sub-time") {
        let value = value_t!(args.value_of("max-sub-time"), Time)
            .unwrap_or_else(|e| e.exit());
        Some(value)
    } else {
        None
    };

    let max_streets_per_round = if args.is_present("max-streets-per-round") {
        let value = value_t!(args.value_of("max-streets-per-round"), usize)
            .unwrap_or_else(|e| e.exit());
        Some(value)
    } else {
        None
    };

    let max_shuffles_per_street = if args.is_present("max-shuffles-per-street")
    {
        let value = value_t!(args.value_of("max-shuffles-per-street"), usize)
            .unwrap_or_else(|e| e.exit());
        Some(value)
    } else {
        None
    };

    env_logger::init();
    println!(crate_description!());

    let simulation = load_simulation(args.value_of("input").unwrap());
    println!(
        "\n\
        Simulation\n\
        ----------\n\
        {}",
        simulation
    );

    let schedule = match args.value_of("scheduler").unwrap() {
        "load" => {
            let mut schedule = Schedule::new(&simulation);
            load_schedule(&mut schedule, args.value_of("schedule").unwrap());
            schedule
        }
        algorithm => {
            let mut best_score = 0;
            let mut best_sched = None;
            for num in 1..=best_of {
                let sched = match algorithm {
                    "adaptive" => AdaptiveScheduler::default().schedule(&simulation),
                    "naive" => NaiveScheduler::default().schedule(&simulation),
                    "traffic" => TrafficScheduler::default().schedule(&simulation),
                    _ => unreachable!(),
                };
                let score = sched.stats().map(|stats| stats.score).unwrap_or(0);
                info!("Schedule {}/{}: score {}", num, best_of, score);
                if score >= best_score {
                    best_score = score;
                    best_sched = Some(sched);
                }
            }
            best_sched.unwrap()
        }
    };

    let sched_stats = match schedule.stats() {
        Ok(score) => score,
        Err(err) => {
            println!("\nError: {}", err);
            exit(4);
        }
    };
    println!(
        "\n\
        Schedule\n\
        --------\n\
        {}",
        sched_stats,
    );

    // Setup Ctrl-C handler
    let abort_flag = Arc::new(AtomicBool::new(false));
    let abort_clone = abort_flag.clone();
    set_handler(move || {
        eprintln!("Received termination request");
        abort_clone.store(true, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    let final_schedule = match args.value_of("improver") {
        Some(algorithm_name) => {
            let mut improver = IncrementalImprover::new(abort_flag);
            if let Some(rounds) = incremental_rounds {
                improver.set_max_rounds(rounds);
            }

            let improved_schedule = match algorithm_name {
                "greedy" => {
                    let mut greedy = GreedyImprover::default();
                    if let Some(value) = min_wait_time {
                        greedy.set_min_wait_time(value);
                    }
                    if let Some(value) = max_add_time {
                        greedy.set_max_add_time(value);
                    }
                    if let Some(value) = max_streets_per_round {
                        greedy.set_max_streets(value);
                    }
                    improver.improve(&schedule, &greedy)
                }
                "phased" => {
                    let mut phased = PhasedImprover::default();
                    if let Some(value) = max_add_time {
                        phased.set_max_add_time(value);
                    }
                    if let Some(value) = max_sub_time {
                        phased.set_max_sub_time(value);
                    }
                    if args.is_present("no-add-new-streets") {
                        phased.set_add_new_streets(false);
                    }
                    improver.improve(&schedule, &phased)
                }
                "shuffle" => {
                    let mut shuffle = ShuffleImprover::default();
                    if let Some(value) = min_wait_time {
                        shuffle.set_min_wait_time(value);
                    }
                    if let Some(value) = max_streets_per_round {
                        shuffle.set_max_streets(value);
                    }
                    if let Some(value) = max_shuffles_per_street {
                        shuffle.set_max_shuffles(value);
                    }
                    improver.improve(&schedule, &shuffle)
                }
                _ => unreachable!(),
            };

            let improved_stats = match improved_schedule.stats() {
                Ok(score) => score,
                Err(err) => {
                    println!("\nError: {}", err);
                    exit(3);
                }
            };

            println!(
                "\n\
                Improved schedule\n\
                -----------------\n\
                {}",
                improved_stats,
            );
            improved_schedule
        }
        _ => schedule,
    };

    if let Some(filename) = args.value_of("output") {
        write_output(&filename, &final_schedule);
    }

    exit(0);
}

fn load_simulation(filename: &str) -> Simulation {
    info!("Loading simulation from '{}'", filename);
    match read_file(filename).parse() {
        Ok(data) => data,
        Err(err) => {
            println!("Failed to parse simulation file: {}", err);
            exit(3);
        }
    }
}

fn load_schedule(schedule: &mut Schedule, filename: &str) {
    info!("Loading schedule from '{}'", filename);
    if let Err(err) = schedule.load_from_str(&read_file(filename)) {
        println!("Failed to parse schedule file: {}", err);
        exit(5);
    }
}

fn read_file(filename: &str) -> String {
    match read_to_string(filename) {
        Ok(data) => data,
        Err(err) => {
            println!("Failed to read '{}': {}", filename, err);
            exit(2);
        }
    }
}

fn write_output(filename: &str, sched: &Schedule) {
    info!("Writing schedule to '{}'", filename);
    write(filename, sched.to_string()).expect("Unable to write file");
}
