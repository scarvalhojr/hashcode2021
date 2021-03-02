use clap::{crate_description, value_t, App, Arg};
use hashcode2021::adapt::AdaptiveScheduler;
use hashcode2021::incr::IncrementalScheduler;
use hashcode2021::naive::NaiveScheduler;
use hashcode2021::sched::{Schedule, Scheduler};
use hashcode2021::traffic::TrafficScheduler;
use hashcode2021::{Simulation, Time};
use std::fs::{read_to_string, write};
use std::process::exit;

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

    println!(crate_description!());

    let simulation = load_simulation(args.value_of("input").unwrap());
    println!(
        "\n\
        Simulation\n\
        ----------\n\
        {}",
        simulation
    );

    let mut schedule = match args.value_of("scheduler").unwrap() {
        "load" => {
            let mut schedule = Schedule::new(&simulation);
            load_schedule(&mut schedule, args.value_of("schedule").unwrap());
            schedule
        }
        "adaptive" => AdaptiveScheduler::default().schedule(&simulation),
        "naive" => NaiveScheduler::default().schedule(&simulation),
        "traffic" => TrafficScheduler::default().schedule(&simulation),
        _ => unreachable!(),
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

    if let Some(rounds) = incremental_rounds {
        let mut scheduler = IncrementalScheduler::new(rounds);
        if let Some(value) = min_wait_time {
            scheduler.set_min_wait_time(value);
        }
        if let Some(value) = max_streets_per_round {
            scheduler.set_max_streets_per_round(value);
        }
        if let Some(value) = max_shuffles_per_street {
            scheduler.set_max_shuffles_per_street(value);
        }
        schedule = scheduler.improve(schedule);

        let sched_stats = match schedule.stats() {
            Ok(score) => score,
            Err(err) => {
                println!("\nError: {}", err);
                exit(3);
            }
        };

        println!(
            "\n\
            Schedule\n\
            --------\n\
            {}",
            sched_stats,
        );
    }

    if let Some(filename) = args.value_of("output") {
        write_output(&filename, &schedule);
    }

    exit(0);
}

fn load_simulation(filename: &str) -> Simulation {
    match read_file(filename).parse() {
        Ok(data) => data,
        Err(err) => {
            println!("Failed to parse simulation file: {}", err);
            exit(3);
        }
    }
}

fn load_schedule(schedule: &mut Schedule, filename: &str) {
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
    write(filename, sched.to_string()).expect("Unable to write file");
}
