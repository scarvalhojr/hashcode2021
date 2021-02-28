use clap::{crate_description, App, Arg};
use hashcode2021::naive::NaiveScheduler;
use hashcode2021::sched::{Schedule, Scheduler};
use hashcode2021::traffic::TrafficScheduler;
use hashcode2021::Simulation;
use std::fs::{read_to_string, write};
use std::process::exit;

fn main() {
    let args = App::new(crate_description!())
        .arg(
            Arg::with_name("input")
                .value_name("input file")
                .help("File path to puzzle input")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("algorithm")
                .help("Scheduler algorithm")
                .required(true)
                .possible_values(&["naive", "traffic"])
                .index(2),
        )
        .arg(
            Arg::with_name("output")
                .value_name("output file")
                .help("File path to save solution")
                .short("o")
                .long("output")
                .takes_value(true),
        )
        .get_matches();

    println!(crate_description!());

    let simulation = match read_input(args.value_of("input").unwrap()) {
        Ok(data) => data,
        Err(err) => {
            println!("Failed to read input: {}", err);
            exit(2);
        }
    };

    println!(
        "\n\
        Simulation\n\
        ----------\n\
        {}",
        simulation
    );

    let schedule = match args.value_of("algorithm").unwrap() {
        "naive" => NaiveScheduler::default().schedule(&simulation),
        "traffic" => TrafficScheduler::default().schedule(&simulation),
        _ => unreachable!(),
    };

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

    if let Some(filename) = args.value_of("output") {
        write_output(&filename, &schedule);
    }

    exit(0);
}

fn read_input(filename: &str) -> Result<Simulation, String> {
    read_to_string(filename)
        .map_err(|err| err.to_string())?
        .parse()
}

fn write_output(filename: &str, sched: &Schedule) {
    write(filename, sched.to_string()).expect("Unable to write file");
}
