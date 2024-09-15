
use std::fs::OpenOptions;
use std::io::stdout;
use std::io::{BufRead, BufReader, Write};

use clap::{CommandFactory, Parser};
use colored::{self, *};
use log::LevelFilter;
use regex::Regex;

use command::{do_command, Cli, SuiS3Cmd};
use logger::Logger;

mod command;
mod logger;
mod utils;
mod operations;
mod walrus;

static MY_LOGGER: Logger = Logger;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    log::set_logger(&MY_LOGGER).unwrap();
    log::set_max_level(LevelFilter::Warn);

    let mut matches = Cli::parse();

    let mut interactive: bool;
    if matches.suis3_cmd.is_some() {
        interactive = false;
        let args: Vec<String> = std::env::args().collect();
        matches.suis3_cmd = Some(SuiS3Cmd::parse_from(args[0..].iter()));
    } else {
        interactive = true;
    };

    let mut command = String::new();
    while matches.suis3_cmd != Some(SuiS3Cmd::Quit) {
        stdout().flush().expect("Could not flush stdout");

        if command.starts_with("exit") || command.starts_with("quit") {
            interactive = false;
        } else {
            do_command(matches.suis3_cmd.take()).await;
        }

        if !interactive {
            break;
        }

        command = match OpenOptions::new().read(true).write(true).open("/dev/tty") {
            Ok(mut tty) => {
                tty.flush().expect("Could not open tty");
                let _ = tty.write_all(
                    format!("{} {} ", "suis3".green(), ">".green())
                        .as_bytes(),
                );
                let reader = BufReader::new(&tty);
                let mut command_iter = reader.lines().map(|l| l.unwrap());
                command_iter.next().unwrap_or("logout".to_string())
            }
            Err(e) => {
                println!("{:?}", e);
                "quit".to_string()
            }
        };

        matches.suis3_cmd = if command.starts_with("help") {
            let mut usage = Vec::<u8>::new();
            let mut command = <SuiS3Cmd as CommandFactory>::command();
            command
                .write_help(&mut usage)
                .expect("fail to get help of program");
            let usage = unsafe { std::str::from_utf8_unchecked(&usage) };
            let mut after_match = false;
            let re = Regex::new("Commands:").unwrap();
            let option_re = Regex::new("Options:").unwrap();
            for line in usage.split('\n') {
                if option_re.is_match(line) {
                    break;
                }
                if after_match {
                    println!("{}", line);
                }
                if !after_match && re.is_match(line) {
                    after_match = true;

                }
            }
            None
        } else {
            let mut new_s3_cmd = vec![""];
            new_s3_cmd.append(&mut command.split_whitespace().collect());
            SuiS3Cmd::try_parse_from(new_s3_cmd).ok()
        };
    }

    Ok(())
}
