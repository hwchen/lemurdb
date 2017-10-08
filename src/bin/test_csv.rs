#![recursion_limit = "1024"]

#[macro_use]
extern crate error_chain;
extern crate lemurdb;

use std::fs::File;
use std::io::{Read, stdout};

mod error {
    error_chain!{
        foreign_links {
            Io(::std::io::Error);
        }
    }
}

use error::*;

fn main() {
    if let Err(ref err) = run() {
        println!("error: {}", err);

        for e in err.iter().skip(1) {
            println!(" cause by: {}", e);
        }

        if let Some(backtrace) = err.backtrace() {
            println!("backtrace: {:?}", backtrace);
        }

        ::std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let f = File::open("ratings.csv")?;
    lemurdb::io::import_from_csv(f, stdout());
    Ok(())
}
