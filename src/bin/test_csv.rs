#![recursion_limit = "1024"]

#[macro_use]
extern crate error_chain;
extern crate lemurdb;

use std::fs::File;

mod error {
    use lemurdb;

    error_chain!{
        foreign_links {
            Io(::std::io::Error);
        }
        links {
            Lemur(lemurdb::error::Error, lemurdb::error::ErrorKind);
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
    let f_in = File::open("ratings.csv")?;
    let mut data_buf = Vec::new();
    lemurdb::io::import_from_csv(f_in, &mut data_buf)?;
    println!("{:?}", &data_buf[0..10]);

//    let f_out = File::create("test.lmr")?;
//    lemurdb::io::import_from_csv(f_in, f_out)?;
    Ok(())
}
