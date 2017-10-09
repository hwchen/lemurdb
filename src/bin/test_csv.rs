#![recursion_limit = "1024"]

#[macro_use]
extern crate error_chain;
extern crate lemurdb;

use lemurdb::{Schema, DbIterator};
use lemurdb::io::CsvSource;
use std::io::Read;
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
    use lemurdb::DataType::*;

    let mut f_in = File::open("ratings.csv")?;

    let schema = Schema{
        column_names: vec!["userId".to_owned(), "movieId".to_owned(), "rating".to_owned(), "timestamp".to_owned()],
        column_types: vec![Integer, Integer, Float, Integer],
    };

    let mut query = CsvSource::new(f_in, schema.clone());

    let mut count = 0;
    while let Some(record) = query.next() {
        println!("{:?}", record.to_string(&schema));
        count += 1;
        if count >= 10 { break; }
    }

//    let f_out = File::create("test.lmr")?;
//    lemurdb::io::import_from_csv(f_in, f_out)?;
    Ok(())
}
