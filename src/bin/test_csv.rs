#![recursion_limit = "1024"]

#[macro_use]
extern crate error_chain;
extern crate lemurdb;

use lemurdb::{Schema, DataType, DbIterator};
use lemurdb::io::CsvSource;
use lemurdb::simplesort::SortOrder;
use lemurdb::aggregate::{AggregateType};
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

    let f_in = File::open("ratings.csv")?;

    let schema = Schema{
        column_names: vec!["userId".to_owned(), "movieId".to_owned(), "rating".to_owned(), "timestamp".to_owned()],
        column_types: vec![Integer, Integer, Float, Integer],
    };

    // Test sort and limit
//    let mut query = CsvSource::new(f_in, schema.clone())
//        .simplesort(2, DataType::Float, SortOrder::Descending)
//        .limit(50);
//
//    while let Some(record) = query.next() {
//        println!("{:?}", record.to_string(&schema));
//    }

    // test overall aggregate
    let f_in = File::open("ratings.csv")?;
    let final_schema = Schema{
        column_names: vec!["rating count".to_owned()],
        column_types: vec![Integer],
    };
    let mut query = CsvSource::new(f_in, schema.clone())
        .aggregate(AggregateType::Count, 2, DataType::Float, None);

    while let Some(record) = query.next() {
        println!("{:?}", record.to_string(&final_schema));
    }

    // test group by aggregate

    Ok(())
}
