#![recursion_limit = "1024"]

#[macro_use]
extern crate error_chain;
extern crate lemurdb;

use lemurdb::{Schema, DataType, DbIterator};
use lemurdb::io::CsvSource;
use lemurdb::simplesort::SortOrder;
use lemurdb::aggregate::{AggregateType};

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

    let rating_schema = Schema{
        column_names: vec!["userId".to_owned(), "movieId".to_owned(), "rating".to_owned(), "timestamp".to_owned()],
        column_types: vec![Integer, Integer, Float, Integer],
    };

    let movie_schema = Schema {
        column_names: vec!["movieId".to_owned(), "title".to_owned(), "genres".to_owned()],
        column_types: vec![Integer, Text, Text],
    };

    // Test sort and limit
//    let mut query = CsvSource::new("ratings.csv".to_owned(), schema.clone())
//        .simplesort(1, DataType::Integer, SortOrder::Ascending)
//        .limit(50);
//
//    while let Some(record) = query.next() {
//        println!("{:?}", record.to_string(&schema));
//    }

    // test aggregate
//    let final_schema = Schema{
//        column_names: vec!["movie_id".to_owned(), "rating count".to_owned()],
//        column_types: vec![Integer, Integer],
//    };
//    let mut query = CsvSource::new("ratings.csv".to_owned(), schema.clone())
//        .simplesort(1, DataType::Integer, SortOrder::Ascending)
//        .aggregate(AggregateType::Count, 2, DataType::Float, Some(1));
//
//    while let Some(record) = query.next() {
//        println!("{:?}", record.to_string(&final_schema));
//    }

    // test join
    let joined_schema = Schema {
        column_names: vec![
            "userId".to_owned(),
            "movieId".to_owned(),
            "rating".to_owned(),
            "timestamp".to_owned(),
            "movieId".to_owned(),
            "title".to_owned(),
            "genres".to_owned()
        ],
        column_types: vec![
            Integer,
            Integer,
            Float,
            Integer,
            Integer,
            Text,
            Text,
        ],
    };

    let agg_schema = Schema{
        column_names: vec!["title".to_owned(), "rating count".to_owned()],
        column_types: vec![Text, Integer],
    };
    let movies = CsvSource::new("test_movies.csv".to_owned(), movie_schema);
    let mut query = CsvSource::new("test_ratings.csv".to_owned(), rating_schema)
        .nested_loops_join(movies,1, 0)
        .simplesort(5, DataType::Text, SortOrder::Ascending)
        .aggregate(AggregateType::Count, 2, DataType::Float, Some(5));

    while let Some(record) = query.next() {
        println!("{:?}", record.to_string(&agg_schema));
    }

    Ok(())
}
