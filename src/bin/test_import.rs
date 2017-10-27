#![recursion_limit = "1024"]

#[macro_use]
extern crate error_chain;
extern crate lemurdb;

use lemurdb::{Schema, RelationSchema, DataType};
use lemurdb::executor::DbIterator;
use lemurdb::executor::simplesort::SortOrder;
use lemurdb::executor::aggregate::{AggregateType};
use lemurdb::storage::from_csv;
use lemurdb::storage::disk::{DiskScan};

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

    let rating_schema = RelationSchema{
        name: "ratings".to_owned(),
        id: 1,
        column_names: vec!["userId".to_owned(), "movieId".to_owned(), "rating".to_owned(), "timestamp".to_owned()],
        column_types: vec![Integer, Integer, Float, Integer],
    };

    let movie_schema = RelationSchema {
        name: "movies".to_owned(),
        id: 2,
        column_names: vec!["movieId".to_owned(), "title".to_owned(), "genres".to_owned()],
        column_types: vec![Integer, Text(255), Text(255)],
    };

    // join and agg schemas
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
            Text(255),
            Text(255),
        ],
    };

    let agg_schema = Schema{
        column_names: vec!["title".to_owned(), "rating count".to_owned()],
        column_types: vec![Text(255), Integer],
    };

    // start import
    //let _ = from_csv("test_data/test_ratings.csv", rating_schema.clone());
    //let _ = from_csv("test_data/test_movies.csv", movie_schema.clone());

    // start query
    let movies = DiskScan::from_path("2", movie_schema.column_types.clone())?;
    let mut query = DiskScan::from_path("1", rating_schema.column_types.clone())?
        .nested_loops_join(movies,1, 0)
        .simplesort(5, DataType::Text(255), SortOrder::Ascending)
        .aggregate(AggregateType::Count, 2, DataType::Float, Some(5));

    let mut count = 0;
    while let Some(record) = query.next() {
        println!("{:?}, {}", record.to_string(&agg_schema), count);
//        println!("{:?}, {}", record.to_string(&Schema {
//            column_names: rating_schema.column_names.clone(),
//            column_types: rating_schema.column_types.clone(),
//        }), count);
        count += 1;
    }

    Ok(())
}
