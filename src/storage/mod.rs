//! Storage module
//!
//! - module for handling binary disk storage
//! - module for buffering a file scan
//! - convenience functions for importing from csv
//! TODO figure out whether or not Text should have a fixed len

pub mod disk;

use csv;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use ::executor::tuple::Tuple;
use ::{RelationSchema, Schema};
use self::disk::{DiskWriter, DiskScan};
use error::*;

/// import a csv file into db
pub fn from_csv(
    path: &str,
    schema: RelationSchema, // do i need col names for better
    ) -> Result<()>
{
    // schema contains the tableid
    // for each csv record
    //   read
    //   according to schema, turn it into bytes
    //   write to block_write_manger (DiskWriter)
    //

    let f_write = File::create(schema.id.to_string())?;
    let mut wtr = DiskWriter::new(f_write, schema.clone())?; //TODO datatypes instead of schema
    let mut rdr = csv::Reader::from_path(path)?;
    for result in rdr.records() { // TODO in the future use byterecords
        let record = result?;

        let tuple = Tuple::from_stringrecord(
            record,
            &Schema {
                column_names: vec![],
                column_types: schema.column_types.clone(),
            }
        )?;
        wtr.add_tuple(tuple)?;
    }
    wtr.flush()?;
    Ok(())
}

