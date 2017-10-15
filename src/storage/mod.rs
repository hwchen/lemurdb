//! Storage module
//!
//! - module for handling binary disk storage
//! - module for buffering a file scan
//! - convenience functions for importing from csv
//! TODO figure out whether or not Text should have a fixed len

pub mod disk;

use csv;
use std::io::Read;
use std::path::Path;

use RelationSchema;
use error::*;

/// import a csv file into db
pub fn from_csv<R>(
    rdr: csv::Reader<R>,
    schema: RelationSchema, // do i need col names for better
    ) -> Result<()>
where R: Read,
{
    // schema contains the tableid
    // for each csv record
    //   read
    //   according to schema, turn it into bytes
    //   write to block_write_manger (DiskWriter)
    //
    Ok(())
}

