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

use error::*;

// TODO figure out where the schema struct should go
// There can be an overall schema used in many modules?
// Is this separate from schema fragments used in the
// executor?
use ::executor::Schema;

/// import a csv file into db
pub fn from_csv_file<R>(
    rdr: csv::Reader<R>,
    schema: Schema,
    ) -> Result<()>
where R: Read,
{
    // TODO generate table id internally before importing
    // for now, just use the csv name.
    let table_id = Path::new("test.lmr");
    import_csv_to_disk(rdr, schema, table_id)
}

fn import_csv_to_disk<R>(
    rdr: csv::Reader<R>,
    schema: Schema,
    db_path: &Path
    ) -> Result<()>
where R: Read,
{
    // for each csv record
    //   read
    //   according to schema, turn it into bytes
    //   write to block_write_manger (DiskWriter)
    //
    Ok(())
}

