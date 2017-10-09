use csv::{self, ReaderBuilder, ByteRecord};
use std::io::{Read, Write, BufWriter};

use error::*;

pub fn import_from_csv<R: Read, W: Write>(
    reader: R,
    writer: W,
    //schema: Schema,
    ) -> Result<()>
{
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(reader);

    let mut wtr = BufWriter::new(writer);

    let mut records = rdr.byte_records();

    for record in records {
        let record = record?;
        let _ = wtr.write(record.as_slice())?;
    }
    wtr.flush()?;

    Ok(())
}

// pub fn read_db(filename: &str) ->
