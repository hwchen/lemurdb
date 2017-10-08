use csv::{self, ReaderBuilder, ByteRecord};
use std::io::{Read, Write};

use error::*;

pub fn import_from_csv<R: Read, W: Write>(
    reader: R,
    writer: W,
    ) -> Result<()>
{
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(reader);

    let mut records = rdr.byte_records();

    for record in records {
        let record = record?;
        println!("{:?}", record);
    }
    Ok(())
}
