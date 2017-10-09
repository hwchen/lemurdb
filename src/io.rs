use csv::{self, ReaderBuilder, StringRecord, StringRecordsIntoIter};
use std::fs::File;
use std::io::{Read, Write, BufWriter};

use ::{DbIterator, Schema, DataType};
use error::*;
use scan::Scan;
use tuple::Tuple;

pub struct CsvSource<R: Read> {
    input: StringRecordsIntoIter<R>,
    schema: Schema,
}

impl<R: Read> CsvSource<R> {
    pub fn new (rdr: R, schema: Schema) -> Self {
        let mut rdr = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(rdr)
            .into_records();

        CsvSource {
            input: rdr,
            schema: schema,
        }
    }
}

impl<R: Read> DbIterator for CsvSource<R> {
    fn next(&mut self) -> Option<Tuple> {
        self.input.next().map(|record| {
            Tuple::from_stringrecord(record.expect("could not read csv record"), &self.schema).expect("convert to tuple failed")
        })
    }
}

//pub fn import_from_csv<R: Read, W: Write>(
//    reader: R,
//    writer: W,
//    //schema: Schema,
//    ) -> Result<()>
//{
//    let mut rdr = ReaderBuilder::new()
//        .has_headers(true)
//        .from_reader(reader);
//
//    let mut wtr = BufWriter::new(writer);
//
//    let mut records = rdr.byte_records();
//
//    for record in records {
//        let record = record?;
//        let _ = wtr.write(record.as_slice())?;
//    }
//    wtr.flush()?;
//
//    Ok(())
//}
