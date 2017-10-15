// In order to be able to reset an io node, the node needs to hold
// the information about the reader
// TODO all nodes need a rest. But nodes that hold state don't need
// to call reset on the input node.
use csv::{ReaderBuilder, StringRecordsIntoIter};
use std::fs::File;

use super::{DbIterator, Schema};
use super::tuple::Tuple;

pub struct CsvSource {
    location: String,
    output: StringRecordsIntoIter<File>,
    schema: Schema,
}

impl CsvSource {
    pub fn new (location: String, schema: Schema) -> Self {

        let f = File::open(&location).expect("fix this panic");

        let rdr = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(f)
            .into_records();

        CsvSource {
            location: location,
            output: rdr,
            schema: schema,
        }
    }
}

impl DbIterator for CsvSource {
    fn next(&mut self) -> Option<Tuple> {
        self.output.next().map(|record| {
            Tuple::from_stringrecord(
                record.expect("could not read csv record"),
                &self.schema
            ).expect("convert to tuple failed")
        })
    }

    fn reset(&mut self) {
        // hacky, let's fix this to be consistent with original
        // initialization
        let f = File::open(&self.location).expect("fix this panic to rtn err");
        self.output = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(f)
            .into_records();
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
