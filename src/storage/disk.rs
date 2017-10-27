// TODO use memmap for reader

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use std::io::{Read, Write, Seek, SeekFrom};
use std::fs::File;

// TODO: move all these to common module?
use ColumnTypes;
use executor::tuple::Tuple;
use executor::DbIterator; //TODO move dbiterator to top level mod?
use error::*;

/// The DiskWriter (and block manager) holds:
///    - handle to the file
///    - buffer for write io (do this myself for now, to see how it
///      works, but could use a system BufWriter)
///    - current block buffer
///    - current block attributes:
///        - next free spot for record pointer
///        - next free spot for record
///
/// Notes:
/// - writes in 8k blocks
/// - currently only writes a completely new file. No updates or inserts
pub struct DiskWriter<W> {
    write_handle: W,
    write_buffer: Vec<u8>, // holds bytes to append to file on disk
    block_buffer: [u8; 8000], // holds current block being written to
    block_upper: u16, // pointer to beginning of free space
    block_lower: u16, // pointer to end of free space
}

impl<W: Write> DiskWriter<W> {
    pub fn new(writer: W) -> Result<Self> {

        Ok(DiskWriter {
            write_handle: writer,
            write_buffer: Vec::new(),
            block_buffer: [0; 8000],
            block_upper: 4, // leave space for header
            block_lower: 8000,
        })
    }

    pub fn add_tuple(&mut self, tuple: Tuple) -> Result<()> {
        // when adding record:
        // - check len of tuple
        // - see if it will fit in block
        // - if yes, write to block
        // - if no, write block to file_buffer and 
        let tuple_len = tuple.data.len() as u16; // assume len less than block

        let free_space = self.block_lower - self.block_upper;

        // tuple len plus the one u16 pointer need to fit in block
        if tuple_len + 2 > free_space {
            self.write_buffer.extend_from_slice(&self.block_buffer);

            // block_upper leaves space for header
            self.block_upper = 4;
            self.block_lower = 8000;

            // can I do this better?
            // This seems like a weird hack
            // TODO figure out how to zero out a fixed array.
            // or just zero out the pointers?
            (&mut self.block_buffer[..]).write(&[0;8000])?;
        }

        // now write to block
        let tuple_start = self.block_lower - tuple_len;

        (&mut self.block_buffer[tuple_start as usize..self.block_lower as usize]).write(&tuple.data)?;

        (&mut self.block_buffer[self.block_upper as usize..self.block_upper as usize+2])
            .write_u16::<BigEndian>(tuple_start)?;

        // increment pointers and free space pointers write to block
        self.block_upper = self.block_upper + 2;
        self.block_lower = tuple_start;
        (&mut self.block_buffer[0..2]).write_u16::<BigEndian>(self.block_upper)?;
        (&mut self.block_buffer[2..4]).write_u16::<BigEndian>(self.block_lower)?;

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        // for guaranteeing that all blocks will be written to disk
        // Writes current block to file_buffer, write file_buffer to disk
        // for now, just writes the write_buffer at once to file.
        // In future, would probably flush at intervals

        self.write_buffer.extend_from_slice(&self.block_buffer);
        self.write_handle.write_all(&mut self.write_buffer)
            .chain_err(|| "error flushing")
    }
}

/// The DiskScan (and block manager) holds:
///    - handle to the file
///    - buffer for read io (do this myself for now, to see how it
///      works, but could use a system BufReader)
///    - current block buffer
///    - current block attributes:
///        - next free spot for record pointer
///        - next free spot for record
///
/// Notes:
/// - reads in 8k blocks
pub struct DiskScan<R> {
    read_handle: R,
    block_buffer: [u8; 8000], // holds current block being written to
    record_pointers: Vec<u16>,
    current_record_pointer: usize, //index into record_pointers
    tuple_indexes: Vec<usize>,
    record_length: usize,
}

impl<R: Read + Seek> DiskScan<R> {
    pub fn new(mut reader: R, col_types: ColumnTypes) -> Result<Self> {
        // On initializationg, read first block
        // for now, just read all to read_buffer (in future, this
        //   could be variable length buffer, so as not to read
        //   entire file at once

        let mut block_buffer = [0u8; 8000];

        reader.read_exact(&mut block_buffer)?;

        // convert record pointers
        // this can be done in one step using unsafe

        // add 1 to this to get the number of bytes to take
        let end_record_pointers = (&block_buffer[0..2]).read_u16::<BigEndian>()?;

        let record_pointers_bytes: Vec<u8> = block_buffer
            .iter()
            .skip(4)
            .cloned() // can I avoid?
            .take(end_record_pointers as usize - 4) // take until
            .collect();

        let record_pointers = record_pointers_bytes.chunks(2)
            .map(|mut bytes| {
                bytes.read_u16::<BigEndian>()
                    .chain_err(|| "error converting rec pointer to u16")
            })
            .collect::<Result<Vec<u16>>>()?;


        // map schema to indexes of fields in tuple
        // also calculate record length
        let mut tuple_indexes = Vec::new();
        let mut offset = 0;
        for col_type in &col_types {
            tuple_indexes.push(offset);
            offset += col_type.bytes_length();
        }
        let record_length = offset;

        Ok(DiskScan {
            read_handle: reader,
            block_buffer: block_buffer,
            record_pointers: record_pointers,
            current_record_pointer: 0,
            tuple_indexes: tuple_indexes,
            record_length: record_length,
        })
    }

}

impl DiskScan<File> {
    pub fn from_path(path: &str, col_types: ColumnTypes) -> Result<Self> {
        let f = File::open(path)?;
        Self::new(f, col_types)
    }
}

impl<R:Read + Seek> DbIterator for DiskScan<R> {
    fn next(&mut self) -> Option<Tuple> {
        // record_pointers needs to hold an iterator instead of a vec
        // iterator should be Windows?
        // then map, using schema to determine where offsets should be
        // ugh, text does have to be a fixed allocation
        if self.current_record_pointer >= self.record_pointers.len() {
            // load next block
            // todo refactor this with the initializing block load?
            if self.read_handle.read_exact(&mut self.block_buffer).ok().is_none() {
                return None;
            }
            self.init_record_pointers();
        }

        let start = self.record_pointers[self.current_record_pointer] as usize;
        let end = start + self.record_length;
        let record = &self.block_buffer[start..end];

        self.current_record_pointer += 1;

        Some(Tuple {
            data: record.to_vec(),
            indexes: self.tuple_indexes.clone(),
        })

    }

    fn reset(&mut self) {
        self.read_handle.seek(SeekFrom::Start(0)).unwrap();
        self.read_handle.read_exact(&mut self.block_buffer).unwrap();
        self.init_record_pointers();
    }
}

impl<R:Read + Seek> DiskScan<R> {
    fn init_record_pointers(&mut self) {
        // convert record pointers
        // this can be done in one step using unsafe

        // add 1 to this to get the number of bytes to take
        let end_record_pointers = (&self.block_buffer[0..2]).read_u16::<BigEndian>().unwrap();

        let record_pointers_bytes: Vec<u8> = self.block_buffer
            .iter()
            .skip(4)
            .cloned() // can I avoid?
            .take(end_record_pointers as usize - 4) // take until
            .collect();

        let record_pointers = record_pointers_bytes.chunks(2)
            .map(|mut bytes| {
                bytes.read_u16::<BigEndian>()
                    .chain_err(|| "error converting rec pointer to u16")
            })
            .collect::<Result<Vec<u16>>>().unwrap();

        self.record_pointers = record_pointers;
        self.current_record_pointer = 0;
    }
}

#[cfg(test)]
mod tests {
    use csv::StringRecord;
    use std::io::Cursor;
    use super::*;

    // TODO deprecate Schema
    use Schema;
    use {RelationSchema, DataType};

    fn generate_relation_schema() -> RelationSchema{
        RelationSchema {
            name: "test".to_owned(),
            id: 11111111,
            column_names: vec!["0".to_owned(), "1".to_owned()],
            column_types: vec![DataType::SmallInt, DataType::Text(5)],
        }
    }

    #[test]
    fn test_block_buffer_one_write() {
        let schema = generate_relation_schema();
        let f = Cursor::new(Vec::new());
        let mut disk_writer = DiskWriter::new(f).unwrap();

        // to_tuple
        let tuple_bytes = Tuple::from_stringrecord(
            StringRecord::from(
                vec!["17", "testy"]
            ),
            &Schema{
                column_names: schema.column_names,
                column_types:schema.column_types
            }
        ).unwrap();
        // Then write tuple to diskwriter
        disk_writer.add_tuple(tuple_bytes.clone()).unwrap();

        let mut expected = [0;8000];
        // header of 4 bytes (two pointers to free space), plus one pointer to record
        (&mut expected[0..6]).write(&[0x00, 0x06, 0x1F, 0x39, 0x1F, 0x39]).unwrap();
        (&mut expected[7993..8000]).write(&[0, 17, 116, 101, 115, 116, 121]).unwrap();

        assert_eq!(disk_writer.block_buffer[0..6], expected[0..6]);
        assert_eq!(disk_writer.block_buffer[7993..8000], expected[7993..8000]);

        // Then write one more tuple to diskwriter
        disk_writer.add_tuple(tuple_bytes).unwrap();

        let mut expected = [0;8000];
        // header of 4 bytes (two pointers to free space),
        // plus two pointers to records
        (&mut expected[0..6]).write(&[0x00, 0x08, 0x1F, 0x32, 0x1F, 0x39, 0x1F, 0x32]).unwrap();
        (&mut expected[7986..8000]).write(&[0, 17, 116, 101, 115, 116, 121, 0, 17, 116, 101, 115, 116, 121]).unwrap();

        assert_eq!(disk_writer.block_buffer[0..6], expected[0..6]);
        assert_eq!(disk_writer.block_buffer[7993..8000], expected[7993..8000]);
    }

    #[test]
    fn test_block_buffer_overflow() {
        let schema = generate_relation_schema();
        let f = Cursor::new(Vec::new());
        let mut disk_writer = DiskWriter::new(f).unwrap();

        // to_tuple
        let tuple_bytes = Tuple::from_stringrecord(
            StringRecord::from(
                vec!["17", "test"]
            ),
            &Schema{
                column_names: schema.column_names,
                column_types: vec![DataType::SmallInt, DataType::Text(4)],
            }
        ).unwrap();

        // Then write tuple to diskwriter 1000 times
        // (each time adds 8 bytes: 2 byte pointer, 6 byte record
        // since header is 4 bytes, the 1000 time should not fit in 8000bytes
        // and there should be an overflow
        for _ in 0..1000 {
            disk_writer.add_tuple(tuple_bytes.clone()).unwrap();
        }

        // for the block, expect one record
        let mut expected = [0;8000];
        // header of 4 bytes (two pointers to free space), plus one pointer to record
        (&mut expected[0..6]).write(&[0x00, 0x06, 0x1F, 0x3a, 0x1F, 0x3a]).unwrap();
        (&mut expected[7994..8000]).write(&[0, 17, 116, 101, 115, 116]).unwrap();

        assert_eq!(disk_writer.block_buffer[0..6], expected[0..6]);
        assert_eq!(disk_writer.block_buffer[7993..8000], expected[7993..8000]);

        // for the filebuffer, expect that a block was written to the first 8k bytes
         assert_eq!(&disk_writer.write_buffer[0..4], &[0x07u8, 0xD2, 0x07, 0xD6][..]);
        assert_eq!(disk_writer.write_buffer[7994..8000], expected[7994..8000]);
    }

    #[test]
    fn test_write_read() {
        let output = Vec::new();

        let schema = generate_relation_schema();
        let f = Cursor::new(output);
        let mut disk_writer = DiskWriter::new(f).unwrap();

        // to_tuple
        let tuple_bytes_1 = Tuple::from_stringrecord(
            StringRecord::from(
                vec!["17", "tes"]
            ),
            &Schema{
                column_names: schema.column_names.clone(),
                column_types:schema.column_types.clone(),
            }
        ).unwrap();

        let tuple_bytes_2 = Tuple::from_stringrecord(
            StringRecord::from(
                vec!["23", "set"]
            ),
            &Schema{
                column_names: schema.column_names.clone(),
                column_types:schema.column_types.clone(),
            }
        ).unwrap();

        disk_writer.add_tuple(tuple_bytes_1.clone()).unwrap();
        disk_writer.add_tuple(tuple_bytes_2.clone()).unwrap();

        // Starting here is different from above.
        disk_writer.flush().unwrap();

        // get the underlying vec from the writer,
        // to use for read.
        let disk_file = Cursor::new(disk_writer.write_handle.into_inner());

        // Now read from the "disk file"
        let mut reader = DiskScan::new(disk_file, schema.column_types.clone()).unwrap();
        assert_eq!(
            reader.next().unwrap(),
            Tuple {
                data: vec![0, 17, 116, 101, 115, 0, 0],
                indexes: vec![0, 2],
            }
        );

        assert_eq!(
            reader.next().unwrap(),
            Tuple {
                data: vec![0, 23, 115, 101, 116, 0, 0],
                indexes: vec![0, 2],
            }
        );

        assert_eq!(reader.next(), None);
    }
}
