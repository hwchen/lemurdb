use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use std::io::Write;
use std::fs::File;
use std::path::Path;

// TODO: move all these to common module?
use {RelationSchema, ColumnTypes};
use executor::tuple::Tuple;
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
    schema: RelationSchema,
}

impl<W: Write> DiskWriter<W> {
    pub fn new(writer: W, schema: RelationSchema) -> Result<Self> {

        Ok(DiskWriter {
            write_handle: writer,
            write_buffer: Vec::new(),
            block_buffer: [0; 8000],
            block_upper: 4,
            block_lower: 8000,
            schema: schema,
        })
    }

    pub fn add_tuple(&mut self, tuple: Tuple) -> Result<()> {
        // when adding record:
        // - check len of tuple
        // - see if it will fit in block
        // - if yes, write to block
        // - if no, write block to file_buffer and 
        let tuple_len = tuple.data.len() as u16; // assume len less than block
        // TODO should I read from block or hold in struct?
        let free_space = self.block_lower - self.block_upper;

        // tuple len plus the one u16 pointer need to fit in block
        if tuple_len + 2 > free_space {
            self.write_buffer.extend_from_slice(&self.block_buffer);

            // block header?
            self.block_upper = 2;
            self.block_lower = 8000;
        }

        // now write to block
        let tuple_start = self.block_lower - tuple_len;

        (&mut self.block_buffer[tuple_start as usize..self.block_lower as usize]).write(&tuple.data)?;

        (&mut self.block_buffer[self.block_upper as usize..self.block_upper as usize+2])
            .write_u16::<BigEndian>(tuple_start)?;

        // increment pointers and free space pointers write to block
        self.block_upper = self.block_upper + 2;
        self.block_lower = tuple_start;
        (&mut self.block_buffer[0..2]).write_u16::<BigEndian>(self.block_upper);
        (&mut self.block_buffer[2..4]).write_u16::<BigEndian>(self.block_lower);

        Ok(())
    }

    pub fn flush() -> Result<()> {
        // for guaranteeing that all blocks will be written to disk
        // Writes current block to file_buffer, write file_buffer to disk
        Ok(())
    }
}

pub struct DiskReader;

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
            column_types: vec![DataType::SmallInt, DataType::Text],
        }
    }

    #[test]
    fn test_block_buffer_one_write() {
        let schema = generate_relation_schema();
        let f = Cursor::new(Vec::new());
        let mut disk_writer = DiskWriter::new(f, schema.clone()).unwrap();

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
        (&mut expected[7993..8000]).write(&[0, 17, 116, 101, 115, 116, 121, 0, 17, 116, 101, 115, 116, 121]).unwrap();

        assert_eq!(disk_writer.block_buffer[0..6], expected[0..6]);
        assert_eq!(disk_writer.block_buffer[7993..8000], expected[7993..8000]);
    }

    #[test]
    fn test_block_buffer_overflow() {
        let schema = generate_relation_schema();
        let f = Cursor::new(Vec::new());
        let mut disk_writer = DiskWriter::new(f, schema.clone()).unwrap();

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
        disk_writer.add_tuple(tuple_bytes).unwrap();

        let mut expected = [0;8000];
        // header of 4 bytes (two pointers to free space), plus one pointer to record
        (&mut expected[0..6]).write(&[0x00, 0x06, 0x1F, 0x39, 0x1F, 0x39]).unwrap();
        (&mut expected[7993..8000]).write(&[0, 17, 116, 101, 115, 116, 121]).unwrap();

        assert_eq!(disk_writer.block_buffer[0..6], expected[0..6]);
        assert_eq!(disk_writer.block_buffer[7993..8000], expected[7993..8000]);
    }

    #[test]
    fn test_write_io() {
    }
}
