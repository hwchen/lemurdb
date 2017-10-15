use std::fs::File;
use std::path::Path;

use {RelationSchema, ColumnTypes};
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
pub struct DiskWriter {
    file_handle: File,
    file_buffer: Vec<u8>, // holds bytes to append to file on disk
    block_buffer: [u8; 8000], // holds current block being written to
    block_upper: u16, // pointer to beginning of free space
    block_lower: u16, // pointer to end of free space
    schema: RelationSchema,
}

impl DiskWriter {
    pub fn new(relation_path: &Path, schema: RelationSchema) -> Result<Self> {
        let f = File::create(relation_path)?;

        Ok(DiskWriter {
            file_handle: f,
            file_buffer: Vec::new(),
            block_buffer: [0; 8000],
            block_upper: 0,
            block_lower: 8000,
            schema: schema,
        })
    }

    pub fn add_record() -> Result<()> {
        Ok(())
    }

    pub fn flush() -> Result<()> {
        Ok(())
    }
}

pub struct DiskReader;
