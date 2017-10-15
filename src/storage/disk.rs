use error::*;

/// The DiskWriter (and block manager) holds:
///    - handle to the file
///    - buffer for write io (do this myself for now, to see how it
///      works, but could use a system BufWriter)
///    - current block buffer
///    - current block attributes:
///        - next free spot for record pointer
///        - next free spot for record
pub struct DiskWriter;

pub struct DiskReader;
