### lemur db
partial toy sql db, written for bradfield databases course.

### goals

My main goal was to understand the flow of bytes from the disk through the executor. As such, in my limited time I've implemented many nodes of an executor, as well as a simplified on-disk binary representation.

### overview

- `lib.rs`, the root of the library, contains high-level types like `RelationSchema`, `DataType`, and `ColumnTypes`, as well as exposes the executor and storage modules.
- `error.rs` contains boilerplate for easy error handling.
- `executor` module contains nodes for:
  - `scan` (doesn't really do much at the moment)
  - `selection`
  - `projection`
  - `simplesort` (in-memory)
  - `nested_loops_join` (streaming)
  - `limit`
  - `aggregate`
  - `io` (used for reading directly from csv, soon to be deprecated)
- `executor` module also contains module for `tuple`:
  - tuple binary representation struct (may be modified to remove internal indexes if information can be gleaned from `ColumnTypes` being passed to getter/setter
  - implements `Index` trait for easy access to each field (and requires internal indexes)
  - implements `From` traits for many types to make it easy to and from binary representation for each `DataType`. I think it may be a useful technique for future Rust library.
  - Some number of the `From` implementations are used for the sort; todo: figure out how to cmp just the binary representations.
- `storage` module
  - convenience method to import from csv to binary disk representation
  - `DiskWriter` to write Tuples (which contain binary data) to disk format with blocks.
  - `DiskScan` to read from disk blocks into a stream of Tuples.
- binaries (for testing end-to-end):
  - `test_csv` has many commented sections, but has the basic code neede to run the executor.
  - `test_import` is the same, with the addition of an import step before using disk scan. Creates two files. Subsequent runs without import step are about 5x faster than directly from csv.

### TODO

- Rethink where DbIterator trait, storage modules, and Tuple should live in module hierarchy.
- B+Tree index
- Plan representation and compiler (and maybe optimizer)

