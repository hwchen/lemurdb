#![recursion_limit = "1024"]
// TODO:
// - constrain to tuple type?
// - relation metadata
// - figure out how to handle tuples
//
// - do a filescan
// - next up: sort, distinct, aggregations

extern crate byteorder;
extern crate csv;
#[macro_use]
extern crate error_chain;

pub mod error;
pub mod executor;
