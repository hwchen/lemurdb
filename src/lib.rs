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
pub mod storage;


// TODO this will be deprecated
#[derive(Debug, Clone)]
pub struct Schema {
    pub column_names: Vec<String>,
    pub column_types: ColumnTypes,
}

pub struct RelationSchema {
    pub name: String,
    pub id: u32,
    pub column_names: Vec<String>,
    pub column_types: ColumnTypes,
}

type ColumnTypes = Vec<DataType>;

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    SmallInt, //u16
    Integer, //u32
    Float, //f32
    Text, //String
}

