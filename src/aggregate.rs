use byteorder::{WriteBytesExt, BigEndian};

use ::{DbIterator, DataType};
use ::tuple::{Tuple, FromTupleField, ToTupleField};

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateType {
    Count,
    Sum,
    //Avg,
    // DistinctCount,
    // Median,
}

#[derive(Debug, Clone)]
pub struct Aggregate<I> {
    // internal state
    buffer: Vec<Tuple>,
    is_done: bool,
    // intitialize
    input: I,
    aggregation: AggregateType,
    aggregate_col: usize,
    aggregate_col_type: DataType,
    group_by: Option<usize>,
}

impl<I: DbIterator> Aggregate<I> {
    pub fn new(
        input: I,
        aggregation: AggregateType,
        aggregate_col: usize,
        aggregate_col_type: DataType,
        group_by: Option<usize>,
        ) -> Self
    {
        Aggregate {
            buffer: Vec::new(),
            is_done: false,
            input: input,
            aggregation: aggregation,
            aggregate_col: aggregate_col,
            aggregate_col_type: aggregate_col_type,
            group_by: group_by,
        }
    }
}

impl <I: DbIterator> DbIterator for Aggregate<I>
    where Self: Sized,
{
    fn next(&mut self) -> Option<Tuple> {
        if self.is_done {
            return None
        } else {
            if self.group_by.is_none() {
                let res = self.aggregate_all();
                self.is_done = true;
                return Some(res)
            } else {
                return None;  // TODO fill in group_by
            }
        }
    }
}

impl<I: DbIterator> Aggregate<I> {
    fn aggregate_all(&mut self) -> Tuple {
        use AggregateType::*;
        match self.aggregation {
            Count => {
                let mut count = 0u32;
                while let Some(_) = self.input.next() {
                    count += 1;
                }
                Tuple::new(vec![count.to_tuple_field()])
            }
            Sum => {
                use DataType::*;
                match self.aggregate_col_type {
                    SmallInt => {
                        let mut count = 0u16;
                        while let Some(tuple) = self.input.next() {
                            count += tuple.get_parse(self.aggregate_col)
                                .expect("internal bug on bad parse of field");
                        }
                        Tuple::new(vec![count.to_tuple_field()])
                    },
                    Integer => {
                        let mut count = 0u32;
                        while let Some(tuple) = self.input.next() {
                            count += tuple.get_parse(self.aggregate_col)
                                .expect("internal bug on bad parse of field");
                        }
                        Tuple::new(vec![count.to_tuple_field()])
                    },
                    Float => {
                        let mut count = 0f32;
                        while let Some(tuple) = self.input.next() {
                            count += tuple.get_parse(self.aggregate_col)
                                .expect("internal bug on bad parse of field");
                        }
                        Tuple::new(vec![count.to_tuple_field()])
                    },
                    _ => {
                        panic!("No aggregation for Text");
                    },
                }
            },
        }
    }
}

