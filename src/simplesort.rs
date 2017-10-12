//TODO fix the cloning of buffer
// tricky because I want to have reference to another field
// in the struct?

use std::cmp::Ordering;
use std::marker::PhantomData;

use ::{DbIterator, DataType};
use ::tuple::{Tuple, FromTupleField};

#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    Ascending,
    Descending,
}

// Sort by only one column first

#[derive(Debug, Clone)]
pub struct SimpleSort<I> {
    buffer: Vec<Tuple>,
    //sort_on_col: usize, // currently only sort on one column
    //sort_on_type: DataType, // currently only sort on one column
    output: ::std::vec::IntoIter<Tuple>,
    phantom: PhantomData<I>, //Takes an I in input but doesn't need to saveto struct
}

impl<I: DbIterator> SimpleSort<I> {
    pub fn new(
        mut input: I, // input iterator
        sort_on_col: usize, // currently only one column at a time
        sort_on_type: DataType,
        sort_order: SortOrder,
        ) -> Self
    {
        let mut buf = Vec::new(); // implement iterator to make this simpler?
        while let Some(tuple) = input.next() {
            buf.push(tuple);
        }
        //sort TODO abstract out in-memory sort for use in out-of-core sort
        // Conversion must not fail, since lib controls deserialization
        buf.sort_unstable_by(|tuple1, tuple2| {
            let field1 = &tuple1[sort_on_col];
            let field2 = &tuple2[sort_on_col];
            match sort_on_type {
                DataType::SmallInt => {
                    // read it into u16
                    let int1: u16 = FromTupleField::from_tuple_field(field1)
                        .expect("incorrect convert");
                    let int2: u16 = FromTupleField::from_tuple_field(field2)
                        .expect("incorrect convert");
                    if sort_order == SortOrder::Ascending {
                        int1.cmp(&int2)
                    } else {
                        int2.cmp(&int1)
                    }
                },
                DataType::Integer => {
                    // read it into u32
                    let int1: u32 = FromTupleField::from_tuple_field(field1)
                        .expect("incorrect convert");
                    let int2: u32 = FromTupleField::from_tuple_field(field2)
                        .expect("incorrect convert");
                    if sort_order == SortOrder::Ascending {
                        int1.cmp(&int2)
                    } else {
                        int2.cmp(&int1)
                    }
                },
                DataType::Float => {
                    // read it into f32
                    let flt1: f32 = FromTupleField::from_tuple_field(field1)
                        .expect("incorrect convert");
                    let flt2: f32 = FromTupleField::from_tuple_field(field2)
                        .expect("incorrect convert");
                    if sort_order == SortOrder::Ascending {
                        flt1.partial_cmp(&flt2).unwrap_or(Ordering::Less)
                    } else {
                        flt2.partial_cmp(&flt1).unwrap_or(Ordering::Less)
                    }
                },
                DataType::Text => {
                    let s1 = String::from_utf8(field1.to_vec()).expect("bad convert");
                    let s2 = String::from_utf8(field2.to_vec()).expect("bad convert");
                    if sort_order == SortOrder::Ascending {
                        s1.cmp(&s2)
                    } else {
                        s2.cmp(&s1)
                    }
                },
            }
        });
        SimpleSort {
            buffer: buf.clone(),
            output: buf.into_iter(),
            phantom: PhantomData,
        }
    }
}

impl<I: DbIterator> DbIterator for SimpleSort<I>
    where Self: Sized,
{
    fn next(&mut self) -> Option<Tuple> {
        self.output.next()
    }

    fn reset(& mut self) {
        self.output = self.buffer.clone().into_iter();
    }
}

