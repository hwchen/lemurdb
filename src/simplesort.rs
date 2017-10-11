use byteorder::{ReadBytesExt, BigEndian};
use std::cmp::Ordering;
use std::io::Cursor;
use std::marker::PhantomData;

use ::{DbIterator, DataType};
use ::tuple::Tuple;

// Sort by only one column first

pub struct SimpleSort<I> {
    //buffer: Vec<Tuple>,
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
                    let mut rdr = Cursor::new(field1);
                    let int1 = rdr.read_u16::<BigEndian>().expect("incorrect convert");
                    let mut rdr = Cursor::new(field2);
                    let int2 = rdr.read_u16::<BigEndian>().expect("incorrect convert");
                    int1.cmp(&int2)
                },
                DataType::Integer => {
                    // read it into u32
                    let mut rdr = Cursor::new(field1);
                    let int1 = rdr.read_u32::<BigEndian>().expect("incorrect convert");
                    let mut rdr = Cursor::new(field2);
                    let int2 = rdr.read_u32::<BigEndian>().expect("incorrect convert");
                    int1.cmp(&int2)
                },
                DataType::Float => {
                    // read it into f32
                    let mut rdr = Cursor::new(field1);
                    let flt1 = rdr.read_f32::<BigEndian>().expect("incorrect convert");
                    let mut rdr = Cursor::new(field2);
                    let flt2 = rdr.read_f32::<BigEndian>().expect("incorrect convert");
                    flt1.partial_cmp(&flt2).unwrap_or(Ordering::Less)
                },
                DataType::Text => {
                    let s1 = String::from_utf8(field1.to_vec()).expect("bad convert");
                    let s2 = String::from_utf8(field2.to_vec()).expect("bad convert");
                    s1.cmp(&s2)
                },
            }
        });
        SimpleSort {
            output: buf.into_iter(),
            phantom: PhantomData,
        }
    }
}

impl <I: DbIterator> DbIterator for SimpleSort<I>
    where Self: Sized,
{
    fn next(&mut self) -> Option<Tuple> {
        self.output.next()
    }
}

