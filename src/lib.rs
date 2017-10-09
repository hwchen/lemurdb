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
pub mod io;
pub mod projection;
pub mod scan;
pub mod selection;
pub mod simplesort;
pub mod tuple;

use error::*;
use projection::Projection;
use scan::Scan;
use selection::Selection;
use simplesort::SimpleSort;
use tuple::Tuple;

pub struct Schema {
    column_names: Vec<String>,
    column_types: Vec<DataType>,
}

pub enum DataType {
    Integer, //u32
    Float, //f32
    Text, //String
}

// The Executor

trait DbIterator {
    fn next(&mut self) -> Option<Tuple>;

    fn scan(self) -> Scan<Self> where Self: Sized {
        Scan {input: self}
    }

    fn selection<P>(self, predicate: P) -> Selection<Self, P>
        where Self: Sized, P: FnMut(&Tuple) -> bool,
    {
        Selection {input: self, predicate: predicate}
    }

    fn projection(self, columns: Vec<usize>) -> Projection<Self>
        where Self: Sized,
    {
        Projection {input: self, columns: columns}
    }

//    fn simplesort(self, column:usize) -> SimpleSort<Self>
//        where Self: Sized,
//    {
//        // sort here, on initialization.
//        //
//        Sort {input: self, columns: columns}
//    }
}

#[derive(Debug)]
pub struct TestSource {
    source: Vec<Tuple>,
    i: usize,
}

impl DbIterator for TestSource {
    fn next(&mut self) -> Option<Tuple> {
        if self.i < self.source.len() {
            let res = self.source[self.i].clone();
            self.i += 1;
            Some(res)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tuples() -> Vec<Tuple> {
        let t0 = Tuple::new(
            vec![
                b"one".to_vec(),
                vec![0u8, 2],
                b"three".to_vec(),
            ]
        );
        let t1 = Tuple::new(
            vec![
                b"four".to_vec(),
                vec![0u8, 66],
                b"six".to_vec(),
            ]
        );
        let t2 = Tuple::new(
            vec![
                b"ten".to_vec(),
                vec![0u8, 222],
                b"twelve".to_vec(),
            ]
        );
        vec![t0, t1, t2]
    }

    #[test]
    fn test_tuples() {
        let tuples = make_tuples();
        println!("{:?}", tuples[0]);
        println!("{:?}", tuples[1]);
        println!("{:?}", tuples[2]);
    }

    #[test]
    fn test_scan() {
        let tuples = make_tuples();
        let test_source = TestSource {
            source: tuples.clone(),
            i: 0,
        };
        let mut scan_iter = test_source.scan();
        assert_eq!(scan_iter.next(), Some(tuples[0].clone()));
        assert_eq!(scan_iter.next(), Some(tuples[1].clone()));
        assert_eq!(scan_iter.next(), Some(tuples[2].clone()));
        assert_eq!(scan_iter.next(), None);
    }

    #[test]
    fn test_selection() {
        let tuples = make_tuples();
        let test_source = TestSource {
            source: tuples.clone(),
            i: 0,
        };
        let mut selection_iter = test_source.selection(
            |t| {
                t.data.len() < 10 // fake, without using api for tuple
            }
        );
        assert_eq!(selection_iter.next(), Some(tuples[1].clone()));
        assert_eq!(selection_iter.next(), None);
    }

    #[test]
    fn test_projection() {
        let tuples = make_tuples();
        let test_source = TestSource {
            source: tuples.clone(),
            i: 0,
        };
        let mut projection_iter = test_source.projection(vec![1]);
        let r0 = Tuple::new(
            vec![
                vec![0u8, 2],
            ]
        );
        let r1 = Tuple::new(
            vec![
                vec![0u8, 66],
            ]
        );
        let r2 = Tuple::new(
            vec![
                vec![0u8, 222],
            ]
        );
        let res_tuples = vec![r0, r1, r2];
        assert_eq!(projection_iter.next(), Some(res_tuples[0].clone()));
        assert_eq!(projection_iter.next(), Some(res_tuples[1].clone()));
        assert_eq!(projection_iter.next(), Some(res_tuples[2].clone()));
        assert_eq!(projection_iter.next(), None);
    }

    #[test]
    fn test_projection_selection() {
        let tuples = make_tuples();
        let test_source = TestSource {
            source: tuples.clone(),
            i: 0,
        };
        let mut query = test_source
            .projection(vec![1])
            .selection(|t| t[0][1] < 100); // will need to translate according to type

        let r0 = Tuple::new(
            vec![
                vec![0u8, 2],
            ]
        );
        let r1 = Tuple::new(
            vec![
                vec![0u8, 66],
            ]
        );
        let res_tuples = vec![r0, r1];
        assert_eq!(query.next(), Some(res_tuples[0].clone()));
        assert_eq!(query.next(), Some(res_tuples[1].clone()));
        assert_eq!(query.next(), None);
    }

    #[test]
    fn test_display_with_type() {
        use DataType::*;

        let schema = Schema {
            column_names: vec![
                "test".to_owned(),
                "number_test".to_owned(),
                "testy".to_owned(),
            ],
            column_types: vec![Text, Integer, Text],
        };
        let tuples = make_tuples();
        assert_eq!(
            format!("{}", tuples[0].clone().to_string(&schema)),
            "one, 2, three"
        );
    }
}

