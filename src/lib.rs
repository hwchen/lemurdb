// TODO:
// - constrain to tuple type?
// - relation metadata
// - tuple type: should it be 

extern crate byteorder;

use byteorder::{ReadBytesExt, BigEndian};
use std::io::Cursor;
use std::ops::Index;

#[derive(Debug, Clone, PartialEq)]
struct Tuple{
    data: Vec<u8>,
    indexes: Vec<usize>,
}

impl Tuple {
    // simple init for testing purposes
    pub fn new(data: Vec<Vec<u8>>) -> Self {
        // index of next value is calculated by adding length
        // of current value. So pop last value of indexes, it's
        // the index for a value that doesn't exist
        let mut buf = vec![];
        let mut i_count = 0;
        let mut indexes = vec![i_count]; // TODO fix for empty data case
        for xs in data.iter() {
            i_count += xs.len();
            indexes.push(i_count);
            buf.extend_from_slice(xs);
        }
        let _ = indexes.pop(); // 
        Tuple {
            data: buf,
            indexes: indexes,
        }
    }
    pub fn to_string(self, schema: &Schema) -> String {
        assert_eq!(schema.column_types.len(), self.indexes.len());

        let fields = (0..self.indexes.len()).map(|i| {
            display_with_type(&self[i], &schema.column_types[i])
            }).collect::<Vec<_>>();

        fields.join(", ")
    }
}

fn display_with_type(data: &[u8], data_type: &DataType) -> String {
    match *data_type {
        DataType::Integer => {
            // read it into u32
            let mut s = String::new();
            let mut rdr = Cursor::new(data);
            let int = rdr.read_u16::<BigEndian>().unwrap();
            s.push_str(&int.to_string()[..]);
            s
        },
        DataType::Float => {
            // read it into f32
            let mut s = String::new();
            s.push_str("It's a float");
            s
        },
        DataType::Text => {
            String::from_utf8(data.to_vec()).unwrap()
        },
    }
}

impl Index<usize> for Tuple {
    type Output = [u8];

    fn index(&self, index: usize) -> &Self::Output {
        if index == self.indexes.len() - 1 {
            &self.data[self.indexes[index]..]
        } else {
            &self.data[self.indexes[index]..self.indexes[index+1]]
        }
    }
}

pub struct Schema {
    column_names: Vec<String>,
    column_types: Vec<DataType>,
}

pub enum DataType {
    Integer, //u32
    Float, //f32
    Text, //String
}

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
}

pub struct Scan<I> {
    input: I,
}

impl <I: DbIterator> DbIterator for Scan<I> {
    fn next(&mut self) -> Option<Tuple> {
        self.input.next()
    }
}

pub struct Selection<I,P> {
    input: I,
    predicate: P,
}

impl <I: DbIterator, P> DbIterator for Selection<I,P> 
    where P: FnMut(&Tuple) -> bool,
{
    fn next(&mut self) -> Option<Tuple> {
        while let Some(x) = self.input.next() {
            if (self.predicate)(&x) {
                return Some(x)
            }
        }
        None
    }
}

#[derive(Debug)]
pub struct Projection<I> {
    input: I,
    columns: Vec<usize>,
}

impl <I: DbIterator> DbIterator for Projection<I> 
    where Self: Sized,
{
    fn next(&mut self) -> Option<Tuple> {
        // TODO assert that all cols exist

        if let Some(tuple) = self.input.next() {
            let new_data: Vec<Vec<_>> = self.columns.iter().map(|i| {
                tuple[*i].to_vec() // try not to allocate?
            }).collect();
            Some(Tuple::new(new_data))
        } else {
            None
        }
    }
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
            .selection(|t| t[0][1] < 100);

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

