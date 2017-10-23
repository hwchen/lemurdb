pub mod aggregate;
pub mod io;
pub mod limit;
pub mod nested_loops_join;
pub mod projection;
pub mod scan;
pub mod selection;
pub mod simplesort;
pub mod tuple;

use DataType;
use self::aggregate::{Aggregate, AggregateType};
use self::limit::Limit;
use self::nested_loops_join::NestedLoopsJoin;
use self::projection::Projection;
use self::scan::Scan;
use self::selection::Selection;
use self::simplesort::{SimpleSort, SortOrder};
use self::tuple::Tuple;

// The Executor

pub trait DbIterator {
    fn next(&mut self) -> Option<Tuple>;

    fn reset(&mut self);

    fn scan(self) -> Scan<Self> where Self: Sized {
        Scan {input: self}
    }

    fn limit(self, limit: usize) -> Limit<Self>
        where Self: Sized,
    {
        Limit {input: self, limit: limit, count: 0,}
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

    fn simplesort(
        self,
        sort_on_col:usize,
        sort_on_type: DataType,
        sort_order: SortOrder,
    ) -> SimpleSort<Self>
        where Self: Sized,
    {
        SimpleSort::new(
            self,
            sort_on_col,
            sort_on_type,
            sort_order,
        )
    }

    fn aggregate(
        self,
        aggregation: AggregateType,
        aggregate_col: usize,
        aggregate_col_type: DataType,
        group_by: Option<usize>,
    ) -> Aggregate<Self>
        where Self: Sized
    {
        Aggregate::new(
            self,
            aggregation,
            aggregate_col,
            aggregate_col_type,
            group_by,
        )
    }

    fn nested_loops_join(
        self,
        other: Self,
        col_l: usize,
        col_r: usize,
    ) -> NestedLoopsJoin<Self>
        where Self: Sized
    {
        NestedLoopsJoin::new(
            self,
            other,
            col_l,
            col_r,
        )
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

    fn reset(&mut self) {
        self.i = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Schema;

    //use csv;

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
            column_types: vec![Text(255), SmallInt, Text(255)],
        };
        let tuples = make_tuples();
        assert_eq!(
            format!("{}", tuples[0].clone().to_string(&schema).unwrap()),
            "one, 2, three"
        );
    }
//    #[test]
//    #[ignore] // TODO figure out a better way to test csv if not from file
//    fn test_csv_to_tuple() {
//        use DataType::*;
//        use ::std::io::Read;
//
//        let schema = Schema {
//            column_names: vec![
//                "name".to_owned(),
//                "test_id".to_owned(),
//                "description".to_owned(),
//            ],
//            column_types: vec![Text, SmallInt, Text, Float],
//        };
//
//        let csv =
//        b"name,test_id,description, rating\n\
//        robin,1,student,25.4\n\
//        sam,22,employee,12.12\n\
//        ";
//        let rdr = ::std::io::Cursor::new(&csv[..]);
//
//        let mut query = io::CsvSource::new(rdr, schema.clone());
//
//        assert_eq!(
//            format!("{}", query.next().unwrap().clone().to_string(&schema).unwrap()),
//            "robin, 1, student, 25.4"
//        );
//
//        assert_eq!(
//            format!("{}", query.next().unwrap().clone().to_string(&schema).unwrap()),
//            "sam, 22, employee, 12.12"
//        );
//    }
}

