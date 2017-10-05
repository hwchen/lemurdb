trait DbIterator {
    type Item;

    fn next(&mut self) -> Option<Self::Item>;

    fn scan(self) -> Scan<Self> where Self: Sized {
        Scan {input: self}
    }

    fn selection<P>(self, predicate: P) -> Selection<Self, P>
        where Self: Sized, P: FnMut(&Self::Item) -> bool,
    {
        Selection {input: self, predicate: predicate}
    }
}

pub struct Scan<I> {
    input: I,
}

impl <I: DbIterator> DbIterator for Scan<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.input.next()
    }
}

pub struct Selection<I,P> {
    input: I,
    predicate: P,
}

impl <I: DbIterator, P> DbIterator for Selection<I,P> 
    where P: FnMut(&I::Item) -> bool,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(x) = self.input.next() {
            if (self.predicate)(&x) {
                return Some(x)
            }
        }
        None
    }
}

pub struct TestSource<T> {
    source: Vec<T>,
    i: usize,
}

impl<T: Clone> DbIterator for TestSource<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
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

    #[test]
    fn test_scan() {
        let test_source = TestSource {
            source: vec!["one", "two", "three"],
            i: 0,
        };
        let mut scan_iter = test_source.scan();
        assert_eq!(scan_iter.next(), Some("one"));
        assert_eq!(scan_iter.next(), Some("two"));
        assert_eq!(scan_iter.next(), Some("three"));
        assert_eq!(scan_iter.next(), None);
    }

    #[test]
    fn test_selection() {
        let test_source = TestSource {
            source: vec!["one", "two", "three"],
            i: 0,
        };
        let mut selection_iter = test_source.selection(
            |s| {
                s.len() < 4
            }
        );
        assert_eq!(selection_iter.next(), Some("one"));
        assert_eq!(selection_iter.next(), Some("two"));
        assert_eq!(selection_iter.next(), None);
    }
}

//pub struct RelationMetadata {
//    num_fields: usize,
//    field_types: Vec<DataType>,
//}
//
//pub struct RowHeader {
//    row_len: usize,
//    //null_bitmap: Option<Vec<u8>>, // change to bit vector later
//}
//
//pub enum DataType {
//    Integer, //u64
//    Float, //f32
//    Text, //String (unicode)
//}
//
//pub struct DbCursor {
//    r_meta: RelationMetadata,
//    buffer: Vec<u8>,
//}


