use ::{DbIterator, DataType};
use ::tuple::Tuple;

// Sort by only one column first

pub struct SimpleSort<I> {
    pub input: I,
    //pub buffer: Vec<Tuple>,
    //pub sort_on_col: usize, // currently only sort on one column
    //pub sort_on_type: DataType, // currently only sort on one column
    output: ::std::vec::IntoIter<Tuple>,
}

impl<I: DbIterator> SimpleSort<I> {
    pub fn new(
        mut input: I, // input iterator
        sort_on_col: usize, // currently only one column at a time
        sort_on_type: DataType,
        ) -> Self
    {
        let mut buf = Vec::new(); // implement iterator to make this simpler?
        while let Some(tup) = input.next() {
            buf.push(tup);
        }
        //sort

        SimpleSort {
            input: input,
            output: buf.into_iter(),
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

