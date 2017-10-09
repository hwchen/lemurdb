use ::DbIterator;
use ::tuple::Tuple;

// Sort by only one column first

pub struct SimpleSort<I> {
    pub input: I,
    pub column: usize,
}

impl <I: DbIterator> DbIterator for SimpleSort<I>
    where Self: Sized,
{
    fn next(&mut self) -> Option<Tuple> {
        // assert that col exists?

        // TODO this isn't sorting yet...
//        if let Some(tuple) = self.input.next() {
//            let new_data: Vec<Vec<_>> = self.columns.iter().map(|i| {
//                tuple[*i].to_vec() // try not to allocate?
//            }).collect();
//            Some(Tuple::new(new_data))
//        } else {
//            None
//        }
        None
    }
}

