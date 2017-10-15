use super::DbIterator;
use super::tuple::Tuple;

#[derive(Debug, Clone)]
pub struct Projection<I> {
    pub input: I,
    pub columns: Vec<usize>,
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

    fn reset(&mut self) {
        self.input.reset();
    }
}


