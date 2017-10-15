use super::DbIterator;
use super::tuple::Tuple;

#[derive(Debug, Clone)]
pub struct Selection<I,P> {
    pub input: I,
    pub predicate: P,
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

    fn reset(&mut self) {
        self.input.reset();
    }
}

