use ::DbIterator;
use ::tuple::Tuple;

pub struct Scan<I> {
    pub input: I,
}

impl <I: DbIterator> DbIterator for Scan<I> {
    fn next(&mut self) -> Option<Tuple> {
        self.input.next()
    }
}

