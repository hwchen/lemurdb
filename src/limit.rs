use ::DbIterator;
use ::tuple::Tuple;

pub struct Limit<I> {
    pub count: usize,
    pub limit: usize,
    pub input: I,
}

impl <I: DbIterator> DbIterator for Limit<I>
    where Self: Sized,
{
    fn next(&mut self) -> Option<Tuple> {
        if self.count < self.limit {
            self.count += 1;
            self.input.next()
        } else {
            None
        }
    }
}

