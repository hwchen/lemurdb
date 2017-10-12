// TODO use predicate instead of equijoin
// This is an inner equijoin only for now.
use ::{DbIterator};
use ::tuple::{Tuple};

#[derive(Debug, Clone)]
pub struct NestedLoopsJoin<I> {
    // intitialize
    input_l: I,
    input_r: I,
    current_l: Option<Tuple>,
    col_l: usize,
    col_r: usize,
}

impl<I: DbIterator> NestedLoopsJoin<I> {
    pub fn new(
        mut input_l: I,
        input_r: I,
        col_l: usize,
        col_r: usize,
        ) -> Self
    {
        let current_l = input_l.next();

        NestedLoopsJoin {
            input_l: input_l,
            input_r: input_r,
            current_l: current_l,
            col_l: col_l,
            col_r: col_r,
        }
    }
}

impl <I: DbIterator> DbIterator for NestedLoopsJoin<I>
    where Self: Sized,
{
    fn next(&mut self) -> Option<Tuple> {
        while let Some(mut tuple_r) = self.input_r.next() {
            let current_l = self.current_l.clone().unwrap();
            if current_l[self.col_l] == tuple_r[self.col_r] {
                return Some(current_l.append(&mut tuple_r));
            }
        }
        self.input_r.reset();
        self.current_l = self.input_l.next();
        if self.current_l.is_none() {
            return None
        } else {
            self.next()
        }
    }

    fn reset(&mut self) {
        self.input_l.reset();
        self.input_r.reset();
    }
}
