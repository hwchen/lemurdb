use ::DbIterator;
use ::tuple::Tuple;

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateType {
    Count,
    //Sum,
    //Avg,
    // DistinctCount,
    // Median,
}

#[derive(Debug, Clone)]
pub struct Aggregate<I> {
    // internal state
    buffer: Vec<Tuple>
    group_count: usize,
    // intitialize
    input: I,
    aggregation: AggregateType,
    group_by: Option<usize>,
}

impl <I: DbIterator> DbIterator for Aggregate<I>
    where Self: Sized,
{
    fn next(&mut self) -> Option<Tuple> {
        None
//        if self.group_by.is_none() {
//            use Aggregate_Type::*;
//            match aggregation {
//                Count => {
//                    let mut count = 0;
//
//                    // TODO how does parse work, so that it
//                    // lets you specify the type of the output?
//                },
//                Sum => {
//                },
//                Avg => {
//                },
//            }
//        }
//        while
    }
}

