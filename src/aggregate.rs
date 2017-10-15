// Make macros!
use ::{DbIterator, DataType};
use ::tuple::{Tuple, ToTupleField};

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateType {
    Count,
    Sum,
    Avg,
    // DistinctCount,
    // Median,
}

#[derive(Debug, Clone)]
pub struct Aggregate<I> {
    // internal state
    previous_tuple: Option<Tuple>, // for groupby, tracking to allow comparing.
    is_done: bool,
    first_time: bool,
    // intitialize
    input: I,
    aggregation: AggregateType,
    agg_col: usize,
    agg_col_type: DataType,
    group_by: Option<usize>,
}

impl<I: DbIterator> Aggregate<I> {
    pub fn new(
        input: I,
        aggregation: AggregateType,
        aggregate_col: usize,
        aggregate_col_type: DataType,
        group_by: Option<usize>,
        ) -> Self
    {
        Aggregate {
            previous_tuple: None,
            is_done: false,
            first_time: true,
            input: input,
            aggregation: aggregation,
            agg_col: aggregate_col,
            agg_col_type: aggregate_col_type,
            group_by: group_by,
        }
    }
}

impl <I: DbIterator> DbIterator for Aggregate<I>
    where Self: Sized,
{
    fn next(&mut self) -> Option<Tuple> {
        if self.is_done { // is_done only needed for aggregate_all
            return None
        } else {
            if self.group_by.is_none() {
                let res = self.aggregate_all();
                self.is_done = true;
                return Some(res)
            } else {
                return self.aggregate_group();
            }
        }
    }

    fn reset(&mut self) {
        self.input.reset();
    }
}

impl<I: DbIterator> Aggregate<I> {
    fn aggregate_all(&mut self) -> Tuple {
        use AggregateType::*;
        match self.aggregation {
            Count => {
                let mut count = 0u32;
                while let Some(_) = self.input.next() {
                    count += 1;
                }
                Tuple::new(vec![count.to_tuple_field()])
            }
            Sum => {
                use DataType::*;
                match self.agg_col_type {
                    SmallInt => {
                        let mut sum = 0u16;
                        while let Some(tuple) = self.input.next() {
                            sum += tuple.get_parse::<u16>(self.agg_col)
                                .expect("internal bug on bad parse of field");
                        }
                        Tuple::new(vec![sum.to_tuple_field()])
                    },
                    Integer => {
                        let mut sum = 0u32;
                        while let Some(tuple) = self.input.next() {
                            sum += tuple.get_parse::<u32>(self.agg_col)
                                .expect("internal bug on bad parse of field");
                        }
                        Tuple::new(vec![sum.to_tuple_field()])
                    },
                    Float => {
                        let mut sum = 0f32;
                        while let Some(tuple) = self.input.next() {
                            sum += tuple.get_parse::<f32>(self.agg_col)
                                .expect("internal bug on bad parse of field");
                        }
                        Tuple::new(vec![sum.to_tuple_field()])
                    },
                    _ => {
                        panic!("No aggregation for Text");
                    },
                }
            },
            Avg => {
                use DataType::*;
                match self.agg_col_type {
                    SmallInt => {
                        let mut sum = 0u16;
                        let mut count = 0u32;
                        while let Some(tuple) = self.input.next() {
                            sum += tuple.get_parse::<u16>(self.agg_col)
                                .expect("internal bug on bad parse of field");
                            count += 1;
                        }
                        let res = sum as f32 / count as f32;
                        Tuple::new(vec![res.to_tuple_field()])
                    },
                    Integer => {
                        let mut sum = 0u32;
                        let mut count = 0u32;
                        while let Some(tuple) = self.input.next() {
                            sum += tuple.get_parse::<u32>(self.agg_col)
                                .expect("internal bug on bad parse of field");
                            count += 1;
                        }
                        let res = sum as f32 / count as f32;
                        Tuple::new(vec![res.to_tuple_field()])
                    },
                    Float => {
                        let mut sum = 0f32;
                        let mut count = 0u32;
                        while let Some(tuple) = self.input.next() {
                            sum += tuple.get_parse::<f32>(self.agg_col)
                                .expect("internal bug on bad parse of field");
                            count += 1;
                        }
                        let res = sum / count as f32;
                        Tuple::new(vec![res.to_tuple_field()])
                    },
                    _ => {
                        panic!("No aggregation for Text");
                    },
                }
            },
        }
    }

    fn aggregate_group(&mut self) -> Option<Tuple> {
        use AggregateType::*;
        match self.aggregation {
            Count => {
                let mut count = 0u32;

                // initialize the first time through
                if self.first_time {
                    self.previous_tuple = self.input.next();
                    self.first_time = false;
                }
                if self.previous_tuple.is_some() {
                    count += 1;
                }

                while let Some(tuple) = self.input.next() {
                    // previous tuple is Some, because of check from above,
                    // so it's ok to unwrap.
                    // Group is done, send back the agg
                    // At this moment, previous_tuple is the first
                    // tuple of the next group
                    if tuple[self.group_by.unwrap()] != self.previous_tuple.as_ref().unwrap()[self.group_by.unwrap()] {
                        let label = self.previous_tuple.clone().unwrap();
                        self.previous_tuple = Some(tuple);
                        return Some(Tuple::new(vec![
                            label[self.group_by.unwrap()].to_vec(),
                            count.to_tuple_field(),
                        ]));
                    } else {
                        count += 1;
                    }
                }
                None
            }
            // TODO implement group by for below this line.
            // Use macros?
//            Sum => {
//                use DataType::*;
//                match self.agg_col_type {
//                    SmallInt => {
//                        let mut sum = 0u16;
//                        while let Some(tuple) = self.input.next() {
//                            sum += tuple.get_parse(self.agg_col)
//                                .expect("internal bug on bad parse of field");
//                        }
//                        Tuple::new(vec![sum.to_tuple_field()])
//                    },
//                    Integer => {
//                        let mut sum = 0u32;
//                        while let Some(tuple) = self.input.next() {
//                            sum += tuple.get_parse(self.agg_col)
//                                .expect("internal bug on bad parse of field");
//                        }
//                        Tuple::new(vec![sum.to_tuple_field()])
//                    },
//                    Float => {
//                        let mut sum = 0f32;
//                        while let Some(tuple) = self.input.next() {
//                            sum += tuple.get_parse(self.agg_col)
//                                .expect("internal bug on bad parse of field");
//                        }
//                        Tuple::new(vec![sum.to_tuple_field()])
//                    },
//                    _ => {
//                        panic!("No aggregation for Text");
//                    },
//                }
//            },
//            Avg => {
//                use DataType::*;
//                match self.agg_col_type {
//                    SmallInt => {
//                        let mut sum = 0u16;
//                        let mut count = 0u32;
//                        while let Some(tuple) = self.input.next() {
//                            sum += tuple.get_parse(self.agg_col)
//                                .expect("internal bug on bad parse of field");
//                            count += 1;
//                        }
//                        let res = sum as f32 / count as f32;
//                        Tuple::new(vec![res.to_tuple_field()])
//                    },
//                    Integer => {
//                        let mut sum = 0u32;
//                        let mut count = 0u32;
//                        while let Some(tuple) = self.input.next() {
//                            sum += tuple.get_parse(self.agg_col)
//                                .expect("internal bug on bad parse of field");
//                            count += 1;
//                        }
//                        let res = sum as f32 / count as f32;
//                        Tuple::new(vec![res.to_tuple_field()])
//                    },
//                    Float => {
//                        let mut sum = 0f32;
//                        let mut count = 0u32;
//                        while let Some(tuple) = self.input.next() {
//                            sum += tuple.get_parse(self.agg_col)
//                                .expect("internal bug on bad parse of field");
//                            count += 1;
//                        }
//                        let res = sum / count as f32;
//                        Tuple::new(vec![res.to_tuple_field()])
//                    },
//                    _ => {
//                        panic!("No aggregation for Text");
//                    },
//                }
//            },
                _ => panic!("not yet implemented"),
        }
    }
}

