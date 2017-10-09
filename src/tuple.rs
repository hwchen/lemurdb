use byteorder::{ReadBytesExt, BigEndian};
use std::io::Cursor;
use std::ops::Index;

use ::{DataType, Schema};
use error::*;

#[derive(Debug, Clone, PartialEq)]
pub struct Tuple{
    data: Vec<u8>,
    indexes: Vec<usize>,
}

impl Tuple {
    // simple init for testing purposes
    pub fn new(data: Vec<Vec<u8>>) -> Self {
        // index of next value is calculated by adding length
        // of current value. So pop last value of indexes, it's
        // the index for a value that doesn't exist
        let mut buf = vec![];
        let mut i_count = 0;
        let mut indexes = vec![i_count]; // TODO fix for empty data case
        for xs in data.iter() {
            i_count += xs.len();
            indexes.push(i_count);
            buf.extend_from_slice(xs);
        }
        let _ = indexes.pop(); // 
        Tuple {
            data: buf,
            indexes: indexes,
        }
    }
    pub fn to_string(self, schema: &Schema) -> Result<String> {
        assert_eq!(schema.column_types.len(), self.indexes.len());

        let fields = (0..self.indexes.len()).map(|i| {
            display_with_type(&self[i], &schema.column_types[i])
            }).collect::<Result<Vec<_>>>()?;

        Ok(fields.join(", "))
    }
}

pub fn display_with_type(data: &[u8], data_type: &DataType) -> Result<String> {
    match *data_type {
        DataType::Integer => {
            // read it into u32
            let mut s = String::new();
            let mut rdr = Cursor::new(data);
            let int = rdr.read_u16::<BigEndian>()?;
            s.push_str(&int.to_string()[..]);
            Ok(s)
        },
        DataType::Float => {
            // read it into f32
            let mut s = String::new();
            s.push_str("It's a float");
            Ok(s)
        },
        DataType::Text => {
            String::from_utf8(data.to_vec())
                .chain_err(|| "Error converting back to Utf8 for display")
        },
    }
}

impl Index<usize> for Tuple {
    type Output = [u8];

    fn index(&self, index: usize) -> &Self::Output {
        if index == self.indexes.len() - 1 {
            &self.data[self.indexes[index]..]
        } else {
            &self.data[self.indexes[index]..self.indexes[index+1]]
        }
    }
}
