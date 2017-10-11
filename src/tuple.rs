use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use csv::StringRecord;
use std::io::Cursor;
use std::ops::Index;

use ::{DataType, Schema};
use error::*;

#[derive(Debug, Clone, PartialEq)]
pub struct Tuple{
    pub data: Vec<u8>,
    pub indexes: Vec<usize>, // pointers to start of data for a field
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
        DataType::SmallInt => {
            // read it into u16
            let mut s = String::new();
            let mut rdr = Cursor::new(data);
            let int = rdr.read_u16::<BigEndian>()?;
            s.push_str(&int.to_string()[..]);
            Ok(s)
        },
        DataType::Integer => {
            // read it into u32
            let mut s = String::new();
            let mut rdr = Cursor::new(data);
            let int = rdr.read_u32::<BigEndian>()?;
            s.push_str(&int.to_string()[..]);
            Ok(s)
        },
        DataType::Float => {
            // read it into f32
            let mut s = String::new();
            let mut rdr = Cursor::new(data);
            let float = rdr.read_f32::<BigEndian>()?;
            s.push_str(&float.to_string()[..]);
            Ok(s)
        },
        DataType::Text => {
            String::from_utf8(data.to_vec())
                .chain_err(|| "Error converting back to Utf8 for display")
        },
    }
}

pub fn string_to_binary(s: &str, data_type: &DataType) -> Result<Vec<u8>> {
    match *data_type {
        DataType::SmallInt => {
            //TODO support other radix
            let integer = s.parse::<u16>()?;
            let mut buf = Vec::new();
            buf.write_u16::<BigEndian>(integer)?;
            Ok(buf)
        },
        DataType::Integer => {
            //TODO support other radix
            let integer = s.parse::<u32>()?;
            let mut buf = Vec::new();
            buf.write_u32::<BigEndian>(integer)?;
            Ok(buf)
        },
        DataType::Float => {
            let float = s.parse::<f32>()?;
            let mut buf = Vec::new();
            buf.write_f32::<BigEndian>(float)?;
            Ok(buf)
        },
        DataType::Text => {
            Ok(s.as_bytes().to_vec())
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

impl Tuple {
    pub fn from_stringrecord(record: StringRecord, schema: &Schema) -> Result<Self> {
        let mut indexes = Vec::new();
        let mut data = Vec::new();
        for col_idx in 0..record.len() {
            // Get the pointer to the start of next field
            indexes.push(data.len());

            // Now convert based on Schema
            let mut field_data = string_to_binary(
                &record[col_idx],
                &schema.column_types[col_idx]
            )?;
            data.append(&mut field_data);

        }

        Ok(Tuple {
            data: data,
            indexes: indexes,
        })
    }
}

