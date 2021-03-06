use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use csv::StringRecord;
use std::io::Cursor;
use std::ops::Index;

// TODO change Schema to ColumnTypes?
// TODO remove indexes, all info can be inferred from schema
//   once Text is allocated as fixed length
use {DataType, Schema};
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
        DataType::Text(_) => {
            let s = String::from_utf8(data.to_vec())
                .chain_err(|| "Error converting back to Utf8 for display")?;
            Ok(s.trim_right_matches('\0').to_owned())
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
        DataType::Text(x) => {
            // Requires padding 0 (null) bytes out to text alloc
            let mut bytes = s.as_bytes().to_vec();
            let padding_len = x - bytes.len(); // for now panic on underflow
            let padding = (0..padding_len).map(|_| 0);

            bytes.extend(padding);

            Ok(bytes)
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

// More final version of type conversions.
// Above will be be refactored to use the below correctly.
pub trait FromTupleField {
    fn from_tuple_field(field: &[u8]) -> Result<Self>
        where Self: Sized;
}

impl FromTupleField for u16 {
    fn from_tuple_field(field: &[u8]) -> Result<u16> {
        if field.len() != 2 {
            return Err("data has wrong number of bytes".into());
        }
        let mut rdr = Cursor::new(field);
        rdr.read_u16::<BigEndian>()
            .chain_err(|| "Error converting field")
    }
}

impl FromTupleField for u32 {
    fn from_tuple_field(field: &[u8]) -> Result<u32> {
        if field.len() != 4 {
            return Err("data has wrong number of bytes".into());
        }
        let mut rdr = Cursor::new(field);
        rdr.read_u32::<BigEndian>()
            .chain_err(|| "Error converting field")
    }
}

impl FromTupleField for f32 {
    fn from_tuple_field(field: &[u8]) -> Result<f32> {
        if field.len() != 4 {
            return Err("data has wrong number of bytes".into());
        }
        let mut rdr = Cursor::new(field);
        rdr.read_f32::<BigEndian>()
            .chain_err(|| "Error converting field")
    }
}

impl FromTupleField for String {
    fn from_tuple_field(field: &[u8]) -> Result<String> {
        String::from_utf8(field.to_vec())
            .chain_err(|| "Error converting field")
    }
}

pub fn field_parse<T: FromTupleField>(field: &[u8]) -> Result<T> {
    FromTupleField::from_tuple_field(field)
}

impl Tuple {
    pub fn get_parse<T>(&self, col: usize) -> Result<T>
        where T: FromTupleField
    {
        let field = &self[col];
        field_parse::<T>(field)
    }
}

// Into (opposite of From) TupleRecord
// Should never fail, so panic on error
pub trait ToTupleField {
    fn to_tuple_field(self) -> Vec<u8>;
}

impl ToTupleField for u16 {
    fn to_tuple_field(self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.write_u16::<BigEndian>(self).expect("Bad Convert");
        buf
    }
}

impl ToTupleField for u32 {
    fn to_tuple_field(self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.write_u32::<BigEndian>(self).expect("Bad Convert");
        buf
    }
}

impl ToTupleField for f32 {
    fn to_tuple_field(self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.write_f32::<BigEndian>(self).expect("Bad Convert");
        buf
    }
}

impl ToTupleField for String {
    fn to_tuple_field(self) -> Vec<u8> {
        self.into_bytes()
    }
}

// Tuple Append
// TODO test
impl Tuple {
    pub fn append(mut self, other: &mut Tuple) -> Tuple {
        let shift = self.data.len();
        self.data.append(&mut other.data);
        // shift indexes on second tuple
        let other_indexes = other.indexes
            .iter()
            .map(|i| {
                *i + shift
            });
        self.indexes.extend(other_indexes);
        self
    }
}

// TODO test text alloc
#[cfg(test)]
mod tests {
    use super::*;

    fn make_tuples() -> Vec<Tuple> {
        let t0 = Tuple::new(
            vec![
                b"one".to_vec(),
                vec![0u8, 2],
                b"three".to_vec(),
            ]
        );
        let t1 = Tuple::new(
            vec![
                b"four".to_vec(),
                vec![0u8, 66],
                b"six".to_vec(),
            ]
        );
        vec![t0, t1]
    }

    #[test]
    fn test_append() {
        let tuples = make_tuples();
        println!("{:?}", tuples[0]);
        println!("{:?}", tuples[1]);
        let expected = Tuple {
            data: vec![
                111,110,101,0,2,116,104,114,101,101,
                102,111,117,114,0,66,115,105,120,
            ],
            indexes: vec![
                0,3,5,10,14,16,
            ],
        };
        let t0 = tuples[0].clone();
        let mut t1 = tuples[1].clone();
        assert_eq!(expected, t0.append(&mut t1));

    }
}
