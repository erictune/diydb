//! provides access to table records in a typed form.
//! Table records in SQLite are stored as bytes, with a serial_type code, and need to be converted
//! to Rust types before they can be used in expressions.
//! Some conversions are fallible.  Any failure to convert any element in a row is treated as a failure to convert the entire row.
//! Null values are not a conversion failure.
//! Any value could possibly be Null (NOT NULL is not handled here.)
//! Any io errors reading the row are also treated as failures to read the entire row.
//! An error returned by the underlying table iterator (such as an error in the btree structure)
//! will manifest as one row having an error, fewer rows being returned.
// TODO: It might be better to treat casting errors differently from errors in the underlying iterator.
use crate::sql_type::SqlType;
use crate::sql_value::SqlValue;
#[allow(unused_imports)] // Needed fot trait FromStr
use std::str::FromStr;

/// can hold a sequence of values of any of the SQL types (sqlite supported subset), along with a rowid.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub items: Vec<SqlValue>,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Deserialization error, column number {}, detail : {}", colnum, detail)]
    Deserialization{
        detail: crate::serial_type::Error,
        colnum: usize,
    },
    #[error("Casting error, column number {}, detail : {}", colnum, detail)]
    Casting{
        detail: crate::serial_type::Error,
        colnum: usize
    },
    #[error("Type array and value array length mismatch.")]
    ArrayLenMismatch,
}

pub fn from_serialized(column_types: &Vec<SqlType>, record: &[u8]) -> Result<Row, Error> {
    use crate::record::ValueIterator;
    let mut ret: Vec<SqlValue> = vec![];
    for (colnum, (serty, bytes)) in ValueIterator::new(record).enumerate() {
        if colnum > column_types.len() {
            return Err(Error::ArrayLenMismatch);
        }
        let v = crate::serial_type::to_sql_value(
            &serty,
            bytes
        ).map_err(|detail| Error::Deserialization{ colnum, detail })?;
        let v = crate::serial_type::cast_to_schema_type(
            &v, column_types[colnum]
        ).map_err(|detail| Error::Casting{colnum, detail})?;  
        ret.push(v);
    }
    Ok(Row {
        items: ret.to_vec(),
    })
}

#[test]
fn test_from_serialized() {
    use SqlValue::*;
    // literal 0 | literal 1 | float 3.1415 | "Ten" | NULL
    let test_record: &[u8] = &[
        0x06, 0x08, 0x09, 0x07, 0x13, 0x00, 0x40, 0x09, 0x21, 0xca, 0xc0, 0x83, 0x12, 0x6f, 0x54,
        0x65, 0x6e,
    ];
    let column_types: Vec<SqlType> = vec![
        SqlType::Int,
        SqlType::Int,
        SqlType::Real,
        SqlType::Text,
        SqlType::Int,
    ];
    let tr = from_serialized(&column_types, &test_record).unwrap();
    assert_eq!(tr.items.len(), 5);
    assert_eq!(tr.items[0], Int(0));
    assert_eq!(tr.items[1], Int(1));
    assert_eq!(tr.items[2], Real(3.1415));
    assert_eq!(tr.items[3], Text(String::from("Ten")));
    assert_eq!(tr.items[4], Null());
}