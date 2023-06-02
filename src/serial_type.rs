//! Serial Types are how SQLite stores values in storage.
use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use std::io::Read;

use crate::sql_type::SqlType;
use crate::sql_value::SqlValue;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Pager: Error accessing database file: {0}")]
    Io(#[from] std::io::Error),
    #[error("Unable to convert type {from} to {to}.")]
    Type { from: SqlType, to: SqlType },
    #[error("Unimplemented type.")]
    Unimplemented,
    #[error("Invalid serial type code.")]
    InvalidSerialTypeCode,
    #[error("Byte were not a valid string valid encoding.")]
    InvalidStringEncoding(#[from] std::string::FromUtf8Error),
    #[error("Null found where non-null value required.")]
    Null,
    #[error("Code which was thought unreachable was reached.")]
    Unreachable,
    #[error("Input value's type is not a valid storage class type.")]
    NotStorageClassType
}

/// Convert a serial type number to a string describing the type suitable for debug printing.
///
/// # Arguments
///
/// * `serial_type` - A SQLite serial type code.
///
/// These are not SQL type, but informal names for debugging.
///
/// # Panics
///
/// Does not panic
#[cfg(debug)]
pub fn typecode_to_string(serial_type: i64) -> &'static str {
    match serial_type {
        // From: https://www.sqlite.org/fileformat.html#record_format
        // Serial Type	Content Size	Meaning
        // 0	0	Value is a NULL.s
        0 => "st:null",
        // 1	1	Value is an 8-bit twos-complement integer.
        1 => "st:int1B",
        // 2	2	Value is a big-endian 16-bit twos-complement integer.
        2 => "st:int2B",
        // 3	3	Value is a big-endian 24-bit twos-complement integer.
        3 => "st:int3B",
        // 4	4	Value is a big-endian 32-bit twos-complement integer.
        4 => "st:int4B",
        // 5	6	Value is a big-endian 48-bit twos-complement integer.
        5 => "st:int6B",
        // 6	8	Value is a big-endian 64-bit twos-complement integer.
        6 => "st:int8B",
        // 7	8	Value is a big-endian IEEE 754-2008 64-bit floating point number.
        7 => "st:float8B",
        // 8	0	Value is the integer 0. (Only available for schema format 4 and higher.)
        8 => "st:zero",
        // 9	0	Value is the integer 1. (Only available for schema format 4 and higher.)
        9 => "st:one",
        // 10,11	variable	Reserved for internal use. These serial type codes will never appear in a well-formed database file, but they might be used in transient and temporary database files that SQLite sometimes generates for its own use. The meanings of these codes can shift from one release of SQLite to the next.
        10 => "st:internal_10",
        11 => "st:internal_11",
        // N≥12 and even	(N-12)/2	Value is a BLOB that is (N-12)/2 bytes in length.
        // N≥13 and odd	(N-13)/2	Value is a string in the text encoding and (N-13)/2 bytes in length. The nul terminator is not stored.
        x @ 12.. => {
            if x % 2 == 0 {
                "st:blob"
            } else {
                "st:text"
            }
        }
        i64::MIN..=-1 => "st:error_negative",
    }
}


/// Convert a sqlite value in "serial type enum" format into an enum that can hold any value (`SqlValue`).
/// Returns an Error if there is a problem reading from the data.
///
///  # Arguments
/// * `serial_type` - A SQLite serial type code.
/// * `data` - A slice of bytes.
///
/// A NULL type code (0) results in `SqlValue::Null()`
///
/// # Comparison to SQLite
///
/// SQLite has "Storage Classes" [https://www.sqlite.org/datatype3.html#storage_classes_and_datatypes]
/// NULL, REAL, INTEGER, TEXT, BLOB, and each value is stored as one of those classes.
///
/// SQLite Columns have SQL type affinities, which are one of:
/// TEXT, NUMERIC, INTEGER, REAL, BLOB
/// We also support these types for columns, except NUMERIC which is wierd and we won't support that type name in the grammar
/// and so not in converstion either.
///
/// SQLites rules for when to convert from a Storage class (serial type) to the type affinity are complex.
///
/// We implement automatic conversion rules that allows common, recently-generated SQLite-generated tables to be read.
/// At the same time, it avoids precision-loosing kr surprising conversions, and does not attempt to provide exact compatibility
/// with SQLite.
///
/// The following table shows what happens if a Storage Class (StoCl) is *read* from a SQLite file, and conversion is requested
/// to the SQL type (SqlTy).   The Ok Return Type column is written assuming that `use sql_value::SqlValue::*;` and
/// `use diydb::serial_type::Error::*;`
///
/// | StoCl | SqlTy | Return type enum variant | comments |
/// | ----- | ----- | -------- | - |
/// | NULL  | *     | Ok(Null) | |
/// | REAL  | REAL  | Ok(Real) | |
/// | REAL  | INT   | Err      | we don't support this to avoid silent loss of precision |
/// | REAL  | TEXT  | Err      | |
/// | REAL  | BLOB  | Err      | |
/// | INT   | REAL  | Ok(Int)  | necessary since SQLite stores 2.0 as Integer(2). |
/// | INT   | INT   | Ok(Int)  | Smaller types are sign extended to i64. |
/// | INT   | TEXT  | Ok(Text) | necessary since SQLite stores "2" as Integer(2), etc. |
/// | INT   | BLOB  | Err      | |
/// | TEXT  | REAL  | Err      | |
/// | TEXT  | TEXT  | Ok(Text) | |
/// | TEXT  | INT   | Err      | |
/// | TEXT  | BLOB  | Err      | |
/// | BLOB  | REAL  | Err      | |
/// | BLOB  | INT   | Err      | |
/// | BLOB  | TEXT  | Err      | |
/// | BLOB  | BLOB  | Ok(Blob) | |
///
/// When we get to writes, we may need a new conversion table.
///
/// # Panics
///
/// Does not panic.
pub fn value_to_sql_typed_value(
    serial_type: &i64,
    sql_type: SqlType,
    data: &[u8],
) -> Result<SqlValue, Error> {
    cast_to_schema_type(to_sql_value(serial_type, data)?, sql_type)    
}

/// Deserialize bytes in "SQLIte serial type" format into one of a few native types (`SqlValue`).
/// 
/// Returns an Error if there is a problem reading from the data.
///
///  # Arguments
/// * `serial_type` - A SQLite serial type code applying to `data`
/// * `data` - A slice of bytes.
///
/// The possible types produced are:
/// - SqlValue::Null
/// - SqlValue::Int
/// - SqlValue::Real
/// - SqlValue::Text
/// - SqlValue::Blob.
/// 
/// These types are correspond to what SQLite calls "Storage Classes" [https://www.sqlite.org/datatype3.html#storage_classes_and_datatypes]
///
/// This function is unaware of what the "schema type" is of the row which the stored value represents.
/// Thus, it may be necessary later to convert SqlValue::Int(0) to SqlValue::Bool(true) or SqlValue::Real(0.0), etc.
///
/// SQLites rules for when to convert from a Storage class (serial type) to the type affinity of the column are complex, and not
/// covered here.
///
/// # Panics
///
/// Does not panic.
pub fn to_sql_value(
    serial_type: &i64,
    data: &[u8],
) -> Result<SqlValue, Error> {
    use SqlValue::*;

    let mut c = std::io::Cursor::new(data);
    match serial_type {
        // Tabular comments have the following columns, and are take from SQLite docs:
        // Serial Type	Content Size	Meaning
        // 0	        0	            Value is a NULL.
        0 => Ok(Null()), // Nulls are always Null, regardless of what the desired type is.  All types have to handle the possibility of Null.
        // 1	        1	            Value is an 8-bit twos-complement integer.
        // 2	        2	            Value is a big-endian 16-bit twos-complement integer.
        // 3	        3	        Value is a big-endian 24-bit twos-complement integer.
        // 4	        4	        Value is a big-endian 32-bit twos-complement integer.
        // 5	        6	        Value is a big-endian 48-bit twos-complement integer.
        // 6	        8	        Value is a big-endian 64-bit twos-complement integer.
        x @ 1..=6 => {
            match x {
                1 => Ok(Int(c.read_i8().map_err(Error::Io)? as i64)),
                2 => Ok(Int(c.read_i16::<BigEndian>().map_err(Error::Io)? as i64)),
                3 => {
                    let mut bytes = [0_u8; 4];
                    c.read_exact(&mut bytes[1..]).map_err(Error::Io)?;
                    bytes[0] = match (bytes[1] & 0b1000_0000) > 0 {
                        false => 0,
                        true => 0xff,
                    };
                    Ok(Int(i32::from_be_bytes(bytes) as i64))
                }
                4 => Ok(Int(c.read_i32::<BigEndian>().map_err(Error::Io)? as i64)),
                5 => Err(Error::Unimplemented),
                6 => Ok(Int(c.read_i64::<BigEndian>().map_err(Error::Io)?)),
                _ => Err(Error::Unreachable),
            }
        }
        // 7	        8	        Value is a big-endian IEEE 754-2008 64-bit floating point number.
        7 => Ok(Real(c.read_f64::<BigEndian>().map_err(Error::Io)?)),
        // 8	        0	        Value is the integer 0. (Only available for schema format 4 and higher.)
        8 => Ok(Int(0_i64)),
        // 9	        0	        Value is the integer 1. (Only available for schema format 4 and higher.)
        9 => Ok(Int(1_i64)),
        // 10,11	variable	Reserved for internal use. These serial type codes will never appear in a well-formed database file...
        10 | 11 => Err(Error::InvalidSerialTypeCode),
        // N≥12 & even	(N-12)/2	Value is a BLOB that is (N-12)/2 bytes in length.
        // N≥13 & odd	(N-13)/2	Value is a string in the text encoding and (N-13)/2 bytes in length. The nul terminator is not stored.
        x @ 12.. => {
            match (*x % 2) == 0 {
                true /* odd */=>  {
                    let mut buf = vec![0_u8; (*x as usize - 12) / 2];
                    c.read_exact(&mut buf[..]).map_err(Error::Io)?;
                    Ok(Blob(buf.clone()))
                }
                false /* even */ => {
                    let mut buf = vec![0_u8; (*x as usize - 13) / 2];
                    c.read_exact(&mut buf[..]).map_err(Error::Io)?;
                    let s = String::from_utf8(buf).map_err(Error::InvalidStringEncoding)?;
                    Ok(Text(s))
                }
            }
        }
        i64::MIN..=-1 => Err(Error::InvalidSerialTypeCode),
    }
}

/// Convert a SQLite "Storage Class" value, stored in `sql_value::SqlValue` enum, into SQL type `t`, if possible.
/// Returns an Error if the requested cast is invalid.
///
///  # Arguments
/// * `t` - A `sql_value::SqlValue`, with one of these variants: `Int`, `Text`, `Real`, `Blob`, `Null`.
/// * `data` - A slice of bytes.
//////
/// # Comparison to SQLite
///
/// SQLite has "Storage Classes" [https://www.sqlite.org/datatype3.html#storage_classes_and_datatypes]
/// NULL, REAL, INTEGER, TEXT, BLOB, and each value is stored as one of those classes.
///
/// SQLite Columns have SQL type affinities, which are one of:
/// TEXT, NUMERIC, INTEGER, REAL, BLOB
/// We also support these types for columns, except NUMERIC which is wierd and we won't support that type name in the grammar
/// and so not in converstion either.
///
/// SQLites rules for when to convert from a Storage class (serial type) to the type affinity are complex.
///
/// We implement conversion rules that allows common, recently-generated SQLite-generated tables to be read.
/// At the same time, it avoids precision-loosing conversions.  It does not attempt to provide exact compatibility
/// with SQLite.
///
/// The following table shows what happens if an input SqlValue is requested to convert to SqlType.
/// The *Returns* column is written using `use sql_value::SqlValue::*;` and
/// `use diydb::serial_type::Error::*;`
///
/// | variant in | target type | returns | comments |
/// | ---------- | ----- | -------- | - |     
/// | NULL       | *     | Ok(Null) |   |
/// | Real       | Real  | Ok(Real) |   |
/// | Real       | Int   | Err      | we don't support this to avoid silent loss of precision |
/// | Real       | Text  | Err      |   |
/// | Real       | Blob  | Err      |   |
/// | Int        | Real  | Ok(Int)  | necessary since SQLite stores 2.0 as Integer(2). |
/// | Int        | Int   | Ok(Int)  | Smaller types are sign extended to i64. |
/// | Int        | Text  | Ok(Text) | necessary since SQLite stores "2" as Integer(2), etc. |
/// | Int        | Blob  | Err      |   |
/// | Text       | Real  | Err      |   |
/// | Text       | Text  | Ok(Text) |   |
/// | Text       | Int   | Err      |   |
/// | Text       | Blob  | Err      |   |
/// | Blob       | Real  | Err      |   |
/// | Blob       | Int   | Err      |   |
/// | Blob       | Text  | Err      |   |
/// | BLOB       | Blob  | Ok(Blob) |   |
///
/// # Panics
///
/// Does not panic.
pub fn cast_to_schema_type(
    v: SqlValue,
    t: SqlType,
) -> Result<SqlValue, Error> {
    use SqlType as SQT;
    use SqlValue::*;

    match v {
        Null() => Ok(Null()), // Nulls are always Null, regardless of what the desired type is.  All types have to handle the possibility of Null.
        Int(i) => {
            match t {
                SQT::Int => Ok(Int(i)),
                SQT::Real => Ok(Real(i as f64)),
                SQT::Text => Ok(Text(format!("{}", i))),
                SQT::Blob => Err(Error::Type {
                    from: SQT::Int,
                    to: SQT::Blob,
                }),
            }
        }
        Real(f) => {
            match t {
                SQT::Int => Err(Error::Type {
                    from: SQT::Real,
                    to: SQT::Int,
                }),
                SQT::Real => Ok(Real(f)),
                SQT::Text => Err(Error::Type {
                    from: SQT::Real,
                    to: SQT::Text,
                }),
                SQT::Blob => Err(Error::Type {
                    from: SQT::Real,
                    to: SQT::Blob,
                }),
            }
        }
        // TODO: Avoid copy of possibly large blobs and strings in some way:
        // a. take &mut ref to the value, and use std::mem::take(), leaving arg `&mut v` empty, and the string in the return value.
        // b. if possible, mutate the variant in place?
        Blob(b) => {
            match t {
                SQT::Int => Err(Error::Type{from: SQT::Blob, to: SQT::Int}),
                SQT::Real => Err(Error::Type{from: SQT::Blob, to: SQT::Real}),
                SQT::Text => Err(Error::Type{from: SQT::Blob, to: SQT::Text}),
                SQT::Blob => Ok(Blob(b.clone())), 
            }
        }
        Text(s) => {
            match t {
                SQT::Int => Err(Error::Type{from: SQT::Text, to: SQT::Int}),
                SQT::Real => Err(Error::Type{from: SQT::Text, to: SQT::Real}),
                SQT::Text => Ok(Text(s)),  // TODO: Avoid copy of possibly large strings using std::mem::take(), leaving arg `&mut v` empty.
                SQT::Blob => Err(Error::Type{from: SQT::Text, to: SQT::Blob}),
            }
        }
        Bool(_) => Err(Error::NotStorageClassType)
    }
}


#[test]
fn test_value_to_sql_typed_value() {
    use SqlValue::*;

    let cases: Vec<(&i64, SqlType, &[u8], SqlValue)> = vec![
        // Null storage to anything is Null
        (&0, SqlType::Int, b"", Null()),
        (&0, SqlType::Real, b"", Null()),
        (&0, SqlType::Text, b"", Null()),
        (&0, SqlType::Blob, b"", Null()),
        // one byte ints to Int works for various values.
        (&1, SqlType::Int, &[0x7f], Int(127)),
        (&1, SqlType::Int, &[0xff], Int(-1)),
        (&1, SqlType::Int, &[0x01], Int(1)),
        // one byte ints to Real.
        (&1, SqlType::Real, &[0x7f], Real(127_f64)),
        (&1, SqlType::Real, &[0xff], Real(-1_f64)),
        (&1, SqlType::Real, &[0x01], Real(1_f64)),
        // two byte ints
        (&2, SqlType::Int, &[0x00, 0x7f], Int(127)),
        (&2, SqlType::Int, &[0xff, 0xff], Int(-1)),
        (&2, SqlType::Int, &[0x00, 0x01], Int(1)),
        (&2, SqlType::Int, &[0x01, 0x00], Int(256)),
        // three byte ints
        (&3, SqlType::Int, &[0x00, 0x00, 0x7f], Int(127)),
        (&3, SqlType::Int, &[0xff, 0xff, 0xff], Int(-1)),
        (&3, SqlType::Int, &[0x00, 0x00, 0x01], Int(1)),
        (&3, SqlType::Int, &[0x00, 0x01, 0x00], Int(256)),
        (&3, SqlType::Int, &[0x01, 0x00, 0x00], Int(65536)),
        // TODO: larger ints and float.
        // Literal 0 and 1
        (&8, SqlType::Int, b"", Int(0)),
        (&8, SqlType::Int, b"", Int(0)),
        (&9, SqlType::Int, b"", Int(1)),
        (&9, SqlType::Int, b"", Int(1)),
        // Text of various lengths
        (&13, SqlType::Text, b"", Text("".to_string())),
        (&19, SqlType::Text, b"Foo", Text("Foo".to_string())),
        (&25, SqlType::Text, b"FooBar", Text("FooBar".to_string())),
        // Blob
        (
            &18,
            SqlType::Blob,
            &[0x00_u8, 0x01, 0xff],
            Blob(Vec::from([0, 1, 255])),
        ),
    ];
    for (i, case) in cases.iter().enumerate() {
        println!(
            "Testing case {}: convert serial type {} to SQL type {}",
            i, case.0, case.1
        );
        assert_eq!(
            value_to_sql_typed_value(case.0, case.1, case.2).unwrap(),
            case.3
        );
    }
}

#[test]
fn test_value_to_sql_typed_value_errors() {
    let cases: Vec<(&i64, SqlType, &[u8])> = vec![
        // ints to blob is error.
        (&1, SqlType::Blob, &[0x7f]),
        (&2, SqlType::Blob, &[0x00, 0x7f]),
        (&3, SqlType::Blob, &[0x01, 0x00, 0x00]),
        (&8, SqlType::Blob, b""),
        (&9, SqlType::Blob, b""),
        // Text to anythin else is an error.
        (&19, SqlType::Int, b"Foo"),
        (&19, SqlType::Real, b"Foo"),
        (&19, SqlType::Blob, b"Foo"),
        // Blob
        (&18, SqlType::Int, &[0x00_u8, 0x01, 0xff]),
        (&18, SqlType::Real, &[0x00_u8, 0x01, 0xff]),
        (&18, SqlType::Text, &[0x00_u8, 0x01, 0xff]),
        // Blob to anythin else is an error.
    ];

    for (i, case) in cases.iter().enumerate() {
        println!(
            "Testing case {}: convert serial type {} to SQL type {}, should error",
            i, case.0, case.1
        );
        assert!(value_to_sql_typed_value(case.0, case.1, case.2).is_err());
    }
}
