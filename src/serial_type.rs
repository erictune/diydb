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

/// returns the length in bytes implied by a SQLite serial type code. 
pub fn serialized_size(serial_type: i64) -> usize {
    match serial_type {
        // Serial Type	Content Size	Meaning
        // 0	        0	            Value is a NULL.
        0 => 0,
        // 1	        1	            Value is an 8-bit twos-complement integer.
        1 => 1,
        // 2	        2	            Value is a big-endian 16-bit twos-complement integer.
        2 => 2,
        // 3	        3	            Value is a big-endian 24-bit twos-complement integer.
        3 => 3,
        // 4	        4	            Value is a big-endian 32-bit twos-complement integer.
        4 => 4,
        // 5	        6	            Value is a big-endian 48-bit twos-complement integer.
        5 => 6,
        // 6	        8	            Value is a big-endian 64-bit twos-complement integer.
        // 7	        8	            Value is a big-endian IEEE 754-2008 64-bit floating point number.
        6 | 7 => 8,
        // 8	        0	            Value is the integer 0. (Only available for schema format 4 and higher.)
        // 9	        0	            Value is the integer 1. (Only available for schema format 4 and higher.)
        8 | 9 => 0,
        // 10,11	    variable	    Reserved for internal use. These serial type codes will never appear in a well-formed database file, but they might be used in transient and temporary database files that SQLite sometimes generates for its own use. The meanings of these codes can shift from one release of SQLite to the next.
        // N≥12 & even	(N-12)/2	    Value is a BLOB that is (N-12)/2 bytes in length.
        // N≥13 & odd	(N-13)/2	    Value is a string in the text encoding and (N-13)/2 bytes in length. The nul terminator is not stored.
        x if x >= 12 => (x as usize - 12 - (x % 2) as usize) / 2,
        _ => unimplemented!(),
    }
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

#[test]
fn test_to_sql_value() {
    use SqlValue::*;

    let cases: Vec<(&i64, &[u8], SqlValue)> = vec![
        // Null storage to anything is Null
        (&0, b"", Null()),
        // one byte ints
        (&1, &[0x7f], Int(127)),
        (&1, &[0xff], Int(-1)),
        (&1, &[0x01], Int(1)),
        // two byte ints
        (&2, &[0x00, 0x7f], Int(127)),
        (&2, &[0xff, 0xff], Int(-1)),
        (&2, &[0x00, 0x01], Int(1)),
        (&2, &[0x01, 0x00], Int(256)),
        // three byte ints
        (&3, &[0x00, 0x00, 0x7f], Int(127)),
        (&3, &[0xff, 0xff, 0xff], Int(-1)),
        (&3, &[0x00, 0x00, 0x01], Int(1)),
        (&3, &[0x00, 0x01, 0x00], Int(256)),
        (&3, &[0x01, 0x00, 0x00], Int(65536)),
        // TODO: larger ints and float.
        // Literal 0 and 1
        (&8, b"", Int(0)),
        (&9, b"", Int(1)),
        // Text of various lengths
        (&13, b"", Text("".to_string())),
        (&19, b"Foo", Text("Foo".to_string())),
        (&25, b"FooBar", Text("FooBar".to_string())),
        // Blob
        (&18, &[0x00_u8, 0x01, 0xff], Blob(Vec::from([0, 1, 255]))),
    ];
    for (i, case) in cases.iter().enumerate() {
        println!("Testing case {}: deserialize typecode {}", i, case.0);
        assert_eq!(to_sql_value(case.0, case.1).unwrap(), case.2);
    }
}

#[test]
fn test_to_sql_value_errors() {
    let cases: Vec<(&i64, &[u8])> = vec![
        // ints to blob is error.
        (&-1, &[0x0, 0x0]),
        (&-12345, &[0x0, 0x0]),
        (&10, &[0x00, 0x7f]),
        (&11, &[0x01, 0x00, 0x00]),
    ];

    for (i, case) in cases.iter().enumerate() {
        println!("Testing case {}: deserializetypecode {} , should error", i, case.0);
        assert!(to_sql_value(case.0, case.1).is_err());
    }
}

/// Convert a SQLite "Storage Class" value, stored in `sql_value::SqlValue` enum, into SQL type `t`, if possible.
/// Returns an Error if the requested cast is invalid.
///
///  # Arguments
/// * `t` - A `sql_value::SqlValue`, with one of these variants: `Int`, `Text`, `Real`, `Blob`, `Null`.
/// * `data` - A slice of bytes.
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
    v: &SqlValue,
    t: SqlType,
) -> Result<SqlValue, Error> {
    use SqlType as SQT;
    use SqlValue::*;
    // TODO: Avoid copy of possibly large blobs and strings in some way:
    // a. take &mut ref to the value, and use std::mem::take(), leaving arg `&mut v` empty, and the string in the return value.
    // b. if possible, mutate the variant in place via `&mut v`?
    match v {
        Null() => Ok(Null()), // Nulls are always Null, regardless of what the desired type is.  All types have to handle the possibility of Null.
        Int(i) => {
            match t {
                SQT::Int => Ok(Int(*i)),
                SQT::Real => Ok(Real(*i as f64)),
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
                SQT::Real => Ok(Real(*f)),
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
                SQT::Text => Ok(Text(s.clone())),
                SQT::Blob => Err(Error::Type{from: SQT::Text, to: SQT::Blob}),
            }
        }
        Bool(_) => Err(Error::NotStorageClassType)
    }
}

#[test]
fn test_cast_to_schema_type() {
    use SqlValue::*;

    let cases: Vec<(SqlValue, SqlType, SqlValue)> = vec![
        // Null storage to anything is Null
        (Null(), SqlType::Int,  Null()),
        (Null(), SqlType::Real, Null()),
        (Null(), SqlType::Text, Null()),
        (Null(), SqlType::Blob, Null()),
        //  int to Int works for various values.
        (Int(127), SqlType::Int, Int(127)),
        (Int(-1),  SqlType::Int, Int(-1)),
        (Int(1),   SqlType::Int, Int(1)),
        //  int to Real.
        (Int(127), SqlType::Real, Real(127_f64)),
        (Int(-1),  SqlType::Real, Real(-1_f64)),
        (Int(1),   SqlType::Real, Real(1_f64)),
        // TODO: larger ints and float.
        // 0 and 1
        (Int(0), SqlType::Int, Int(0)),
        (Int(1), SqlType::Int, Int(1)),
        // Text of various lengths
        (Text("".to_string()),       SqlType::Text, Text("".to_string())),
        (Text("Foo".to_string()),    SqlType::Text, Text("Foo".to_string())),
        (Text("FooBar".to_string()), SqlType::Text, Text("FooBar".to_string())),
        // Blob
        (Blob(Vec::from([0, 1, 255])), SqlType::Blob, Blob(Vec::from([0, 1, 255]))),
    ];
    for (i, case) in cases.iter().enumerate() {
        println!(
            "Testing case {}: convert SQL value {} to SQL type {}",
            i, case.0, case.1
        );
        assert_eq!(cast_to_schema_type(&case.0, case.1).unwrap(), case.2);
    }
}

#[test]
fn test_value_to_sql_typed_value_errors() {
    use SqlValue::*;

    let cases: Vec<(SqlValue, SqlType)> = vec![
        // Ints to blob is error.
        (Int(1), SqlType::Blob),
        // Text to anything else is an error.
        (Text("hi".to_string()), SqlType::Int),
        (Text("hi".to_string()), SqlType::Real),
        (Text("hi".to_string()), SqlType::Blob),
        // Blob to anything else is an error.
        (Blob(Vec::from([0, 1, 255])), SqlType::Int),
        (Blob(Vec::from([0, 1, 255])), SqlType::Real),
        (Blob(Vec::from([0, 1, 255])), SqlType::Text),
        // Bool is not supported for casting at this time.
        (Bool(false), SqlType::Int),
        (Bool(true), SqlType::Int),
    ];

    for (i, case) in cases.iter().enumerate() {
        println!("Testing case {}: convert serial type {} to SQL type {}, should error", i, &case.0, case.1);
        assert!(cast_to_schema_type(&case.0, case.1).is_err());
    }
}

/// Convert a native value (SqlValue) into a SQL "serial type" format, consisting of a serial type code and bytes.
///
/// # Arguments
/// * `v` - a sql_value::SqlValue.
/// 
/// # Precondition
/// Zero in the range `into_bytes[0..into_bytes.len()]`, or values you are prepared to have overwritten.
///
/// # Returns
/// On success, `Ok((slice, typecode, length))`
/// where:
///   - `slice` is the encoded bytes to be stored in the body of the row record.
///   - `typecode` is the sqlite typecode to be stored in the header.
/// On failure, `Err(Error::_)`.
///
/// # Details
/// A `SqlValue::Null()` results in a NULL type code (0).
///
/// Yields values compatible with SQLite schema format 4 only.
/// SQLites rules for when to convert from a Storage class (serial type) to the type affinity are complex.
///
/// We implement automatic conversion rules that allows common, writing SQLite-compatible values for common use cases, 
/// but do not attempt to provide exact compatibility with SQLite.
/// For example:
///   - Unlike SQLite, Text("0") is not stored as zero bytes with serial code 8.  It is stored as any other small integer.
///   - Unlike SQLite, Real("1.0") is not stored as 1 byte, 1_u8, with serial code 1. It is stored as any other real.

/// The following table shows what happens if a value with a certain SQL type enum variant (Enum) is converted.
/// Its Storage Class will be a shown in SerTy#.
/// The "enum" column is written assuming `use sql_value::SqlValue::*;`
///
/// | SqlValue | SerTy# | bytes len() |         comments       |
/// | -------- | ------ | ----------- | ---------------------- |
/// |   Null   | 0      | 0           |                        |
/// |   Int    | 1      | 1           |  if it fits in an i8.  |
/// |   Int    | 2      | 2           |  if it fits in an i16. |
/// |          | 3      |             |  24-bit repr not supported yet. |
/// |   Int    | 4      | 4           |  if it fits in an i32. |
/// |          | 5      |             |  48-bit repr not supported yet. |
/// |   Int    | 6      | 8           |  if it fits in an i64. |
/// |   Real   | 7      | 8           |                        |
/// |   Int    | 8      | 0           |  if it is 0. |
/// |   Int    | 9      | 0           |  if it is 1. |
/// |   Text   | N≥12 & even |        |   |
/// |   Blob   | N≥13 & odd  |        |   |
///
/// Code is not optimized for memory usage for large Blobs or Text.
/// When we get to writes, we may need a new conversion table.
///
/// # Panics
///
/// Does not panic.
pub fn to_serial_type<'a>(v: &'a SqlValue) -> Result<(Vec<u8>, i64, usize), Error> {
    use SqlValue::*;
    match v {
        Null() => Ok((Vec::new(), 0, 0)),
        Int(x) => {
            match x {
                0 => {
                    Ok((Vec::new(), 8, 0))
                }
                1 => {
                    Ok((Vec::new(), 9, 0))
                }
                -127..=128 => {
                    Ok(((*x as u8).to_be_bytes().to_vec(), 1, 1))
                }
                -32_768..=32_767 => {
                    Ok(((*x as u16).to_be_bytes().to_vec(), 2, 2))
                }
                // TODO: support 24, 32, 48 bits.
                _ => {
                    Ok(((*x as u64).to_be_bytes().to_vec(), 6, 8))
                }
            }
        }
        Real(x) => Ok((x.to_be_bytes().to_vec(), 7, 8)),
        Text(x) => {
            let b = x.as_bytes().to_vec();
            let l = b.len();
            Ok((b, (l as i64)*2 + 13, l))
        }
        // These could be supported, but aren't yet.
        Blob(_) => Err(Error::Unimplemented),
        Bool(_) => Err(Error::NotStorageClassType),
    }
}

#[test]
fn test_to_serial_type_simple() {
    let (data, typecode, length) = to_serial_type(&SqlValue::Int(37)).unwrap();
    assert_eq!(typecode, 1);
    assert_eq!(data, &[37_u8; 1]);
    assert_eq!(length, 1);

}

#[test]
fn test_to_serial_type() {
    //0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
    let cases: Vec<(SqlValue, Vec<u8>, i64)> = vec![
        (SqlValue::Int(0), vec![], 8),
        (SqlValue::Int(1), vec![], 9),
        (SqlValue::Int(2), vec![2], 1),
        (SqlValue::Int(-1), vec![0xff], 1),
        (SqlValue::Int(-512), vec![0xfe, 0x00], 2),
        (SqlValue::Int(0x7f_ff_ff_ff_ff_ff_ff_ff), vec![0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], 6),
        (SqlValue::Null(), vec![], 0),
        (SqlValue::Text("Hi".to_string()), vec!['H' as u8, 'i' as u8], 17),
    ];
    let numcases = cases.len();
    let mut casenum = 1;
    for case in cases {
        println!("Case {} of {}", casenum, numcases);
        let (data, typecode, _) = to_serial_type(&case.0).unwrap();
        assert_eq!(typecode, case.2);
        assert_eq!(data.to_vec(), case.1);
        casenum +=1;
    }
}