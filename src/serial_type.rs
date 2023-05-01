use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use std::io::Read;

use crate::sql_type::SqlType;
use crate::sql_value::SqlValue;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Pager: Error accessing database file: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Unable to convert type {from} to {to}.")]
    TypeError { from: String, to: String },
    #[error("Unimplemented type.")]
    UnimplementedError,
    #[error("Invalid serial type code.")]
    InvalidSerialTypeCode,
    #[error("Byte were not a valid string valid encoding.")]
    InvalidStringEncodingError(#[from] std::string::FromUtf8Error),
}
use Error::*;
const TYPE_NAME_INT: &str = "INT";
const TYPE_NAME_NULL: &str = "NULL";
const TYPE_NAME_REAL: &str = "REAL";

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

/// Convert a sqlite value in "serial type" format into a new String suitable for debug printing.
///
///  # Arguments
/// * `serial_type` - A SQLite serial type code.
/// * `data` - A byte slice containing the data to be read.
///
/// The strings are not suitable for direct SQL query output, since the serial type needs to be converted to the
/// schema type.
///
/// Does not handle overflowing TEXT or BLOB.
///
/// # Panics
///
/// Does not panic.
pub fn value_to_string(serial_type: &i64, data: &[u8]) -> Result<String, Error> {
    let mut c = std::io::Cursor::new(data);
    match serial_type {
        // Serial Type	Content Size	Meaning
        // 0	        0	            Value is a NULL.
        0 => Ok("NULL".to_string()),
        // 1	        1	            Value is an 8-bit twos-complement integer.
        1 => Ok(format!("{}", c.read_i8().map_err(|e| IoError(e))?)),
        // 2	        2	            Value is a big-endian 16-bit twos-complement integer.
        2 => Ok(format!(
            "{}",
            c.read_i16::<BigEndian>().map_err(|e| IoError(e))?
        )),
        // 3	        3	        Value is a big-endian 24-bit twos-complement integer.
        3 => {
            let mut bytes = [0_u8; 4];
            c.read_exact(&mut bytes[1..]).map_err(|e| IoError(e))?;
            bytes[0] = match (bytes[1] & 0b1000_0000) > 0 {
                false => 0,
                true => 0xff,
            };
            Ok(i32::from_be_bytes(bytes).to_string())
        }
        // 4	        4	        Value is a big-endian 32-bit twos-complement integer.
        4 => Ok(format!(
            "{}",
            c.read_i32::<BigEndian>().map_err(|e| IoError(e))?
        )),
        // 5	        6	        Value is a big-endian 48-bit twos-complement integer.
        5 => Err(UnimplementedError),
        // 6	        8	        Value is a big-endian 64-bit twos-complement integer.
        6 => Ok(format!(
            "{}",
            c.read_i64::<BigEndian>().map_err(|e| IoError(e))?
        )),
        // 7	        8	        Value is a big-endian IEEE 754-2008 64-bit floating point number.
        7 => Ok(format!(
            "{}",
            c.read_f64::<BigEndian>().map_err(|e| IoError(e))?
        )),
        // 8	        0	        Value is the integer 0. (Only available for schema format 4 and higher.)
        8 => Ok("0".to_string()),
        // 9	        0	        Value is the integer 1. (Only available for schema format 4 and higher.)
        9 => Ok("1".to_string()),
        // 10,11	    variable	Reserved for internal use. These serial type codes will never appear in a well-formed database file, but they might be used in transient and temporary database files that SQLite sometimes generates for its own use. The meanings of these codes can shift from one release of SQLite to the next.
        10 | 11 => Err(InvalidSerialTypeCode),
        // N≥12 & even	(N-12)/2	Value is a BLOB that is (N-12)/2 bytes in length.
        // N≥13 & odd	(N-13)/2	Value is a string in the text encoding and (N-13)/2 bytes in length. The nul terminator is not stored.
        x @ 12.. => {
            match (x % 2) == 0 {
                true /* odd */ => {
                    let mut buf = vec![0_u8; (*x as usize - 12) / 2];
                    c.read_exact(&mut buf[..]).map_err(|e| IoError(e))?;
                    Ok(format!("{:?}", buf))
                }
                false /* even */ => {
                let mut buf = vec![0_u8; (*x as usize - 13) / 2];
                    c.read_exact(&mut buf[..]).map_err(|e| IoError(e))?;
                    Ok(String::from_utf8(buf).map_err(|e: std::string::FromUtf8Error| InvalidStringEncodingError(e))?)
                }
            }
        }
        i64::MIN..=-1 => Err(Error::InvalidSerialTypeCode),
    }
}

#[test]
fn test_value_to_string() {
    // Null
    assert_eq!(value_to_string(&0, b"").unwrap(), "NULL".to_string());

    // one byte ints
    assert_eq!(value_to_string(&1, &[0x7f]).unwrap(), "127".to_string());
    assert_eq!(value_to_string(&1, &[0xff]).unwrap(), "-1".to_string());
    assert_eq!(value_to_string(&1, &[0x01]).unwrap(), "1".to_string());

    // two byte ints
    assert_eq!(
        value_to_string(&2, &[0x00, 0x7f]).unwrap(),
        "127".to_string()
    );
    assert_eq!(
        value_to_string(&2, &[0xff, 0xff]).unwrap(),
        "-1".to_string()
    );
    assert_eq!(value_to_string(&2, &[0x00, 0x01]).unwrap(), "1".to_string());
    assert_eq!(
        value_to_string(&2, &[0x01, 0x00]).unwrap(),
        "256".to_string()
    );

    // three byte ints
    assert_eq!(
        value_to_string(&3, &[0x00, 0x00, 0x7f]).unwrap(),
        "127".to_string()
    );
    assert_eq!(
        value_to_string(&3, &[0xff, 0xff, 0xff]).unwrap(),
        "-1".to_string()
    );
    assert_eq!(
        value_to_string(&3, &[0x00, 0x00, 0x01]).unwrap(),
        "1".to_string()
    );
    assert_eq!(
        value_to_string(&3, &[0x00, 0x01, 0x00]).unwrap(),
        "256".to_string()
    );
    assert_eq!(
        value_to_string(&3, &[0x01, 0x00, 0x00]).unwrap(),
        "65536".to_string()
    );

    // TODO: larger ints and float.

    // Literal 0 and 1
    assert_eq!(value_to_string(&8, b"").unwrap(), "0".to_string());

    assert_eq!(value_to_string(&9, b"").unwrap(), "1".to_string());

    // Text of various lengths
    assert_eq!(value_to_string(&13, b"").unwrap(), "".to_string());

    assert_eq!(value_to_string(&19, b"Foo").unwrap(), "Foo".to_string());

    assert_eq!(
        value_to_string(&25, b"FooBar").unwrap(),
        "FooBar".to_string()
    );

    // Blob
    assert_eq!(
        value_to_string(&18, &[0x00_u8, 0x01, 0xff]).unwrap(),
        "[0, 1, 255]".to_string()
    );
}

/// Convert a sqlite value in "serial type" format into Some(i64).
/// Returns an Error if the type is unsuitable for conversion to i64.
/// Returns an Error if there is a problem reading from the data.
///
/// Provides simplified code for internal use cases where only an i64 is ever expected.
///
///  # Arguments
/// * `serial_type` - A SQLite serial type code.
/// * `data` - A slice of bytes.
/// * `convert_nulls_to_zero`  - controls result when type is NULL.
///
/// If `convert_nulls_to_zero` is true, NULL results in a Zero value.  If false, NULL results in None.
/// If the type is f64, None is returned.
/// BLOB and TEXT always return NONE.
///
/// # Panics
///
/// Does not panic
pub fn value_to_i64(
    serial_type: &i64,
    data: &[u8],
    convert_nulls_to_zero: bool,
) -> Result<i64, Error> {
    let mut c = std::io::Cursor::new(data);
    match serial_type {
        // Serial Type	Content Size	Meaning
        // 0	        0	            Value is a NULL.
        0 => {
            if convert_nulls_to_zero {
                Ok(0)
            } else {
                let from = String::from(TYPE_NAME_NULL);
                let to = String::from(TYPE_NAME_INT);
                Err(TypeError { from, to })
            }
        }
        // 1	        1	            Value is an 8-bit twos-complement integer.
        1 => Ok(c.read_i8().map_err(|e| IoError(e))? as i64),
        // 2	        2	            Value is a big-endian 16-bit twos-complement integer.
        2 => Ok(c.read_i16::<BigEndian>().map_err(|e| IoError(e))? as i64),
        // 3	        3	        Value is a big-endian 24-bit twos-complement integer.
        3 => {
            let mut bytes = [0_u8; 4];
            c.read_exact(&mut bytes[1..]).map_err(|e| IoError(e))?;
            bytes[0] = match (bytes[1] & 0b1000_0000) > 0 {
                false => 0,
                true => 0xff,
            };
            Ok(i32::from_be_bytes(bytes) as i64)
        }
        // 4	        4	        Value is a big-endian 32-bit twos-complement integer.
        4 => Ok(c.read_i32::<BigEndian>().map_err(|e| IoError(e))? as i64),
        // 5	        6	        Value is a big-endian 48-bit twos-complement integer.
        5 => Err(UnimplementedError),
        // 6	        8	        Value is a big-endian 64-bit twos-complement integer.
        6 => Ok(c.read_i64::<BigEndian>().map_err(|e| IoError(e))?),
        // 7	        8	        Value is a big-endian IEEE 754-2008 64-bit floating point number.
        7 => {
            let from = String::from(TYPE_NAME_REAL);
            let to = String::from(TYPE_NAME_INT);
            Err(TypeError { from, to })
        }
        // 8	        0	        Value is the integer 0. (Only available for schema format 4 and higher.)
        8 => Ok(0_i64),
        // 9	        0	        Value is the integer 1. (Only available for schema format 4 and higher.)
        9 => Ok(1_i64),
        // 10,11	    variable	Reserved for internal use. These serial type codes will never appear in a well-formed database file, but they might be used in transient and temporary database files that SQLite sometimes generates for its own use. The meanings of these codes can shift from one release of SQLite to the next.
        10 | 11 => Err(InvalidSerialTypeCode),
        // N≥12         variable    BLOB or TEXT
        12.. => Err(Error::UnimplementedError),
        i64::MIN..=-1 => Err(Error::InvalidSerialTypeCode),
    }
}

#[test]
fn test_value_to_i64() {
    // Null
    assert!(value_to_i64(&0, b"", false).is_err());
    assert_eq!(value_to_i64(&0, b"", true).unwrap(), 0_i64);

    // one byte ints
    assert_eq!(value_to_i64(&1, &[0x7f], false).unwrap(), 127);
    assert_eq!(value_to_i64(&1, &[0xff], true).unwrap(), -1);
    assert_eq!(value_to_i64(&1, &[0x01], false).unwrap(), 1);

    // two byte ints
    assert_eq!(value_to_i64(&2, &[0x00, 0x7f], false).unwrap(), 127);
    assert_eq!(value_to_i64(&2, &[0xff, 0xff], true).unwrap(), -1);
    assert_eq!(value_to_i64(&2, &[0x00, 0x01], false).unwrap(), 1);
    assert_eq!(value_to_i64(&2, &[0x01, 0x00], true).unwrap(), 256);

    // three byte ints
    assert_eq!(value_to_i64(&3, &[0x00, 0x00, 0x7f], true).unwrap(), 127);
    assert_eq!(value_to_i64(&3, &[0xff, 0xff, 0xff], false).unwrap(), -1);
    assert_eq!(value_to_i64(&3, &[0x00, 0x00, 0x01], true).unwrap(), 1);
    assert_eq!(value_to_i64(&3, &[0x00, 0x01, 0x00], false).unwrap(), 256);
    assert_eq!(value_to_i64(&3, &[0x01, 0x00, 0x00], true).unwrap(), 65536);

    // TODO: larger ints and float.

    // Literal 0 and 1
    assert_eq!(value_to_i64(&8, b"", false).unwrap(), 0);
    assert_eq!(value_to_i64(&8, b"", true).unwrap(), 0);

    assert_eq!(value_to_i64(&9, b"", false).unwrap(), 1);
    assert_eq!(value_to_i64(&9, b"", true).unwrap(), 1);

    // Text of various lengths
    assert!(value_to_i64(&13, b"", false).is_err());
    assert!(value_to_i64(&13, b"", true).is_err());

    assert!(value_to_i64(&19, b"Foo", false).is_err());
    assert!(value_to_i64(&19, b"Foo", true).is_err());

    // Blob
    assert!(value_to_i64(&18, &[0x00_u8, 0x01, 0xff], false).is_err());
}

/// Convert a sqlite value in "serial type enum" format into an enum that can hold any value (`SqlValue`).
/// Returns an Error if there is a problem reading from the data.
///
///  # Arguments
/// * `serial_type` - A SQLite serial type code.
/// * `data` - A slice of bytes.
/// * `convert_nulls_to_zero`  - controls result when type is NULL.
///
/// If `convert_nulls_to_zero` is true, NULL serial type results in a `SqlValue::Int(0)` value.
/// If false, NULL results in `SqlValue::None()`.
///
/// # Panics
///
/// Does not panic.
pub fn value_to_sql_typed_value(
    serial_type: &i64,
    data: &[u8],
    convert_nulls_to_zero: bool,
) -> Result<SqlValue, Error> {
    use SqlValue::*;

    let mut c = std::io::Cursor::new(data);
    match serial_type {
        // Serial Type	Content Size	Meaning
        // 0	        0	            Value is a NULL.
        0 => {
            if convert_nulls_to_zero {
                Ok(Int(0))
            } else {
                Ok(Null())
            }
        }
        // 1	        1	            Value is an 8-bit twos-complement integer.
        1 => Ok(Int(c.read_i8().map_err(|e| IoError(e))? as i64)),
        // 2	        2	            Value is a big-endian 16-bit twos-complement integer.
        2 => Ok(Int(
            c.read_i16::<BigEndian>().map_err(|e| IoError(e))? as i64
        )),
        // 3	        3	        Value is a big-endian 24-bit twos-complement integer.
        3 => {
            let mut bytes = [0_u8; 4];
            c.read_exact(&mut bytes[1..]).map_err(|e| IoError(e))?;
            bytes[0] = match (bytes[1] & 0b1000_0000) > 0 {
                false => 0,
                true => 0xff,
            };
            Ok(Int(i32::from_be_bytes(bytes) as i64))
        }
        // 4	        4	        Value is a big-endian 32-bit twos-complement integer.
        4 => Ok(Int(
            c.read_i32::<BigEndian>().map_err(|e| IoError(e))? as i64
        )),
        // 5	        6	        Value is a big-endian 48-bit twos-complement integer.
        5 => Err(UnimplementedError),
        // 6	        8	        Value is a big-endian 64-bit twos-complement integer.
        6 => Ok(Int(c.read_i64::<BigEndian>().map_err(|e| IoError(e))?)),
        // 7	        8	        Value is a big-endian IEEE 754-2008 64-bit floating point number.
        7 => Ok(Real(c.read_f64::<BigEndian>().map_err(|e| IoError(e))?)),
        // 8	        0	        Value is the integer 0. (Only available for schema format 4 and higher.)
        8 => Ok(Int(0_i64)),
        // 9	        0	        Value is the integer 1. (Only available for schema format 4 and higher.)
        9 => Ok(Int(1_i64)),
        // 10,11	variable	Reserved for internal use. These serial type codes will never appear in a well-formed database file, but they might be used in transient and temporary database files that SQLite sometimes generates for its own use. The meanings of these codes can shift from one release of SQLite to the next.
        10 | 11 => Err(InvalidSerialTypeCode),
        // N≥12 & even	(N-12)/2	Value is a BLOB that is (N-12)/2 bytes in length.
        // N≥13 & odd	(N-13)/2	Value is a string in the text encoding and (N-13)/2 bytes in length. The nul terminator is not stored.
        x @ 12.. => {
            match (*x % 2) == 0 {
                true /* odd */=>  {
                    let mut buf = vec![0_u8; (*x as usize - 12) / 2];
                    c.read_exact(&mut buf[..]).map_err(|e| IoError(e))?;
                    Ok(Blob(buf.clone()))
                }
                false /* even */ => {
                    let mut buf = vec![0_u8; (*x as usize - 13) / 2];
                    c.read_exact(&mut buf[..]).map_err(|e| IoError(e))?;
                    Ok(Text(String::from_utf8(buf).map_err(|e| InvalidStringEncodingError(e))?))
                }
            }
        }
        i64::MIN..=-1 => Err(Error::InvalidSerialTypeCode),
    }
}

#[test]
fn test_value_to_sql_typed_value() {
    use SqlValue::*;

    // Null
    assert_eq!(value_to_sql_typed_value(&0, b"", false).unwrap(), Null());
    assert_eq!(value_to_sql_typed_value(&0, b"", true).unwrap(), Int(0_i64));

    // one byte ints
    assert_eq!(
        value_to_sql_typed_value(&1, &[0x7f], false).unwrap(),
        Int(127)
    );
    assert_eq!(
        value_to_sql_typed_value(&1, &[0xff], true).unwrap(),
        Int(-1)
    );
    assert_eq!(
        value_to_sql_typed_value(&1, &[0x01], false).unwrap(),
        Int(1)
    );

    // two byte ints
    assert_eq!(
        value_to_sql_typed_value(&2, &[0x00, 0x7f], false).unwrap(),
        Int(127)
    );
    assert_eq!(
        value_to_sql_typed_value(&2, &[0xff, 0xff], true).unwrap(),
        Int(-1)
    );
    assert_eq!(
        value_to_sql_typed_value(&2, &[0x00, 0x01], false).unwrap(),
        Int(1)
    );
    assert_eq!(
        value_to_sql_typed_value(&2, &[0x01, 0x00], true).unwrap(),
        Int(256)
    );

    // three byte ints
    assert_eq!(
        value_to_sql_typed_value(&3, &[0x00, 0x00, 0x7f], true).unwrap(),
        Int(127)
    );
    assert_eq!(
        value_to_sql_typed_value(&3, &[0xff, 0xff, 0xff], false).unwrap(),
        Int(-1)
    );
    assert_eq!(
        value_to_sql_typed_value(&3, &[0x00, 0x00, 0x01], true).unwrap(),
        Int(1)
    );
    assert_eq!(
        value_to_sql_typed_value(&3, &[0x00, 0x01, 0x00], false).unwrap(),
        Int(256)
    );
    assert_eq!(
        value_to_sql_typed_value(&3, &[0x01, 0x00, 0x00], true).unwrap(),
        Int(65536)
    );

    // TODO: larger ints and float.

    // Literal 0 and 1
    assert_eq!(value_to_sql_typed_value(&8, b"", false).unwrap(), Int(0));
    assert_eq!(value_to_sql_typed_value(&8, b"", true).unwrap(), Int(0));

    assert_eq!(value_to_sql_typed_value(&9, b"", false).unwrap(), Int(1));
    assert_eq!(value_to_sql_typed_value(&9, b"", true).unwrap(), Int(1));

    // Text of various lengths
    assert_eq!(
        value_to_sql_typed_value(&13, b"", true).unwrap(),
        Text("".to_string())
    );
    assert_eq!(
        value_to_sql_typed_value(&19, b"Foo", false).unwrap(),
        Text("Foo".to_string())
    );
    assert_eq!(
        value_to_sql_typed_value(&25, b"FooBar", true).unwrap(),
        Text("FooBar".to_string())
    );

    // Blob
    assert_eq!(
        value_to_sql_typed_value(&18, &[0x00_u8, 0x01, 0xff], false).unwrap(),
        Blob(Vec::from([0, 1, 255]))
    );
}
