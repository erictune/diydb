use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use std::io::Read;

/// Convert a serial type number to a string.
///
///  # Arguments
///
/// * `serial_type` - A SQLite serial type code.
///
/// These are not SQL type, but informal names for debugging.
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
        x if x >= 12 && (x % 2 == 0) => "st:blob",
        // N≥13 and odd	(N-13)/2	Value is a string in the text encoding and (N-13)/2 bytes in length. The nul terminator is not stored.
        x if x >= 13 && (x % 2 == 1) => "st:text",
        _ => panic!("Unknown column type: {}", serial_type),
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
/// Panics on errors.
// TODO: handle errors better, by returning a Result.
pub fn value_to_string(serial_type: &i64, data: &[u8]) -> String {
    let mut c = std::io::Cursor::new(data);
    match serial_type {
        // Serial Type	Content Size	Meaning
        // 0	        0	            Value is a NULL.
        0 => "NULL".to_string(),
        // 1	        1	            Value is an 8-bit twos-complement integer.
        1 => format!("{}", c.read_i8().unwrap()),
        // 2	        2	            Value is a big-endian 16-bit twos-complement integer.
        2 => format!("{}", c.read_i16::<BigEndian>().unwrap()),
        // 3	        3	        Value is a big-endian 24-bit twos-complement integer.
        3 => {
            let mut bytes = [0_u8; 4];
            c.read_exact(&mut bytes[1..]).unwrap();
            bytes[0] = match (bytes[1] & 0b1000_0000) > 0 {
                false => 0,
                true => 0xff,
            };
            i32::from_be_bytes(bytes).to_string()
        }
        // 4	        4	        Value is a big-endian 32-bit twos-complement integer.
        4 => format!("{}", c.read_i32::<BigEndian>().unwrap()),
        // 5	        6	        Value is a big-endian 48-bit twos-complement integer.
        5 => unimplemented!(),
        // 6	        8	        Value is a big-endian 64-bit twos-complement integer.
        6 => format!("{}", c.read_i64::<BigEndian>().unwrap()),
        // 7	        8	        Value is a big-endian IEEE 754-2008 64-bit floating point number.
        7 => format!("{}", c.read_f64::<BigEndian>().unwrap()),
        // 8	        0	        Value is the integer 0. (Only available for schema format 4 and higher.)
        8 => "0".to_string(),
        // 9	        0	        Value is the integer 1. (Only available for schema format 4 and higher.)
        9 => "1".to_string(),
        // 10,11	    variable	Reserved for internal use. These serial type codes will never appear in a well-formed database file, but they might be used in transient and temporary database files that SQLite sometimes generates for its own use. The meanings of these codes can shift from one release of SQLite to the next.
        // N≥12 & even	(N-12)/2	Value is a BLOB that is (N-12)/2 bytes in length.
        x if *x >= 12 && (*x % 2 == 0) => {
            let mut buf = vec![0_u8; (*x as usize - 12) / 2];
            c.read_exact(&mut buf[..]).unwrap();
            format!("{:?}", buf)
        }
        // N≥13 & odd	(N-13)/2	Value is a string in the text encoding and (N-13)/2 bytes in length. The nul terminator is not stored.
        x if *x >= 13 && (*x % 2 == 1) => {
            // TODO: avoid the copy somehow?
            let mut buf = vec![0_u8; (*x as usize - 13) / 2];
            c.read_exact(&mut buf[..]).unwrap();
            String::from_utf8(buf)
                .expect("Should have converted string to utf8")
        }
        _ => panic!("Unknown column type: {}", serial_type),
    }
}

#[test]
fn test_value_to_string() {
    // Null
    assert_eq!(value_to_string(&0, b""), "NULL".to_string());

    // one byte ints
    assert_eq!(value_to_string(&1, &[0x7f]), "127".to_string());
    assert_eq!(value_to_string(&1, &[0xff]), "-1".to_string());
    assert_eq!(value_to_string(&1, &[0x01]), "1".to_string());

    // two byte ints
    assert_eq!(value_to_string(&2, &[0x00, 0x7f]), "127".to_string());
    assert_eq!(value_to_string(&2, &[0xff, 0xff]), "-1".to_string());
    assert_eq!(value_to_string(&2, &[0x00, 0x01]), "1".to_string());
    assert_eq!(value_to_string(&2, &[0x01, 0x00]), "256".to_string());

    // three byte ints
    assert_eq!(value_to_string(&3, &[0x00, 0x00, 0x7f]), "127".to_string());
    assert_eq!(value_to_string(&3, &[0xff, 0xff, 0xff]), "-1".to_string());
    assert_eq!(value_to_string(&3, &[0x00, 0x00, 0x01]), "1".to_string());
    assert_eq!(value_to_string(&3, &[0x00, 0x01, 0x00]), "256".to_string());
    assert_eq!(
        value_to_string(&3, &[0x01, 0x00, 0x00]),
        "65536".to_string()
    );

    // TODO: larger ints and float.

    // Literal 0 and 1
    assert_eq!(value_to_string(&8, b""), "0".to_string());

    assert_eq!(value_to_string(&9, b""), "1".to_string());

    // Text of various lengths
    assert_eq!(value_to_string(&13, b""), "".to_string());

    assert_eq!(value_to_string(&19, b"Foo"), "Foo".to_string());

    assert_eq!(value_to_string(&25, b"FooBar"), "FooBar".to_string());

    // Blob
    assert_eq!(
        value_to_string(&18, &[0x00_u8, 0x01, 0xff]),
        "[0, 1, 255]".to_string()
    );
}

/// Convert a sqlite value in "serial type" format into Some(i64) or None if the type is unsuitable for conversion to i64.
///
///  # Arguments
/// * `serial_type` - A SQLite serial type code.
/// * `data` - A slice of bytes.
/// * `convert_nulls_to_zero`  - controls result when type is NULL.
///
/// If `convert_nulls_to_zero` is true, NULL results in a Zero value.  If false, NULL results in None.
/// If the type is f64, None is returned.
/// BLOB and TEXT always return NONE.
/// Panics on errors.
// TODO: handle errors better, by returning a Result instead of Option, with more detail on the error, and not panicing.
pub fn value_to_i64(serial_type: &i64, data: &[u8], convert_nulls_to_zero: bool) -> Option<i64> {
    let mut c = std::io::Cursor::new(data);
    match serial_type {
        // Serial Type	Content Size	Meaning
        // 0	        0	            Value is a NULL.
        0 => {
            if convert_nulls_to_zero {
                Some(0)
            } else {
                None
            }
        }
        // 1	        1	            Value is an 8-bit twos-complement integer.
        1 => Some(c.read_i8().unwrap() as i64),
        // 2	        2	            Value is a big-endian 16-bit twos-complement integer.
        2 => Some(c.read_i16::<BigEndian>().unwrap() as i64),
        // 3	        3	        Value is a big-endian 24-bit twos-complement integer.
        3 => {
            let mut bytes = [0_u8; 4];
            c.read_exact(&mut bytes[1..]).unwrap();
            bytes[0] = match (bytes[1] & 0b1000_0000) > 0 {
                false => 0,
                true => 0xff,
            };
            Some(i32::from_be_bytes(bytes) as i64)
        }
        // 4	        4	        Value is a big-endian 32-bit twos-complement integer.
        4 => Some(c.read_i32::<BigEndian>().unwrap() as i64),
        // 5	        6	        Value is a big-endian 48-bit twos-complement integer.
        5 => unimplemented!(),
        // 6	        8	        Value is a big-endian 64-bit twos-complement integer.
        6 => Some(c.read_i64::<BigEndian>().unwrap()),
        // 7	        8	        Value is a big-endian IEEE 754-2008 64-bit floating point number.
        7 => None,
        // 8	        0	        Value is the integer 0. (Only available for schema format 4 and higher.)
        8 => Some(0_i64),
        // 9	        0	        Value is the integer 1. (Only available for schema format 4 and higher.)
        9 => Some(1_i64),
        // 10,11	    variable	Reserved for internal use. These serial type codes will never appear in a well-formed database file, but they might be used in transient and temporary database files that SQLite sometimes generates for its own use. The meanings of these codes can shift from one release of SQLite to the next.
        // N≥12         variable    BLOB or TEXT
        _ => None,
    }
}

#[test]
fn test_value_to_i64() {
    // Null
    assert_eq!(value_to_i64(&0, b"", false), None);
    assert_eq!(value_to_i64(&0, b"", true), Some(0_i64));

    // one byte ints
    assert_eq!(value_to_i64(&1, &[0x7f], false), Some(127));
    assert_eq!(value_to_i64(&1, &[0xff], true), Some(-1));
    assert_eq!(value_to_i64(&1, &[0x01], false), Some(1));

    // two byte ints
    assert_eq!(value_to_i64(&2, &[0x00, 0x7f], false), Some(127));
    assert_eq!(value_to_i64(&2, &[0xff, 0xff], true), Some(-1));
    assert_eq!(value_to_i64(&2, &[0x00, 0x01], false), Some(1));
    assert_eq!(value_to_i64(&2, &[0x01, 0x00], true), Some(256));

    // three byte ints
    assert_eq!(value_to_i64(&3, &[0x00, 0x00, 0x7f], true), Some(127));
    assert_eq!(value_to_i64(&3, &[0xff, 0xff, 0xff], false), Some(-1));
    assert_eq!(value_to_i64(&3, &[0x00, 0x00, 0x01], true), Some(1));
    assert_eq!(value_to_i64(&3, &[0x00, 0x01, 0x00], false), Some(256));
    assert_eq!(value_to_i64(&3, &[0x01, 0x00, 0x00], true), Some(65536));

    // TODO: larger ints and float.

    // Literal 0 and 1
    assert_eq!(value_to_i64(&8, b"", false), Some(0));
    assert_eq!(value_to_i64(&8, b"", true), Some(0));

    assert_eq!(value_to_i64(&9, b"", false), Some(1));
    assert_eq!(value_to_i64(&9, b"", true), Some(1));

    // Text of various lengths
    assert_eq!(value_to_i64(&13, b"", false), None);
    assert_eq!(value_to_i64(&13, b"", true), None);

    assert_eq!(value_to_i64(&19, b"Foo", false), None);
    assert_eq!(value_to_i64(&19, b"Foo", true), None);

    // Blob
    assert_eq!(value_to_i64(&18, &[0x00_u8, 0x01, 0xff], false), None);
}
