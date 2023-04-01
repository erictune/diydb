use byteorder::{BigEndian};
use byteorder::ReadBytesExt;
use std::io::{Read, Seek};

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
/// * `data` - A reader (e.g. Cursor), pointing to the data to read.  The cursor will be advanced to the byte after the last
///            byte read.
///
/// The strings are not suitable for direct SQL query output, since the serial type needs to be converted to the
/// schema type.
/// 
/// Does not handle overflowing TEXT or BLOB.
/// 
/// Panics on errors. 
// TODO: handle errors better, by returning a Result.
pub fn read_value_to_string<R: Read + Seek>(serial_type: &i64, c: &mut R) -> String {
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
            bytes[0] = 1;
            c.read_exact(&mut bytes[1..]).unwrap();
            i32::from_be_bytes(bytes).to_string()
        },
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
        },
        // N≥13 & odd	(N-13)/2	Value is a string in the text encoding and (N-13)/2 bytes in length. The nul terminator is not stored.
        x if *x >= 13 && (*x % 2 == 1) => {
            // TODO: avoid the copy somehow?
            let mut buf = vec![0_u8; (*x as usize - 13) / 2];
            c.read_exact(&mut buf[..]).unwrap();
            String::from_utf8(buf)
                .expect("Should have converted string to utf8")
                .to_string()
        }
        _ => panic!("Unknown column type: {}", serial_type),
    }
}

#[test]
fn test_read_value_to_string() {

    // Null
    let mut c = std::io::Cursor::new(&b"");
    assert_eq!(read_value_to_string(&0, &mut c), "NULL".to_string());

    // one byte ints
    let mut c = std::io::Cursor::new(&[0x7f]);
    assert_eq!(read_value_to_string(&1, &mut c), "127".to_string());

    let mut c = std::io::Cursor::new(&[0xff]);
    assert_eq!(read_value_to_string(&1, &mut c), "-1".to_string());

    let mut c = std::io::Cursor::new(&[0x01]);
    assert_eq!(read_value_to_string(&1, &mut c), "1".to_string());

    // two byte ints
    let mut c = std::io::Cursor::new(&[0x00, 0x7f]);
    assert_eq!(read_value_to_string(&2, &mut c), "127".to_string());

    let mut c = std::io::Cursor::new(&[0xff, 0xff]);
    assert_eq!(read_value_to_string(&2, &mut c), "-1".to_string());

    let mut c = std::io::Cursor::new(&[0x00, 0x01]);
    assert_eq!(read_value_to_string(&2, &mut c), "1".to_string());
    
    let mut c = std::io::Cursor::new(&[0x01, 0x00]);
    assert_eq!(read_value_to_string(&2, &mut c), "256".to_string());

    // TODO: larger ints and float.

    // Literal 0 and 1
    let mut c = std::io::Cursor::new(&b"");
    assert_eq!(read_value_to_string(&8, &mut c), "0".to_string());

    let mut c = std::io::Cursor::new(&b"");
    assert_eq!(read_value_to_string(&9, &mut c), "1".to_string());

    // Text of various lengths
    let mut c = std::io::Cursor::new(&b"");
    assert_eq!(read_value_to_string(&13, &mut c), "".to_string());

    let mut c = std::io::Cursor::new(&b"Foo");
    assert_eq!(read_value_to_string(&19, &mut c), "Foo".to_string());

    let mut c = std::io::Cursor::new(&b"FooBar");
    assert_eq!(read_value_to_string(&25, &mut c), "FooBar".to_string());

    // Blob
    let mut c = std::io::Cursor::new(&[0x00_u8, 0x01, 0xff]);
    assert_eq!(read_value_to_string(&18, &mut c), "[0, 1, 255]".to_string());

}
