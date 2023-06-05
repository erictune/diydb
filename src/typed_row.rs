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
    Deserialization {
        detail: crate::serial_type::Error,
        colnum: usize,
    },
    #[error("Casting error, column number {}, detail : {}", colnum, detail)]
    Casting {
        detail: crate::serial_type::Error,
        colnum: usize,
    },
    #[error("Type array and value array length mismatch.")]
    ArrayLenMismatch,
    #[error("Serialization error, column number {}, detail : {}", colnum, detail)]
    Serialization {
        detail: crate::serial_type::Error,
        colnum: usize,
    },
    #[error("Header length longer than supported.")]
    HeaderTooBig,
    #[error("Not enough space in target to hold serialized data.")]
    NotEnoughSpace,

}

// TODO: if this took a Row, and Row held the RowID, then the error messages could provide the rowid where the error occured.
pub fn from_serialized(column_types: &Vec<SqlType>, record: &[u8]) -> Result<Row, Error> {
    use crate::record::ValueIterator;
    let mut ret: Vec<SqlValue> = vec![];
    for (colnum, (serty, bytes)) in ValueIterator::new(record).enumerate() {
        if colnum > column_types.len() {
            return Err(Error::ArrayLenMismatch);
        }
        let v = crate::serial_type::to_sql_value(&serty, bytes)
            .map_err(|detail| Error::Deserialization { colnum, detail })?;
        let v = crate::serial_type::cast_to_schema_type(&v, column_types[colnum])
            .map_err(|detail| Error::Casting { colnum, detail })?;
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

// TODO: rationalize why all of the serialization is in this file, but the deserialization is split between this file and record.rs.

/// Write a row of SqlValues into a row record in SQLite record format.
///
/// Note: Call this after downcasting `row` to be storage types.
///
/// # Arguments
///
/// * `row` - a Row (vector of SqlValues).
/// * `buf` - An empty (zeroed) byte slice with sufficient space to hold the whole row.  Borrowed for the lifetime of the writer.  
///
/// # Returns
///
/// `Err(_)` when there is not enough space to hold the values to be serialized (overflow of strings and blobs is not supported.)
/// `Ok((len))`, where len is the number of bytes of record  written.
///
/// # Postconditions
///
/// * `buf` contains the row's record header and record data, in the range `buf[0 .. len]`.  The remaining space is zeros, namely the range `buf[len .. buf.len()]`.
///   In the case of an error, `buf` is zeroed (though it may have been modified before the writer discovered that there was not enough space.)
pub fn to_serialized<'a>(row: &Row, buf: &'a mut [u8]) -> Result<usize, Error> {
    use sqlite_varint::serialize_to_varint;
    
    // "A record contains a header and a body, in that order.
    // The header begins with a single varint which determines the total number of bytes in the header"
    // - https://www.sqlite.org/fileformat.html#record_format

    let mut header: Vec<u8> = vec![];
    let mut body: Vec<u8> = vec![];

    for (colnum, v) in row.items.iter().enumerate() {
        let (data, code, _) = crate::serial_type::to_serial_type(v).map_err(|detail| Error::Serialization{detail, colnum})?;
        header.append(&mut serialize_to_varint(code));

        body.append(&mut data.clone());
    }
    // For simplicitly assume that there are not so many cells  that the varint with the header length will become 2 bytes.
    let header_len = 1 /* encoded length */ + header.len();
    let encoded_header_len = serialize_to_varint(1_i64 /* encoded length */ + header.len() as i64);
    if encoded_header_len.len() != 1 {
        return Err(Error::HeaderTooBig);  // This will happen with 127 short columns, or with fewer long strings.
    }
    //println!("buf.len() = {}, header_len = {}, body.len() = {}", buf.len(), header_len, body.len());
    if buf.len() < header_len + body.len() {
        return Err(Error::NotEnoughSpace)
    }
    let start = buf.len() - header_len - body.len();
    buf[start] = encoded_header_len[0];
    buf[start+1 .. start+1+header.len()].clone_from_slice(&mut header);
    buf[start+1+header.len() .. start+1+header.len()+body.len()].clone_from_slice(&mut body);
    Ok(1+header.len()+body.len())
}

#[test]
fn test_to_serialized() {
    use crate::sql_value::SqlValue::*;
    let cases = vec![
        // 2 byte record header, record type is literal 1 (09), record body has zero bytes.
        (
            vec![Int(1)],
            [
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x02, 0x09,
            ],
            2,
        ),
        // ints 10..=15
        (
            vec![Int(10), Int(11), Int(12), Int(13), Int(14)],
            [
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x01, 0x01, 0x01, 0x01,
                0x01, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,  
            ],
            11,
        ),
        // literal 0 | literal 1 | float 3.1415 | "Ten" | NULL
        (
            vec![
                Int(0),
                Int(1),
                Real(3.1415),
                Text("Ten".to_string()),
                Null(),
            ],
            [
                0x00, 0x06, 0x08, 0x09, 0x07, 0x13, 0x00, 0x40, 0x09, 0x21, 0xca, 0xc0,
                0x83, 0x12, 0x6f, 0x54, 0x65, 0x6e
            ],
            17,
        ),
        // A 16 byte string barely fits.
        (
            vec![
                Text("123456789TETTFFS".to_string())
            ],
            [
                0x02, 16*2+13, b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'T',
                b'E', b'T', b'T', b'F', b'F', b'S',
            ],
            18
        ),        
    ];
    let mut casenum = 1;
    let numcases = cases.len();
    for case in cases {
        println!("Case {} of {}", casenum, numcases);
        let mut buf = [0_u8; 18];
        let res = to_serialized(&Row{ items: case.0 }, &mut buf);
        assert!(res.is_ok());
        let bytes_added = res.unwrap();
        assert_eq!(buf, case.1);
        assert_eq!(bytes_added, case.2);
        casenum += 1;
    }
}

#[test]
fn test_to_serialized_errors() {
    use crate::sql_value::SqlValue::*;
    let cases = vec![
        (
            // Twenty zeros, at one byte each, don't fit.
            vec![Int(0); 20],
            [
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
            ],
        ),
        (
            // A 17 byte string won't fit, and the array is still zeroed.
            vec![Text("123456789TETTFFSS".to_string())],
            [
                0x00, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
            ],
        ),
    ];
    for case in cases {
        let mut buf = [0_u8; 18];
        let result = to_serialized(&Row{ items: case.0}, &mut buf);
        assert!(result.is_err());
    }
}
