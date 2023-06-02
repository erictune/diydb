//! Btree Cells hold Records, which contain SQL rows.
//! Each field in a row has a serial type which is not the same as the column SQL type, but varies by the value stored.

use crate::serial_type;

pub struct ValueIterator<'a> {
    // Borrow the byte slice
    data: &'a [u8],
    hdr_offset: usize,
    hdr_len: usize,
    value_offset: usize,
}

impl<'a> ValueIterator<'a> {
    /// Creates an iterator over a slice of bytes in SQLite record format.
    ///
    /// Iterator produces tuples (t, bs).
    ///
    /// `t` is a SQLite serial type code
    /// See: <https://www.sqlite.org/fileformat.html#record_format>
    ///
    /// `bs` is byte slice accessing the value, valid for the lifetime of the iterator.
    ///
    /// # Arguments
    ///
    /// * `s` - A byte slice.  Borrowed for the lifetime of the iterator.  Slice begins with the record header length (a varint).
    ///         slives ends with the last byte of the record body.
    ///

    pub fn new(s: &[u8]) -> ValueIterator {
        // "A record contains a header and a body, in that order.
        // The header begins with a single varint which determines the total number of bytes in the header"
        // - https://www.sqlite.org/fileformat.html#record_format
        let (hdr_len, hdr_len_len) = sqlite_varint::read_varint(s);
        ValueIterator {
            data: s,
            hdr_offset: hdr_len_len,
            hdr_len: hdr_len as usize,
            value_offset: hdr_len as usize,
        }
    }
}

impl<'a> Iterator for ValueIterator<'a> {
    // The iterator returns a reference to each item in the record as as a byte slice the value in the data
    type Item = (i64, &'a [u8]);

    /// Returns the next item, which is a tuple of (type, &[u8] - a reference to a slice of bytes for this value).
    ///
    fn next(&mut self) -> Option<Self::Item> {
        if self.hdr_offset >= self.hdr_len {
            return None;
        }
        let (serial_type, bytes_read) = sqlite_varint::read_varint(&self.data[self.hdr_offset..]);
        self.hdr_offset += bytes_read;
        let value_len = serial_type::serialized_size(serial_type);
        let old_value_offset = self.value_offset;
        self.value_offset += value_len;
        Some((
            serial_type,
            &self.data[old_value_offset..old_value_offset + value_len],
        ))
    }
}

#[test]
fn test_value_iterator_one_byte_int() {
    // 2 byte record header, record type is literal 1 (09), record body has zero bytes.
    let test_record: &[u8] = &[0x02, 0x09];

    let mut hi = ValueIterator::new(&test_record);
    assert_eq!(hi.next(), Some((9, &[][..])));
    assert_eq!(hi.next(), None);
}

#[test]
fn test_value_iterator_five_one_byte_ints_value_ten_to_fourteen() {
    let test_record: &[u8] = &[
        0x06, 0x01, 0x01, 0x01, 0x01, 0x01, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
    ];

    let mut hi = ValueIterator::new(&test_record);

    assert_eq!(hi.next(), Some((1, &[10][..])));
    assert_eq!(hi.next(), Some((1, &[11][..])));
    assert_eq!(hi.next(), Some((1, &[12][..])));
    assert_eq!(hi.next(), Some((1, &[13][..])));
    assert_eq!(hi.next(), Some((1, &[14][..])));
    assert_eq!(hi.next(), None);
}

#[test]
fn test_value_iterator_various_types() {
    // literal 0 | literal 1 | float 3.1415 | "Ten" | NULL
    let test_record: &[u8] = &[
        0x06, 0x08, 0x09, 0x07, 0x13, 0x00, 0x40, 0x09, 0x21, 0xca, 0xc0, 0x83, 0x12, 0x6f, 0x54,
        0x65, 0x6e,
    ];

    let mut hi = ValueIterator::new(&test_record);

    assert_eq!(hi.next(), Some((8, &[][..]))); // Literal 0
    assert_eq!(hi.next(), Some((9, &[][..]))); // Literal 1
    assert_eq!(
        hi.next(),
        Some((7, &[0x40, 0x09, 0x21, 0xca, 0xc0, 0x83, 0x12, 0x6f][..]))
    ); // Float 64
    assert_eq!(hi.next(), Some((0x13, &b"Ten"[..]))); // String of length 3; (19-13)/2 = 3
    assert_eq!(hi.next(), Some((0, &[][..]))); // NULL
    assert_eq!(hi.next(), None);
}