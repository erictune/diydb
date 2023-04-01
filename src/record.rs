// Btree Cells hold Records, which contain SQL rows.
// Each field in a row has a serial type which is not the same as the column SQL type, but varies by the value stored.

// TODO: consider defining enum types in serial_type.rs, which contains the values (as bytes in a fixed size array per type)?

pub struct HeaderIterator<'a> {
    // Borrow the byte slice
    data: &'a [u8],
    offset: usize,
    hdr_len: usize,
}

impl<'a> HeaderIterator<'a> {
    /// Creates an iterator over a slice of bytes in SQLite record format.
    ///
    /// Iterator produces i64s which are SQLite serial types numbers.
    /// See: https://www.sqlite.org/fileformat.html#record_format.
    /// 
    /// # Arguments
    ///
    /// * `s` - A byte slice.  Borrowed for the lifetime of the iterator.  Slice begins with the record header length (a varint).
    ///         slives ends with the last byte of the record body.
    /// 

    pub fn new(s: &[u8]) -> HeaderIterator {
        // "A record contains a header and a body, in that order.
        // The header begins with a single varint which determines the total number of bytes in the header"
        // - https://www.sqlite.org/fileformat.html#record_format
        let (hdr_len, hdr_len_len) = sqlite_varint::read_varint(s);
        
        HeaderIterator { 
            data: s, 
            offset: hdr_len_len, 
            hdr_len: hdr_len as usize,
        }
    }
}

impl<'a> Iterator for HeaderIterator<'a> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.hdr_len {
            return None;
        }
        let (serial_type, bytes_read) = sqlite_varint::read_varint(&self.data[self.offset..]);
        self.offset += bytes_read;
        // TODO: read the sqlite_varint::read_varint to see what it does if it want to read past the end of the slice.
        Some(serial_type)
    }
}



#[test]
fn test_header_iterator_literal_one() {
    // 2 byte record header, record type is literal 1, record body has zero bytes.
    let test_record: &[u8] = &[0x02, 0x09];

    let mut hi = HeaderIterator::new(&test_record);
    
    assert_eq!(hi.next(), Some(9));
    assert_eq!(hi.next(), None);
}

#[test]
fn test_header_iterator_five_one_byte_ints_value_ten() {
    // 06 0101 0101 010a 0a0a 0a0a
    let test_record: &[u8] = &[0x06, 0x01, 0x01, 0x01, 0x01, 0x01, 0x0a, 0x0a, 0x0a, 0x0a, 0x0a];

    let mut hi = HeaderIterator::new(&test_record);
    
    assert_eq!(hi.next(), Some(1));
    assert_eq!(hi.next(), Some(1));
    assert_eq!(hi.next(), Some(1));
    assert_eq!(hi.next(), Some(1));
    assert_eq!(hi.next(), Some(1));
    assert_eq!(hi.next(), None);
}

#[test]
fn test_header_iterator_various_types() {
    // 0608 0907 1300 4009 21ca c083 126f 5465 6e
    // literal 0 | literal 1 | float 3.1415 | "Ten" |
    let test_record: &[u8] = &[0x06, 0x08, 0x09, 0x07, 0x13, 0x00, 0x40, 0x09, 0x21, 0xca, 0xc0, 0x83, 0x12, 0x6f, 0x54, 0x65, 0x6e];

    let mut hi = HeaderIterator::new(&test_record);
    
    assert_eq!(hi.next(), Some(8));    // Literal 0
    assert_eq!(hi.next(), Some(9));    // Literal 1
    assert_eq!(hi.next(), Some(7));    // Float 64
    assert_eq!(hi.next(), Some(0x13)); // String of length 3; (19-13)/2 = 3
    assert_eq!(hi.next(), Some(0));    // NULL
    assert_eq!(hi.next(), None);
}

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
    /// See: https://www.sqlite.org/fileformat.html#record_format.
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
    // The iterator returns a reference to each item the record as as a byte slice the value in the data
    type Item = (i64, &'a [u8]);

    /// Returns the next item, which is a tuple of (type, &[u8] - a reference to a slice of bytes for this value).
    ///
    /// # Arguments
    ///
    /// * `s` - A byte slice.  Borrowed for the lifetime of the iterator.  Slice begins with the record header length (a varint).
    ///         and ends with the last byte of the record body.
    ///
    fn next(&mut self) -> Option<Self::Item> {
        if self.hdr_offset >= self.hdr_len {
            return None;
        }
        let (serial_type, bytes_read) = sqlite_varint::read_varint(&self.data[self.hdr_offset..]);
        self.hdr_offset += bytes_read;
        let value_len = match serial_type {
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
            x if x >= 12  => (x as usize - 12 - (x % 2) as usize)/ 2,
            _ => unimplemented!(),
        };
        let old_value_offset = self.value_offset;
        self.value_offset += value_len;
        Some((serial_type, &self.data[old_value_offset..old_value_offset+value_len]))
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
    let test_record: &[u8] = &[0x06, 0x01, 0x01, 0x01, 0x01, 0x01, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e];

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
    let test_record: &[u8] = &[0x06, 0x08, 0x09, 0x07, 0x13, 0x00, 0x40, 0x09, 0x21, 0xca, 0xc0, 0x83, 0x12, 0x6f, 0x54, 0x65, 0x6e];

    let mut hi = ValueIterator::new(&test_record);
    
    assert_eq!(hi.next(), Some((8,      &[][..])));                                                 // Literal 0
    assert_eq!(hi.next(), Some((9,      &[][..])));                                                 // Literal 1
    assert_eq!(hi.next(), Some((7,      &[0x40, 0x09, 0x21, 0xca, 0xc0, 0x83, 0x12, 0x6f][..])));   // Float 64
    assert_eq!(hi.next(), Some((0x13,   &b"Ten"[..])));                                             // String of length 3; (19-13)/2 = 3
    assert_eq!(hi.next(), Some((0, &[][..])));                                                      // NULL
    assert_eq!(hi.next(), None);
}
