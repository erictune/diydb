use super::cell;
use super::RowId;

pub struct Iterator<'a> {
    ci: cell::Iterator<'a>,
}

impl<'a> Iterator<'a> {
    /// Creates an iterator over the cells of a single page of a btree, with page of type TableLeaf.
    ///
    /// Iterator produces cells which are slices of bytes, which contain a record.
    ///
    /// # Arguments
    ///
    /// * `s` - A byte slice.  Borrowed for the lifetime of the iterator.  Slice begins with the record header length (a varint).
    ///         slives ends with the last byte of the record body.
    pub fn new(ci: cell::Iterator) -> Iterator {
        Iterator { ci: ci }
    }
}

impl<'a> core::iter::Iterator for Iterator<'a> {
    // The iterator returns a tuple of (rowid, cell_payload).
    // Overflowing payloads are not supported.
    type Item = (RowId, &'a [u8]);

    /// Returns the next item, which is a tuple of (k, v), where
    ///   `k` is a key, the row number (u64)
    ///   `v` is a value, &[u8].
    fn next(&mut self) -> Option<Self::Item> {
        match self.ci.next() {
            None => None,
            Some(cell) => {
                let mut offset = 0;
                let (payload_len, bytesread) = sqlite_varint::read_varint(cell);
                offset += bytesread;
                let (rowid, bytesread2) = sqlite_varint::read_varint(&cell[offset..]);
                offset += bytesread2;
                if cell.len() - offset != (payload_len as usize) {
                    unimplemented!("Spilled payloads not implemented.");
                }
                //let payload = &cell[offset..].to_vec();
                //println!("payload bytes {:?}", &payload);
                Some((rowid as RowId, &cell[offset..]))
            }
        }
    }
}
