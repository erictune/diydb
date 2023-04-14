use byteorder::{BigEndian, ReadBytesExt};
use std::io::{Cursor};

use super::cell;
use super::RowId;

pub struct TableInteriorCellIterator<'a> {
    ci: cell::CellIterator<'a>,
}

impl<'a> TableInteriorCellIterator<'a> {
    /// Creates an iterator over the cells of a single page of a btree, with page of type TableLeaf.
    ///
    /// Iterator produces cells which are slices of bytes, which contain a record.
    ///
    /// # Arguments
    ///
    /// * `s` - A byte slice.  Borrowed for the lifetime of the iterator.  Slice begins with the record header length (a varint).
    ///         slives ends with the last byte of the record body.
    pub fn new(ci: cell::CellIterator) -> TableInteriorCellIterator {
        TableInteriorCellIterator { ci: ci }
    }
}

impl<'a> Iterator for TableInteriorCellIterator<'a> {
    // The iterator returns a tuple of (rowid, cell_payload).
    // Overflowing payloads are not supported.
    type Item = (crate::pager::PageNum, RowId);

    /// Returns the next item, which is a tuple of (k, v), where
    ///   `k` is a key, the row number (u64)
    ///   `v` is a left child page number.
    ///   All values in page v are less than or equal to k.
    fn next(&mut self) -> Option<Self::Item> {
        match self.ci.next() {
            None => None,
            Some(cell) => {
                // Table B-Tree Interior Cell (header 0x05):
                // A 4-byte big-endian page number which is the left child pointer.
                // A varint which is the integer key.
                println!("{:?}", cell);
                let mut c = Cursor::new(cell);
                let left_child_pagenum = c
                    .read_u32::<BigEndian>()
                    .expect("Should have read left child page number.")
                    as u32;
                let (key, _) = sqlite_varint::read_varint(&cell[4..]);
                Some((left_child_pagenum as crate::pager::PageNum, key as i64))
            }
        }
    }
}
