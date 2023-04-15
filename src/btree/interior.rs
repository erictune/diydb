use byteorder::{BigEndian, ReadBytesExt};
use std::io::Cursor;

use super::cell;
use super::RowId;
use crate::pager::PageNum;

/// Iterator over the values and child pointers of a btree interior page.
/// Intended for searching for a specific value or range.
/// Produces a tuple (left_child_pagenum, value, right_child_pagenum).
pub struct SearchIterator<'a> {
    ci: cell::Iterator<'a>,
    // TODO: implement this.
    // note it is rarely possible for there to not be two child pointers on page 1.  but IIUC, there is always a rightmost, so there is always 
    // a right and a left to return.
}

/// Iterator over the child pointers of a btree interior page.
/// Intended for use in full scans.
/// Produces child page numbers.
pub struct ScanIterator<'a> {
    ci: cell::Iterator<'a>,
    returned_rightmost: bool,
    rightmost_pointer: PageNum,
}

impl<'a> SearchIterator<'a> {
    /// Creates an iterator over the cells of a single page of a btree, with page of type TableLeaf.
    ///
    /// # Arguments
    ///
    /// * `ci` - A cell iterator for the page. Borrowed for the lifetime of the iterator.  
    pub fn new(ci: cell::Iterator) -> SearchIterator {
        SearchIterator { ci: ci }
    }
}

impl<'a> ScanIterator<'a> {
    /// Creates an iterator over the cells of a single page of a btree, with page of type TableLeaf.
    ///
    /// # Arguments
    ///
    /// * `ci` - A cell iterator for the page. Borrowed for the lifetime of the iterator. 
    /// * `rmp` - The rightmost pointer for this page. 
    pub fn new(ci: cell::Iterator, rmp: PageNum) -> ScanIterator {
        ScanIterator { ci: ci, returned_rightmost: false, rightmost_pointer: rmp }
    }
}

impl<'a> core::iter::Iterator for SearchIterator<'a> {
    type Item = (PageNum, RowId, PageNum);

    /// Returns the next item, which is a tuple of (lc, v, rc), where
    ///   `lc` is the page number of the left child.
    ///   `v` is the row number (u64).
    ///   `rc` is the page number of the right child.
    ///   All values in page lc are less than or equal to v.
    ///   All values in page rc are greater than v.
    fn next(&mut self) -> Option<Self::Item> {
        match self.ci.next() {
            None => {
                unimplemented!();
            }
            Some(cell) => {
                let mut c = Cursor::new(cell);
                let _ = c
                    .read_u32::<BigEndian>()
                    .expect("Should have read left child page number.")
                    as u32;
                let (_, _) = sqlite_varint::read_varint(&cell[4..]);
                unimplemented!();
            }
        }
    }
}

impl<'a> core::iter::Iterator for ScanIterator<'a> {
    // The iterator returns a tuple of (rowid, cell_payload).
    // Overflowing payloads are not supported.
    type Item = PageNum;

    /// Returns the next item, which is a tuple of (k, v), where
    ///   `k` is a key, the row number (u64)
    ///   `v` is a left child page number.
    ///   All values in page v are less than or equal to k.
    fn next(&mut self) -> Option<Self::Item> {
        if self.returned_rightmost { return None }
        match self.ci.next() {
            None => {
                self.returned_rightmost = true; 
                Some(self.rightmost_pointer)
            }
            Some(cell) => {
                // Table B-Tree Interior Cell (header 0x05):
                // A 4-byte big-endian page number which is the left child pointer.
                // A varint which is the integer key.
                let mut c = Cursor::new(cell);
                let left_child_pagenum = c
                    .read_u32::<BigEndian>()
                    .expect("Should have read left child page number.")
                    as u32;
                Some(left_child_pagenum as crate::pager::PageNum)
            }
        }
    }
}
