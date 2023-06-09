use byteorder::{BigEndian, ReadBytesExt};
use std::io::Cursor;

use super::cell;
use super::RowId;
use crate::stored_db::PageNum;

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
    #[allow(dead_code)] // Use to build lookup by rowid as part of using indexes.
    pub fn new(ci: cell::Iterator) -> SearchIterator {
        SearchIterator { ci }
    }
}

impl<'a> ScanIterator<'a> {

    fn btree_start_offset(pgnum: usize) -> usize {
        match pgnum {
            1 => 100,
            _ => 0,
        }
    }

    /// Creates an iterator over the cells of a single page of a btree, with page of type TableLeaf.
    ///
    /// # Arguments
    ///
    /// * `ci` - A cell iterator for the page. Borrowed for the lifetime of the iterator.
    /// * `rmp` - The rightmost pointer for this page.
    pub fn new(pager: &crate::stored_db::StoredDb, pgnum: usize) -> ScanIterator {
        let page = pager.get_page_ro(pgnum).unwrap();
        let hdr = super::header::check_header(page, Self::btree_start_offset(pgnum));

        let ci = cell::Iterator::new(
            page,
            Self::btree_start_offset(pgnum),
            pager.get_page_size()
        );
        ScanIterator {
            ci,
            returned_rightmost: false,
            rightmost_pointer: hdr.rightmost_pointer.expect("Interior pages should always have rightmost pointer.") as usize,
        }
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
    #[allow(dead_code)] // Use to build lookup by rowid as part of using indexes.
    fn next(&mut self) -> Option<Self::Item> {
        match self.ci.next() {
            None => {
                unimplemented!();
            }
            Some(cell) => {
                let mut c = Cursor::new(cell);
                let _ = c
                    .read_u32::<BigEndian>()
                    .expect("Should have read left child page number.");
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
        if self.returned_rightmost {
            return None;
        }
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
                    .expect("Should have read left child page number.");
                Some(left_child_pagenum as crate::stored_db::PageNum)
            }
        }
    }
}

#[cfg(test)]
fn path_to_testdata(filename: &str) -> String {
    std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set")
        + "/resources/test/"
        + filename
}

#[test]
fn test_interior_iterator_on_multipage_db() {
    // This tests iterating over the root page which is interior type.
    // The table has these rows:
    // row 1: "AAA"
    // row 2: "AAB"
    // ...
    // row 1000: "JJJ"
    //
    // The file has 4 x 4k pages:
    // Page 1: schema
    // Page 2: root of "digits" table.
    // Page 3: Index type page.
    // Page 4: first leaf page (AAA to DFA ; rows 1-351)
    // Page 5: second leaf page (DFB to GJA ; rows 352-691)
    // Page 6: third leaf page (GJB to JJJ ; 692-1000)
    let path = path_to_testdata("multipage.db");
    let db =
        crate::stored_db::StoredDb::open(path.as_str()).expect("Should have opened pager for db {path}.");
    let pgnum = db.get_root_pagenum("thousandrows").expect("Should have looked up table.");
    assert_eq!(pgnum, 3);
    let pgr = db;
    
    use crate::btree;
    let page = pgr.get_page_ro(pgnum)
        .unwrap_or_else(|e| panic!("Error loading db page #{} : {}", pgnum, e));
    let btree_start_offset = match pgnum {
        1 => 100,
        _ => 0,
    };
    let hdr = super::header::check_header(page, btree_start_offset);
    println!("Examining page {} with header {:?}", pgnum, hdr);

    let mut ri: ScanIterator<'_> = match hdr.btree_page_type {
        btree::PageType::TableInterior => btree::interior::ScanIterator::new(
            &pgr, pgnum),
        _ => {
            unreachable!();
        }
    };

    assert_eq!(ri.next(), Some(4));
    assert_eq!(ri.next(), Some(5));
    assert_eq!(ri.next(), Some(6));
    assert_eq!(ri.next(), None);
}
