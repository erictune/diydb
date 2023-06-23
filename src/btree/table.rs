//! table provides iterators over Table type btrees.  
//! It hides the fact that btrees span several pages.

use super::{cell, interior, leaf, PageType, RowId};
use crate::stored_db::PageNum;

enum EitherIter<'z> {
    Leaf(super::leaf::Iterator<'z>),
    Interior(super::interior::ScanIterator<'z>),
}

impl<'z> EitherIter<'z> {
    #[allow(dead_code)] // Use for SearchIterator
    pub fn unwrap_leaf(&mut self) -> &mut super::leaf::Iterator<'z> {
        match self {
            EitherIter::Leaf(l) => l,
            EitherIter::Interior(_) => panic!("Incorrect enum variant in unwrap_leaf"),
        }
    }
    pub fn unwrap_interior(&mut self) -> &mut super::interior::ScanIterator<'z> {
        match self {
            EitherIter::Leaf(_) => panic!("Incorrect enum variant in unwrap_interior"),
            EitherIter::Interior(i) => i,
        }
    }
}

pub struct Iterator<'p> {
    root_page: crate::stored_db::PageNum,
    pager: &'p crate::stored_db::StoredDb,
    stack: Vec<EitherIter<'p>>, // The lifetime of the references in the inner iterators is good as long as the pager is, since the pager holds the pages.
    page_size: u32,
}

impl<'p> Iterator<'p> {
    /// Creates an iterator over the records of a Table-typed btree.
    ///
    /// Iterator produces cells which are slices of bytes, which contain a record.  
    /// The called needs to interpret the record as a database row.
    ///
    /// When you call new, the iterator does an in-order traversal of the table and records
    /// all the page numbers it needs during its scan.  
    ///
    /// # Arguments
    ///
    /// * `root_page` - The root page of the btree.  Borrowed for the lifetime of the iterator.  
    /// * `pager`     - A pager for the file that holds this btree.  
    pub fn new(root_page: crate::stored_db::PageNum, pager: &'p crate::stored_db::StoredDb) -> Iterator<'p> {
        // We will traverse the tree during the constructor to get a list of pages we need access to
        // during the iteration (excluding overflow pages).  This approach avoids having a stack of
        // iterators which are multiple borrows against  approach avoids having page references
        //  during the iteration phase, which allows the next() c

        let pgsz = pager.get_page_size();
        Iterator {
            root_page,
            pager,
            stack: vec![],
            page_size: pgsz,
        }
    }

    fn btree_start_offset(pgnum: usize) -> usize {
        match pgnum {
            1 => 100,
            _ => 0,
        }
    }

    fn seek_leftmost_leaf(&mut self, starting_page: PageNum) {
        let mut next_page = starting_page;
        loop {
            let page = self.pager.get_page_ro(next_page).unwrap();
            // TODO: if the borrow checker gets confused by this loop, then the stack could be made to
            // have a maximum height, e.g. 12, given that there are at most 2^64 pages and it is balanced.
            let hdr = super::header::check_header(page, Self::btree_start_offset(next_page));
            let rmp = hdr.rightmost_pointer;
            let page_type = hdr.btree_page_type;
            match page_type {
                PageType::TableLeaf => {
                    self.stack
                        .push(EitherIter::Leaf(leaf::Iterator::new(cell::Iterator::new(
                            page,
                            Self::btree_start_offset(next_page),
                            self.pager.get_page_size(),
                        ))));
                    return;
                }
                PageType::TableInterior => {
                    self.stack
                        .push(EitherIter::Interior(interior::ScanIterator::new(
                            cell::Iterator::new(
                                page,
                                Self::btree_start_offset(next_page),
                                self.page_size,
                            ),
                            rmp.expect("Interior pages should always have rightmost pointer.")
                                as usize,
                        )));
                    let top_of_stack_iter = self.stack.last_mut().unwrap();
                    next_page = top_of_stack_iter
                        .unwrap_interior()
                        .next()
                        .expect("Interior page should have at least 1 child always");
                }
                PageType::IndexInterior | PageType::IndexLeaf => {
                    unreachable!("Should not have index pages in table btree.");
                }
            }
        }
    }
}

impl<'p> core::iter::Iterator for Iterator<'p> {
    // The iterator returns a tuple of (rowid, cell_payload).
    // Overflowing payloads are not supported.
    type Item = (RowId, &'p [u8]);

    /// Returns the next item, which is a tuple of (k, v), where
    ///   `k` is a key, the row number (u64)
    ///   `v` is a value, &[u8].
    fn next(&mut self) -> Option<Self::Item> {
        if self.stack.is_empty() {
            self.seek_leftmost_leaf(self.root_page)
        }
        assert!(!self.stack.is_empty(), "Internal logical error");
        while !self.stack.is_empty() {
            match self.stack.last_mut().unwrap() {
                EitherIter::Leaf(l) => match l.next() {
                    // When we are iterating over a leaf and aren't done, return items from the leaf.
                    Some(x) => return Some(x),
                    // When we are iterating over a leaf and finish done, go up to the previous interior page, if any.
                    // We will process that on the next iteration of the loop.
                    None => {
                        self.stack.pop().unwrap();
                        continue;
                    }
                },
                EitherIter::Interior(i) => match i.next() {
                    // When we are still iterating on in an interior page, explore down the next child pointer to a leaf.
                    Some(x) => {
                        self.seek_leftmost_leaf(x);
                        continue;
                    }
                    // If we ran out of items on an interior page, go up to its parent.
                    None => {
                        self.stack.pop();
                        continue;
                    }
                },
            }
        }
        None
    }
}

#[cfg(test)]
fn path_to_testdata(filename: &str) -> String {
    std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set")
        + "/resources/test/"
        + filename
}

#[test]
fn test_table_iterator_on_minimal_db() {
    let path = path_to_testdata("minimal.db");
    let db =
        crate::stored_db::StoredDb::open(path.as_str()).expect("Should have opened db with pager.");
    let pgnum = db.get_root_pagenum("a").expect("Should have gotten page number.");
    let pager = db;
    let mut ri = crate::new_table_iterator(&pager, pgnum);
    let first_item = ri.next().clone();
    assert!(first_item.is_some());
    assert_eq!(first_item.unwrap().0, 1);
    assert!(ri.next().is_none());
}

#[test]
fn test_table_iterator_on_three_level_db() {
    // This tests iterating over a btree of three levels (root, non-root interior pages, leaf pages).
    // The table has these rows:
    // row 1: 1
    // row 1000000: 1000000
    let path = path_to_testdata("threelevel.db");
    let db =
        crate::stored_db::StoredDb::open(path.as_str()).expect("Should have opened db with pager.");
    let pgnum = db.get_root_pagenum("t").expect("Should have found root pagenum.");
    let pager = db;
    let ri = crate::new_table_iterator(&pager, pgnum);
    let mut last_rowid = 0;
    for e in ri.enumerate() {
        let (expected, (rowid, _)) = e;
        println!("Visiting rowid {} on iteration {}", rowid, expected);
        assert_eq!(expected + 1, rowid as usize);
        last_rowid = rowid
    }
    assert_eq!(last_rowid, 100000);
}
