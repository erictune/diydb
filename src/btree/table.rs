//! table provides iterators over Table type btrees.  
//! It hides the fact that btrees span several pages.

use super::{cell, interior, leaf, PageType, RowId};
use crate::pager::PageNum;

// TODO: add table object: represents a Table (as opposed to Index) btree stored in SQLite format.
// pub struct Table<'a> {
//     root_page: crate::pager::PageNum,
//     pager: &'a crate::pager::Pager,
// }
//
// impl<'a> Table<'a> {
//     /// Returns an interator over all items in the Table.
//     pub fn iter() -> TableIterator<'a> {
//         unimplemented!();
//     }
//     pub fn get() -> Option<(i64, &'a [u8])> {
//         unimplemented!();
//     }
//     /// Returns an interator over items with keys between lo and hi.
//     /// TODO: use Exclusive/Inclusive like the std btree hashmap does.
//     pub fn range() -> TableIterator<'a> {
//         unimplemented!();
//     }
// }

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
    root_page: crate::pager::PageNum,
    pager: &'p crate::pager::Pager,
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
    /// TODO: Actually this is going to be a root page number.
    /// * `root_page` - The root page of the btree.  Borrowed for the lifetime of the iterator.  
    /// * `pager`     - A pager for the file that holds this btree.  
    pub fn new(root_page: crate::pager::PageNum, pager: &'p crate::pager::Pager) -> Iterator<'p> {
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
            // TODO: make it less complicated to just get the type of the new page you are about to work with.
            let hdr = super::header::check_header(page, Self::btree_start_offset(starting_page));
            let rmp = hdr.rightmost_pointer;
            let page_type = hdr.btree_page_type;
            match page_type {
                PageType::TableLeaf => {
                    self.stack
                        .push(EitherIter::Leaf(leaf::Iterator::new(cell::Iterator::new(
                            page,
                            Self::btree_start_offset(starting_page),
                            self.pager.get_page_size(),
                        ))));
                    return;
                }
                PageType::TableInterior => {
                    self.stack
                        .push(EitherIter::Interior(interior::ScanIterator::new(
                            cell::Iterator::new(
                                page,
                                Self::btree_start_offset(starting_page),
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

// TODO: if this is hard to make work, then test a simpler version that passes in a slice of ints and then iterates over them with references.

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
    let mut pager =
        crate::pager::Pager::open(path.as_str()).expect("Should have opened db with pager.");
    pager.initialize().expect("Should have initialized pager.");
    let x = crate::get_creation_sql_and_root_pagenum(&mut pager, "a");
    let mut ri = crate::new_table_iterator(&mut pager, x.unwrap().0);
    let first_item = ri.next().clone();
    assert!(first_item.is_some());
    assert_eq!(first_item.unwrap().0, 1);
    assert!(ri.next().is_none());
}
