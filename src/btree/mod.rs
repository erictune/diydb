//! Btree provides iterators over tables stored in SQLlite btrees.
//! SQLlite btrees come in two types: Tables and Indexes.    Indexes are not implemented yet.
//! Btree pages are either leaves or interior pages.
//! Each of these 4 combinations has a different cell format.
#[derive(Debug, Clone)]
pub enum PageType {
    IndexInterior,
    TableInterior,
    IndexLeaf,
    TableLeaf,
}

// SQLite row ids are 64b integers.
type RowId = i64;

/// Organization of btree submodules and types:
/// *  `pub table::Iterator` iterates over all the pages of one btree.
/// *  `pub table::Iterator` uses either `leaf::Iterator` or `interior::ScanIterator` on a given page.
/// *  `leaf::Iterator` or `interior::ScanIterator`  use `cell::Iterator` to iterate over the cells on a page.

/// module `table` defines iterators over btrees.
pub mod table;
/// module `header` defines types and methods for btree page headers.
pub mod header;
// module `leaf` provides an interator over the cells of the leaf pages of a table btree.
mod leaf;
// module `interior` provides an interator over the cells of the interior pages of a table btree.
mod interior;
// module `cell` provides an interator over the cells of a page, without interpreting what byte of cell they are.
/// It is used by `leaf` and `interior` modules.
mod cell;
