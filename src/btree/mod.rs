/// SQLlite btrees come in two types: Tables and Indexes.  
/// Btree pages are either leaves or interior pages.
/// Each of these 4 combinations has a different cell format.
#[derive(Debug, Clone)]
pub enum PageType {
    IndexInterior,
    TableInterior,
    IndexLeaf,
    TableLeaf,
}

/// A b-tree page is divided into regions in the following order
/// 1 The 100-byte database file header (found on page 1 only)
/// 2 The 8 or 12 byte b-tree page header
/// 3 The cell pointer array
/// 4 Unallocated space
/// 5 The cell content area
/// 6 The reserved region.  (hope to assume always 0)

// SQLite row ids are 64b integers.
type RowId = i64;

/// module `header` defines types and methods for btree page headers.
pub mod header;
// module `leaf` provides an interator over the cells of the leaf pages of a table btree.
pub mod leaf;
// module `interior` provides an interator over the cells of the interior pages of a table btree.
pub mod interior;
// module `cell` provides an interator over the cells of a page, without interpreting what byte of cell they are.
/// It is used by `leaf` and `interior` modules.
pub mod cell;
