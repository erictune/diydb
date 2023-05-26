//! cell::Iterator ierates over the cells in a btree page.

use byteorder::{BigEndian, ReadBytesExt};
use std::io::{Cursor, Seek, SeekFrom};

use super::PageType;

/// Iterator over cells within a page, without interpreting the cell contents.
pub struct Iterator<'a> {
    page: &'a Vec<u8>,
    cell_idx: usize,
    cell_offsets: Vec<usize>,
    cell_lengths: Vec<usize>,
}

impl<'a> Iterator<'a> {
    /// Creates an iterator over the cells of a single page of a btree.
    ///
    /// Iterator produces cells which are slices of bytes, which contain a record.
    ///
    /// # Arguments
    ///
    /// * `s` - A byte slice.  Borrowed for the lifetime of the iterator.  Slice begins with the record header length (a varint).
    ///         slives ends with the last byte of the record body.
    pub fn new(p: &Vec<u8>, non_btree_header_bytes: usize, page_size: u32) -> Iterator {
        let mut c = Cursor::new(p);
        c.seek(SeekFrom::Start(non_btree_header_bytes as u64))
            .expect("Should have seeked.");
        let btree_page_type = match c.read_u8().expect("Should have read btree header") {
            0x02 => PageType::IndexInterior,
            0x05 => PageType::TableInterior,
            0x0a => PageType::IndexLeaf,
            0x0d => PageType::TableLeaf,
            b => panic!("Invalid Btree Page Type: {}", b ),
        };
        c.seek(SeekFrom::Start(3 + non_btree_header_bytes as u64))
            .expect("Should have seeked.");
        let num_cells: u32 = c
            .read_u16::<BigEndian>()
            .expect("Should have read btree header") as u32;

        let btree_header_bytes = match btree_page_type {
            PageType::IndexInterior | PageType::TableInterior => 12,
            PageType::IndexLeaf | PageType::TableLeaf => 8,
        };
        c.seek(SeekFrom::Start(
            btree_header_bytes + non_btree_header_bytes as u64,
        ))
        .expect("Should have seeked to cell offset.");

        let mut it = Iterator {
            page: p,
            cell_idx: 0,
            cell_offsets: Vec::new(),
            cell_lengths: Vec::new(),
        };

        // Read the cell pointer array:
        // """
        // The cell pointer array of a b-tree page immediately follows the b-tree page header.
        // Let K be the number of cells on the btree. The cell pointer array consists of K 2-byte
        // integer offsets to the cell contents. The cell pointers are arranged in key order with
        // left-most cell (the cell with the smallest key) first and the right-most cell (the cell
        // with the largest key) last.
        // """()
        let mut last_offset: usize = page_size as usize; // First cell in pointer list is the last cell on the page, so it ends on byte PAGESIZE, I think (?).
        for _ in 0..num_cells {
            let off = c
                .read_u16::<BigEndian>()
                .expect("Should have read cell pointer") as usize;
            it.cell_offsets.push(off);
            it.cell_lengths.push(last_offset - off);
            last_offset = off;
        }
        it
    }
}

impl<'a> core::iter::Iterator for Iterator<'a> {
    // The iterator returns a reference to a cell (&[u8]).  The format of the data in the cell
    // is dependent on the type of the btree page.
    type Item = &'a [u8];

    /// Returns the next item, which is a &[u8], the slice of bytes containing the contents of the cell.
    fn next(&mut self) -> Option<Self::Item> {
        if self.cell_idx >= self.cell_offsets.len() {
            return None;
        }
        let mut c = Cursor::new(self.page);
        c.seek(SeekFrom::Start(self.cell_offsets[self.cell_idx] as u64))
            .expect("Should have seeked to cell offset.");
        let b = self.cell_offsets[self.cell_idx];
        let e = b + self.cell_lengths[self.cell_idx];
        self.cell_idx += 1;
        Some(&self.page[b..e])
    }
}

// From command: xxd resources/test/multipage-512B-page.db
#[cfg(test)]
const TEST_PAGE: &str = "0d00 0000 0a01 ce00 01fb 01f6 01f1 01ec
01e7 01e2 01dd 01d8 01d3 01ce 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 030a
020f 4a03 0902 0f49 0308 020f 4803 0702
0f47 0306 020f 4603 0502 0f45 0304 020f
4403 0302 0f43 0302 020f 4203 0102 0f41";

#[test]
fn test_cell_iterator() {
    use hex::FromHex;
    let p: Vec<u8> =
        Vec::from_hex(TEST_PAGE.replace(&[' ', '\n'][..], "")).expect("Invalid Hex String");
    println!("{:?}", p);
    assert_eq!(p.len(), 512);
    let mut ci = Iterator::new(&p, 0, 512);
    assert_eq!(ci.next().unwrap(), Vec::from_hex("0301020f41").unwrap());
    assert_eq!(ci.next().unwrap(), Vec::from_hex("0302020f42").unwrap());
    assert_eq!(ci.next().unwrap(), Vec::from_hex("0303020f43").unwrap());
    assert_eq!(ci.next().unwrap(), Vec::from_hex("0304020f44").unwrap());
    assert_eq!(ci.next().unwrap(), Vec::from_hex("0305020f45").unwrap());
    assert_eq!(ci.next().unwrap(), Vec::from_hex("0306020f46").unwrap());
    assert_eq!(ci.next().unwrap(), Vec::from_hex("0307020f47").unwrap());
    assert_eq!(ci.next().unwrap(), Vec::from_hex("0308020f48").unwrap());
    assert_eq!(ci.next().unwrap(), Vec::from_hex("0309020f49").unwrap());
    assert_eq!(ci.next().unwrap(), Vec::from_hex("030a020f4a").unwrap());
    assert_eq!(ci.next(), None);
}

// Cell Formats from https://www.sqlite.org/fileformat2.html#b_tree_pages
//
// Table B-Tree Leaf Cell (header 0x0d):
// A varint which is the total number of bytes of payload, including any overflow
// A varint which is the integer key, a.k.a. "rowid"
// The initial portion of the payload that does not spill to overflow pages.
// A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
//
// Table B-Tree Interior Cell (header 0x05):
// A 4-byte big-endian page number which is the left child pointer.
// A varint which is the integer key
//
// Index B-Tree Leaf Cell (header 0x0a):
// A varint which is the total number of bytes of key payload, including any overflow
// The initial portion of the payload that does not spill to overflow pages.
// A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
//
// Index B-Tree Interior Cell (header 0x02):
// A 4-byte big-endian page number which is the left child pointer.
// A varint which is the total number of bytes of key payload, including any overflow
// The initial portion of the payload that does not spill to overflow pages.
// A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
