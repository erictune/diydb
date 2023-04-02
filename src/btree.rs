// There are 4 types of btree page:
//     - A table b-tree interior page
//     - A table b-tree leaf page
//     - An index b-tree interior page
//     - An index b-tree leaf page

// A b-tree page is divided into regions in the following order
// 1 The 100-byte database file header (found on page 1 only)
// 2 The 8 or 12 byte b-tree page header
// 3 The cell pointer array
// 4 Unallocated space
// 5 The cell content area
// 6 The reserved region.  (hope to assume always 0)

use crate::serial_type;
use crate::record;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::{Cursor, Seek, SeekFrom};
// Sqlite supports different page sizes, but we are just going to support the default.
// TODO: consolidate multiple definitions of this constant in other modules.
const PAGESIZE: u32 = 4096;

// The database file header.
#[derive(Debug, Clone)]
pub struct Header {
    pub btree_page_type: PageType,
    pub freeblock_start: u32,
    pub num_cells: u32,
    pub cell_content_start: u32,
}

#[derive(Debug, Clone)]
pub enum PageType {
    IndexInterior,
    TableInterior,
    IndexLeaf,
    TableLeaf,
}

pub struct PageReader<'a> {
    page: &'a Vec<u8>,
    non_btree_header_bytes: usize,
}

impl<'a> PageReader<'a> {
    pub fn new(p: &Vec<u8>, non_btree_header_bytes: usize) -> PageReader {
        PageReader {
            page: p,
            non_btree_header_bytes: non_btree_header_bytes,
        }
    }

    pub fn check_header(&self) -> Header {
        //The 8 or 12 byte b-tree page (currently just the header).
        let mut c = Cursor::new(self.page);
        // The first page has a header which is not btree content, but which is included in cell pointers.
        if self.non_btree_header_bytes > 0 {
            c.seek(SeekFrom::Current(self.non_btree_header_bytes as i64))
                .expect("Should have seeked past db file header.");
        }
        // Read btree header.

        // Offset	Size	Description
        // 0	1	The one-byte flag at offset 0 indicating the b-tree page type.
        let btree_page_type = match c.read_u8().expect("Should have read btree header") {
            0x02 => PageType::IndexInterior,
            0x05 => PageType::TableInterior,
            0x0a => PageType::IndexLeaf,
            0x0d => PageType::TableLeaf,
            b => panic!("Invalid Btree Page Type: {}", b as u8),
        };

        // 1	2	The two-byte integer at offset 1 gives the start of the first freeblock on the page, or is zero if there are no freeblocks.
        let freeblock_start: u32 = c.read_u16::<BigEndian>().expect("Should have btree header") as u32;
        // 3	2	The two-byte integer at offset 3 gives the number of cells on the page.
        let num_cells: u32 = c.read_u16::<BigEndian>().expect("Should have read btree header") as u32;
        // 5	2	The two-byte integer at offset 5 designates the start of the cell content area. A zero value for this integer is interpreted as 65536.
        let cell_content_start: u32 = match c.read_u16::<BigEndian>().expect("Should have read btree header") {
            0 => 655365,
            x => x as u32,
        };
        // 7	1	The one-byte integer at offset 7 gives the number of fragmented free bytes within the cell content area.
        let _: u32 = c.read_u8().expect("Should have read btree header") as u32;
        // 8	4	The four-byte page number at offset 8 is the right-most pointer. This value appears in the header of interior b-tree pages only and is omitted from all other pages.
        
        let _ = match btree_page_type {
            PageType::IndexInterior | PageType::TableInterior => {
                Some(c.read_u32::<BigEndian>().expect("Should have read rightmost pointer"))
            }
            PageType::IndexLeaf | PageType::TableLeaf => None,
        };

       Header {
                btree_page_type: btree_page_type,
                freeblock_start: freeblock_start,
                num_cells: num_cells,
                cell_content_start: cell_content_start,
            }
    }
    
    pub fn print_cell_contents(&self) {
        let it = CellIterator::new(self.page, self.non_btree_header_bytes);
        // TODO: implment cell iterators for other page types with different Item
        // and different parsing code.
        for cell in it {
            // Cell format for table leaf page.
            // payload_len: A varint which is the total number of bytes of payload, including any overflow
            // rowid: A varint which is the integer key, a.k.a. "rowid"
            // unspilled_payload: The initial portion of the payload that does not spill to overflow pages.
            // overflow_page_num: A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
            let mut offset = 0;
            let (payload_len, bytesread) = sqlite_varint::read_varint(&cell);
            offset += bytesread;
            let (_rowid, bytesread2) = sqlite_varint::read_varint(&cell[offset..]);
            offset += bytesread2;
            //println!("payload_len: {} rowid: {}", payload_len, rowid);
            if cell.len() - offset != (payload_len as usize) {
                unimplemented!("Spilled payloads not implemented.");
            }
            let payload = &cell[offset..].to_vec();
            //println!("payload bytes {:?}", &payload);

            {
                // TODO: use map(typecode_to_string).join("|") or something like that.
                let rhi = record::HeaderIterator::new(&payload[..]);
                print!("|");
                for t in rhi {   
                    print!(" {} |", serial_type::typecode_to_string(t)); 
                }
                println!("");
            }
            print!("|");
            let hi = record::ValueIterator::new(&payload[..]);
            for (t, v) in hi {
                // TODO: map the iterator using a closure that calls to_string, and then intersperses the delimiters and then reduces into a string.
                // TODO: move cursor use into read_value_to_string, so it just uses a byte slice.
                print!(" {} |", serial_type::read_value_to_string(&t, &mut Cursor::new(v)));
            }
            println!("");
        }
    }
}

pub struct CellIterator<'a> {
    page: &'a Vec<u8>,
    cell_idx: usize,
    cell_offsets: Vec<usize>,
    cell_lengths: Vec<usize>,
}

impl<'a> CellIterator<'a> {
    /// Creates an iterator over the cells of a btree.
    ///
    /// Iterator produces cells which are slices of bytes, which contain a record.
    /// 
    /// # Arguments
    ///
    /// * `s` - A byte slice.  Borrowed for the lifetime of the iterator.  Slice begins with the record header length (a varint).
    ///         slives ends with the last byte of the record body.
    pub fn new(p: &Vec<u8>, non_btree_header_bytes: usize) -> CellIterator {
        let mut c = Cursor::new(p);
        c.seek(SeekFrom::Start(non_btree_header_bytes as u64)).expect("Should have seeked.");
        let btree_page_type = match c.read_u8().expect("Should have read btree header") {
            0x02 => PageType::IndexInterior,
            0x05 => PageType::TableInterior,
            0x0a => PageType::IndexLeaf,
            0x0d => PageType::TableLeaf,
            b => panic!("Invalid Btree Page Type: {}", b as u8),
        };
        c.seek(SeekFrom::Start(3 + non_btree_header_bytes as u64))
            .expect("Should have seeked.");
        let num_cells: u32 = c.read_u16::<BigEndian>().expect("Should have read btree header") as u32;

        let btree_header_bytes = match btree_page_type {
            PageType::IndexInterior | PageType::TableInterior => 12,
            PageType::IndexLeaf | PageType::TableLeaf => 8,
        };    
        c.seek(SeekFrom::Start(btree_header_bytes + non_btree_header_bytes as u64))
            .expect("Should have seeked to cell offset.");

        let mut it = CellIterator {
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
        let mut last_offset: usize = PAGESIZE as usize; // First cell in pointer list is the last cell on the page, so it ends on byte PAGESIZE, I think (?).
        for _ in 0..num_cells {
            let off = c.read_u16::<BigEndian>().expect("Should have read cell pointer") as usize;
            it.cell_offsets.push(off);
            it.cell_lengths.push(last_offset - off);
            last_offset = off;
        }
        it
    }
}

impl<'a> Iterator for CellIterator<'a> {
    // The iterator returns a reference to a cell (&[u8]).  The format of the data in the cell
    // is dependent on the type of the btree page.
    type Item = &'a [u8];

    /// Returns the next item, which is a tuple of (k, v), where
    ///   `k` is a key, the row number (u64)
    ///   `v` is a value, &[u8], the slice of bytes containing the "payload" of the cell.
    fn next(&mut self) -> Option<Self::Item> {
        if self.cell_idx >= self.cell_offsets.len() {
            return None
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

