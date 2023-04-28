//! header reads the header of a btree page.
//! A b-tree page is divided into regions in the following order
//! 1. The 100-byte database file header (found on page 1 only)
//! 2. The 8 or 12 byte b-tree page header
//! 3. The cell pointer array
//! 4. Unallocated space
//! 5. The cell content area
//! 6. The reserved region.  (hope to assume always 0)

use super::PageType;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::{Cursor, Seek, SeekFrom};

// The database file header.
#[derive(Debug, Clone)]
pub struct Header {
    pub btree_page_type: PageType,
    pub freeblock_start: u32,
    pub num_cells: u32,
    pub cell_content_start: u32,
    pub rightmost_pointer: Option<u32>,
}

pub fn check_header<'a>(page: &'a Vec<u8>, non_btree_header_bytes: usize) -> Header {
    //The 8 or 12 byte b-tree page (currently just the header).
    let mut c = Cursor::new(page);
    // The first page has a header which is not btree content, but which is included in cell pointers.
    if non_btree_header_bytes > 0 {
        c.seek(SeekFrom::Current(non_btree_header_bytes as i64))
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
    let num_cells: u32 = c
        .read_u16::<BigEndian>()
        .expect("Should have read btree header") as u32;
    // 5	2	The two-byte integer at offset 5 designates the start of the cell content area. A zero value for this integer is interpreted as 65536.
    let cell_content_start: u32 = match c
        .read_u16::<BigEndian>()
        .expect("Should have read btree header")
    {
        0 => 655365,
        x => x as u32,
    };
    // 7	1	The one-byte integer at offset 7 gives the number of fragmented free bytes within the cell content area.
    let _: u32 = c.read_u8().expect("Should have read btree header") as u32;
    // 8	4	The four-byte page number at offset 8 is the right-most pointer. This value appears in the header of interior b-tree pages only and is omitted from all other pages.

    let rightmost_pointer = match btree_page_type {
        PageType::IndexInterior | PageType::TableInterior => Some(
            c.read_u32::<BigEndian>()
                .expect("Should have read rightmost pointer"),
        ),
        PageType::IndexLeaf | PageType::TableLeaf => None,
    };

    Header {
        btree_page_type,
        freeblock_start,
        num_cells,
        cell_content_start,
        rightmost_pointer,
    }
}
