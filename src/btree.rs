// Read a sqlite3 file as defined at https://www.sqlite.org/fileformat.html
// Supports very simplified subset of file format.

// Excepts from above docs:
// - The complete state of an SQLite database is usually contained in a single file on disk called the "main database file".
// - The main database file consists of one or more pages.
// - Every page in the main database has a single use which is one of the following:
//   - The lock-byte page
//   - A freelist page
//   - A freelist trunk page
//   - A freelist leaf page
//   - A b-tree page
//     - A table b-tree interior page
//     - A table b-tree leaf page
//     - An index b-tree interior page
//     - An index b-tree leaf page
//   - A payload overflow page
//   - A pointer map page
//
//  [ I aspire just to implement btree-pages, as the others don't seem to be required for simple databases that haven't been modified. ]
//
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
use std::io::{Cursor, Read, Seek, SeekFrom};
// Sqlite supports different page sizes, but we are just going to support the default.
// TODO: consolidate multiple definitions of this constant in other modules.
const PAGESIZE: u32 = 4096;

#[derive(thiserror::Error, Debug, Clone)]
pub enum Error {
    #[error("The btree page type is not valid.")]
    InvalidBtreePageType,
    #[error("Error reading file.")]
    ReadFailed,
}

// The database file header.
#[derive(Debug, Clone)]
pub struct BtreePageHeader {
    pub btree_page_type: BtreePageType,
    pub freeblock_start: u32,
    pub num_cells: u32,
    pub cell_content_start: u32,
}

#[derive(Debug, Clone)]
pub enum BtreePageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}

// TODO: function to iterate over contents of a specific btree rooted at page #.

// TODO: function to iterate over  It needs to have functions to iterate over all keys for a btree rooted at (page), or to search for a key in a btree rooded at (page).

//The 8 or 12 byte b-tree page (currently just the header).
pub fn get_btree_page(
    v: &Vec<u8>,
    non_btree_header_bytes: usize,
) -> Result<BtreePageHeader, Error> {
    // f should contain an entire page of size "pagesize".

    let mut c = Cursor::new(v);
    // The first page has a header which is not btree content, but which is included in cell pointers.
    if non_btree_header_bytes > 0 {
        c.seek(SeekFrom::Current(non_btree_header_bytes as i64))
            .expect("Should have seeked past db file header.");
    }
    println!(
        "Reading btree header that starts at page offset {}",
        non_btree_header_bytes
    );

    // Read btree header.

    // Offset	Size	Description
    // 0	1	The one-byte flag at offset 0 indicating the b-tree page type.
    let btree_page_type = match c.read_u8().map_err(|_| Error::ReadFailed)? {
        0x02 => BtreePageType::InteriorIndex,
        0x05 => BtreePageType::InteriorTable,
        0x0a => BtreePageType::LeafIndex,
        0x0d => BtreePageType::LeafTable,
        _ => return Err(Error::InvalidBtreePageType),
    };

    // 1	2	The two-byte integer at offset 1 gives the start of the first freeblock on the page, or is zero if there are no freeblocks.
    let freeblock_start: u32 = c.read_u16::<BigEndian>().map_err(|_| Error::ReadFailed)? as u32;
    // 3	2	The two-byte integer at offset 3 gives the number of cells on the page.
    let num_cells: u32 = c.read_u16::<BigEndian>().map_err(|_| Error::ReadFailed)? as u32;
    // 5	2	The two-byte integer at offset 5 designates the start of the cell content area. A zero value for this integer is interpreted as 65536.
    let cell_content_start: u32 = match c.read_u16::<BigEndian>().map_err(|_| Error::ReadFailed)? {
        0 => 655365,
        x => x as u32,
    };
    // 7	1	The one-byte integer at offset 7 gives the number of fragmented free bytes within the cell content area.
    let _: u32 = c.read_u8().map_err(|_| Error::ReadFailed)? as u32;
    // 8	4	The four-byte page number at offset 8 is the right-most pointer. This value appears in the header of interior b-tree pages only and is omitted from all other pages.
    let _ = match btree_page_type {
        BtreePageType::InteriorIndex | BtreePageType::InteriorTable => {
            Some(c.read_u32::<BigEndian>().map_err(|_| Error::ReadFailed)?)
        }
        BtreePageType::LeafIndex | BtreePageType::LeafTable => None,
    };

    // Read the cell pointer array:
    // """
    // The cell pointer array of a b-tree page immediately follows the b-tree page header.
    // Let K be the number of cells on the btree. The cell pointer array consists of K 2-byte
    // integer offsets to the cell contents. The cell pointers are arranged in key order with
    // left-most cell (the cell with the smallest key) first and the right-most cell (the cell
    // with the largest key) last.
    // """()
    let mut cell_offsets: Vec<usize> = Vec::new();
    let mut cell_lengths: Vec<usize> = Vec::new();
    let last_offset: usize = PAGESIZE as usize; // First cell in pointer list is the last cell on the page, so it ends on byte PAGESIZE, I think (?).
    for _ in 0..num_cells {
        let off = c.read_u16::<BigEndian>().map_err(|_| Error::ReadFailed)? as usize;
        cell_offsets.push(off);
        cell_lengths.push(last_offset - off);
    }
    // TODO: implement this as an iterator over cells in a page,
    // using iterators: https://doc.rust-lang.org/beta/rust-by-example/trait/iter.html
    // and returning a slice that gives access to a payload.
    println!(
        "cell_offsets: {:?} cell_lengths: {:?}",
        cell_offsets, cell_lengths
    );
    for kk in 0..num_cells as usize {
        // Go to start of page plus offset.
        println!("Seeking to {}", (cell_offsets[kk] as u64));
        c.seek(SeekFrom::Start(cell_offsets[kk] as u64))
            .expect("Should have seeked to cell offset.");
        let mut celltmp = vec![0u8; cell_lengths[kk]];
        {
            c.read_exact(&mut celltmp).expect("Should have read cell");
        }
        match btree_page_type {
            BtreePageType::LeafTable => {
                // Cell format for table leaf page.
                // payload_len: A varint which is the total number of bytes of payload, including any overflow
                // rowid: A varint which is the integer key, a.k.a. "rowid"
                // unspilled_payload: The initial portion of the payload that does not spill to overflow pages.
                // overflow_page_num: A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
                let mut offset = 0;
                let (payload_len, bytesread) = sqlite_varint::read_varint(&celltmp);
                offset += bytesread;
                let (rowid, bytesread2) = sqlite_varint::read_varint(&celltmp[offset..]);
                offset += bytesread2;
                println!("payload_len: {} rowid: {}", payload_len, rowid);
                if celltmp.len() - offset != (payload_len as usize) {
                    unimplemented!("Spilled payloads not implemented.");
                }
                let payload = &celltmp[offset..].to_vec();
                println!("payload bytes {:?}", &payload);

                {
                    // TODO: use map(typecode_to_string).join("|") or something like that.
                    let rhi = record::HeaderIterator::new(&payload[..]);
                    print!("|");
                    for t in rhi {   
                        print!(" {} |", serial_type::typecode_to_string(t)); 
                    }
                    println!("");
                }
                println!("---");
                print!("|");
                let hi = record::ValueIterator::new(&payload[..]);
                for (t, v) in hi {
                    // TODO: map the iterator using a closure that calls to_string, and then intersperses the delimiters and then reduces into a string.
                    // TODO: move cursor use into read_value_to_string, so it just uses a byte slice.
                    print!(" {} |", serial_type::read_value_to_string(&t, &mut Cursor::new(v)));
                }
                println!("");
            }
            _ => unimplemented!("Only Leaf Table page types implemented."),
        }
    }
    // TODO: does this need to be returned really?
    Ok(BtreePageHeader {
        btree_page_type: btree_page_type,
        freeblock_start: freeblock_start,
        num_cells: num_cells,
        cell_content_start: cell_content_start,
    })
}
