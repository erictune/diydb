use super::cell;
use super::RowId;

pub struct Iterator<'a> {
    ci: cell::Iterator<'a>,
}

impl<'a> Iterator<'a> {
    /// Creates an iterator over the cells of a single page of a btree, with page of type TableLeaf.
    ///
    /// Iterator produces cells which are slices of bytes, which contain a record.
    ///
    /// # Arguments
    ///
    /// * `s` - A byte slice.  Borrowed for the lifetime of the iterator.  Slice begins with the record header length (a varint).
    ///         slives ends with the last byte of the record body.
    pub fn new(ci: cell::Iterator) -> Iterator {
        Iterator { ci }
    }
}

impl<'a> core::iter::Iterator for Iterator<'a> {
    // The iterator returns a tuple of (rowid, cell_payload).
    // Overflowing payloads are not supported.
    type Item = (RowId, &'a [u8]);

    /// Returns the next item, which is a tuple of (k, v), where
    ///   `k` is a key, the row number (u64)
    ///   `v` is a value, &[u8].
    fn next(&mut self) -> Option<Self::Item> {
        match self.ci.next() {
            None => None,
            Some(cell) => {
                let mut offset = 0;
                let (payload_len, bytesread) = sqlite_varint::read_varint(cell);
                offset += bytesread;
                let (rowid, bytesread2) = sqlite_varint::read_varint(&cell[offset..]);
                offset += bytesread2;
                if cell.len() - offset != (payload_len as usize) {
                    unimplemented!("Spilled payloads not implemented.");
                }
                //let payload = &cell[offset..].to_vec();
                //println!("payload bytes {:?}", &payload);
                Some((rowid as RowId, &cell[offset..]))
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

#[cfg(test)]
pub fn new_table_leaf_cell_iterator_for_page(
    pgr: &crate::pager::Pager,
    pgnum: usize,
) -> crate::btree::leaf::Iterator {
    use crate::btree;

    let pgsz = pgr.get_page_size();
    let page = match pgr.get_page_ro(pgnum) {
        Ok(p) => p,
        Err(e) => panic!("Error loading db page #{} : {}", pgnum, e),
    };
    let btree_start_offset = match pgnum {
        1 => 100,
        _ => 0,
    };
    let hdr = btree::header::check_header(page, btree_start_offset);
    println!("Examining page {} with header {:?}", pgnum, hdr);
    match hdr.btree_page_type {
        btree::PageType::TableLeaf => {
            btree::leaf::Iterator::new(btree::cell::Iterator::new(page, btree_start_offset, pgsz))
        }
        _ => {
            unreachable!()
        }
    }
}

#[test]
fn test_leaf_iterator_on_minimal_db() {
    let path = path_to_testdata("minimal.db");
    let mut pager = crate::pager::Pager::open(path.as_str());
    pager.initialize();
    let x = crate::get_creation_sql_and_root_pagenum(&mut pager, "a");
    let mut ri = new_table_leaf_cell_iterator_for_page(&mut pager, x.unwrap().0);
    let first_item = ri.next().clone();
    assert!(first_item.is_some());
    assert_eq!(first_item.unwrap().0, 1);
    assert!(ri.next().is_none());
}
