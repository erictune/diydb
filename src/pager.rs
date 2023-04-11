// Manages pages from a sqlite3 file as defined at https://www.sqlite.org/fileformat.html
// Supports very simplified subset of file format.
//
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
// The pager owns the data in each page, and allows callers to access it for reading or writing.
// Goal is to avoid copying pages.
// Pages are loaded on demand.
// All pages have the same size.

use std::io::{Read, Seek, SeekFrom};

/// A pager manages the file locking and the memory use for one open database file.
// TODO: When several files are open, coordinate between different pagers to maintain an overall memory limit.
// TODO: rw locking for concurrent accesses by multiple cursors to one file.
pub struct Pager {
    f: std::fs::File,
    pages: Vec<Option<Vec<u8>>>,
    initialized: bool,
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum Error {
    #[error("The page number is higher than the file contains or the code supports.")]
    PageNumberBeyondLimits,
    #[error("Error reading file.")]
    ReadFailed,
    #[error("Internal error.")]
    InternalError,
}

// Page numbers are 1-based, to match how Sqlite numbers pages.  PageNum ensures people pass something that is meant to be a page number
// to a function that expects a page number.
pub type PageNum = usize;

// TODO: support variable page sizes, using the page size specified in the DB.
pub const PAGE_SIZE: usize = 4096;

// TODO: support databases with more on-disk pages, limiting memory usage by paging out unused pages.
const MAX_PAGE_NUM: PageNum = 10_000; // 10_000 * 4k page ~= 40MB

impl Pager {
    pub fn open(path: &str) -> Self {
        Pager {
            f: {
                // TODO: Lock file when opening so that other processes do not also
                // open and modify it, and so that is not modified while reading.
                // See https://docs.rs/file-lock/latest/file_lock/
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(false)
                    .create(false)
                    .open(path)
                    .expect("Should have opened file.")
                // let h = crate::vfs::get_header(&mut self.f.unwrap().borrow());
            },
            pages: vec![],
            initialized: false,
        }
        // TODO: get the header and check that the number of pages in the DB is less than the maximum number of pages allowed.
    }

    // Reads the header of a file after the file has been opened to 
    // ensure it is a valid file.
    // Separate from open because I could not figure out how to return a
    // file from the constructor and use the file in one function.
    // To be called before using other methods.
    // TODO: figure out how to do this in the constructor or with
    // interior mutability so that it doesn't force all other methods
    // to be mutable.
    fn ensure_initialized(&mut self) -> Result<(), Error> {
	    if self.initialized	{ return Ok(()) }
        let h = crate::dbheader::get_header_clone(&mut self.f).expect("Should have parsed db header"); 
        self.f.seek(SeekFrom::Start(0)).expect("Should have returned file cursor to start");
        if h.numpages > MAX_PAGE_NUM as u32 { 
            panic!("Too many pages");
        }               
        self.initialized = true;
        Ok(())
    }

    #[allow(dead_code)]
    fn alloc_new_page(self) -> PageNum {
        // TODO: to support writes, need to allocate new pages: write to the database header to increase the page count.
        unimplemented!()
    }

    fn read_page_from_file(&mut self, pn: PageNum) -> Result<Vec<u8>, Error> {
        let mut v = vec![0_u8; PAGE_SIZE];
        self.f
            .seek(SeekFrom::Start((pn - 1) as u64 * PAGE_SIZE as u64))
            .unwrap();
        match self.f.read_exact(&mut v[..]).map_err(|_| Error::ReadFailed) {
            Ok(()) => Ok(v),
            Err(e) => Err(e),
        }
    }

    fn ensure_present(&mut self, pn: PageNum) {
        // We are increasing the capacity of what pages we cache in memory, not changing the on-disk database file.
        if pn > self.pages.len() {
            // println!("Extending pager capacity to {}", pn);
            self.pages.resize(pn, None)
        }

        let need_load = match self.pages[pn - 1] {
            None => true,
            Some(_) => false,
        };
        if !need_load {
            return;
        }
        // println!("Reading page {} on demand.", pn);
        let v = self
            .read_page_from_file(pn)
            .map_err(|_| Error::ReadFailed)
            .unwrap();
        self.pages[pn - 1] = Some(v);
    }

    // I think this says that the self object, has lifetime 'b which must be longer than the lifetime of the returned reference
    // to the vector it contains.
    pub fn get_page_ro<'a, 'b: 'a>(&'b mut self, pn: PageNum) -> Result<&'a Vec<u8>, Error> {
	    self.ensure_initialized().unwrap();
        if pn > MAX_PAGE_NUM {
            return Err(Error::PageNumberBeyondLimits);
        }

        self.ensure_present(pn);
        match &self.pages[pn - 1] {
            Some(v) => Ok(v.as_ref()),
            None => Err(Error::InternalError),
        }
    }

    #[allow(dead_code)]
    pub fn get_page_rw(self, _: PageNum) -> Result<Vec<u8>, Error> {
	    //self.ensure_initialized().unwrap();
        // TODO: support writing pages. This will need reader/writer locks.
        unimplemented!("Writing not implemented")
    }
}
