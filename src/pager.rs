//! `pager` manages pages from a sqlite3 file as defined at https://www.sqlite.org/fileformat.html
//!
//! Currently, it only supports single-threaded read-only access to a single file. It reads all the pages into memory at once.
//!
//! A full implementation of a pager would support concurrent read and write accesses, with demand paging and multiple files,
//! with the necessary reference counting and locking.
//!
//! A `pager` is responsible for opening and locking a database file at the OS level.  A pager owns the data in each page,
//! and allows callers to access it for reading without copying.
//!
//! There are a number of page types in a SQLite database: Summarizing the SQLite documentation:
//!
//! > -   The complete state of an SQLite database is usually contained in a single file on disk called the "main database file".
//! >    The main database file consists of one or more pages.*
//! > -   Every page in the main database has a single use which is one of the following:
//! >    -   The lock-byte page
//! >    -   A freelist page
//! >    -   A freelist trunk page
//! >    -   A freelist leaf page
//! >    -   A b-tree page
//! >        -   A table b-tree interior page
//! >        -   A table b-tree leaf page
//! >        -   An index b-tree interior page
//! >        -   An index b-tree leaf page
//! >    -   A payload overflow page
//! >    -   A pointer map page
//!
//! However, simple database files only contain table btree pages.
//! Freelist pages will be managed by the pager once supported.
//!
//! # Future work
//!
//! -   Use OS locking to lock the opened database file.
//! -   Support accessing pages for modification by locking the entire pager.
//! -   Support concurrent access for read and write via table or page-level locking.
//! -   Support adding pages to the database.
//! -   Support reading pages on demand.
//! -   Support dropping unused pages when memory is low.
//! -   Support multiple open files by coordinating between several pager objects to stay under a total memory limit.

use std::boxed::Box;
use std::cell::RefCell;
use std::io::{Read, Seek, SeekFrom};

/// A `Pager` manages the file locking and the memory use for one open database file.
pub struct Pager {
    f: Box<RefCell<std::fs::File>>,
    // TODO: pages could return a RefCell so that pages can be paged in on demand.
    // Then get rid of `initialize()` and have `check_present()` call `make_present()` where needed.
    // When implementing that, some things to consider are:
    // - The memory overhead: I think it should be low, given that pages (512B-4kB) are much larger than the overhead (16B-24B?).
    // - The cpu overhead: Is it paid by every function in the stack of iterators and , or once per at allocation and de-allocation, or on every access?  Perhaps benchmark it.
    //   - Scan Preloaded pages without RefCell vs scan preloaded pages acessed via RefCell.
    // Does the need to deal with a type other than byte slice hurt the readability of all the downstream code?
    // - Does RefCell allow locally converting to a readonly byte slice, within a scope?  Does that help?
    // Should `get_page_ro()` return a PageHandle?
    //   - Or should downstream code just be generic enough (require Traits it needs) that it can deal with
    //     a RefCell<...> or whatever locking wrapper is needed next?
    //   - Will I end up with RefCell<...<RefCell<...>>...>  since both the list needs locking to expand, and the
    //     pages need locking for presence?
    // Do I need a way to deal with failure other than panicing (which is what RefCell does?)  Like waiting, or logging
    // specific information?
    pages: Vec<Option<Vec<u8>>>,
    page_size: u32,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Pager: Page number greater than maximum supported page number.")]
    PageNumberBeyondLimits,
    #[error("Pager: Internal error.")]
    InternalError,
    #[error("Pager: Error accessing database file: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Pager: Error in database header: {0}")]
    DbHdrError(#[from] crate::dbheader::Error),
}

// Page numbers are 1-based, to match how Sqlite numbers pages.  PageNum ensures people pass something that is meant to be a page number
// to a function that expects a page number.
pub type PageNum = usize;

// TODO: support databases with more on-disk pages, limiting memory usage by paging out unused pages.
const MAX_PAGE_NUM: PageNum = 10_000; // 10_000 * 4k page ~= 40MB

impl Pager {
    pub fn open(path: &str) -> Result<Self, Error> {
        let file =
                // TODO: Lock file when opening so that other processes do not also
                // open and modify it, and so that is not modified while reading.
                // I tried  https://docs.rs/file-lock/latest/file_lock/ but it doesn't support opening readonly and locking at the same time.
                //  Instead, try https://crates.io/crates/fd-lock to see if it is any better.
                RefCell::new(
                    std::fs::OpenOptions::new()
                        .read(true)
                        .write(false)
                        .create(false)
                        .open(path)
                        .map_err(|e| Error::IoError(e))?
                );
        let h = crate::dbheader::get_header_clone(&mut file.borrow_mut())
            .map_err(|e| Error::DbHdrError(e))?;
        file.borrow_mut()
            .seek(SeekFrom::Start(0))
            .map_err(|e| Error::IoError(e))?;
        if h.numpages > MAX_PAGE_NUM as u32 {
            return Err(Error::PageNumberBeyondLimits);
        }
        Ok(Pager {
            f: Box::new(file),
            pages: vec![],
            page_size: h.pagesize as u32,
        })
    }

    // Reads in all the pages of the file. TODO: do this on demand.
    pub fn initialize(&mut self) -> Result<(), Error> {
        let h = crate::dbheader::get_header_clone(&mut self.f.borrow_mut())
            .map_err(|e| Error::DbHdrError(e))?;
        self.f
            .borrow_mut()
            .seek(SeekFrom::Start(0))
            .map_err(|e| Error::IoError(e))?;
        if h.numpages > MAX_PAGE_NUM as u32 {
            panic!("Too many pages");
        }
        for pn in 1..h.numpages + 1 {
            self.make_page_present(pn as usize)?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn alloc_new_page(self) -> PageNum {
        // TODO: to support writes, need to allocate new pages: write to the database header to increase the page count.
        unimplemented!()
    }

    fn read_page_from_file(&self, pn: PageNum) -> Result<Vec<u8>, Error> {
        let mut v = vec![0_u8; self.page_size as usize];
        self.f
            .borrow_mut()
            .seek(SeekFrom::Start((pn - 1) as u64 * self.page_size as u64))
            .map_err(|e| Error::IoError(e))?;
        self.f
            .borrow_mut()
            .read_exact(&mut v[..])
            .map_err(|e| Error::IoError(e))?;
        Ok(v)
    }

    // TODO: implement transparent paging in of pages.
    pub fn make_page_present(&mut self, pn: PageNum) -> Result<(), Error> {
        if pn > self.pages.len() {
            // println!("Extending pager capacity to {}", pn);
            self.pages.resize(pn, None)
        }
        if self.pages[pn - 1].is_none() {
            // println!("Reading page {} on demand.", pn);
            let v = self.read_page_from_file(pn)?;
            self.pages[pn - 1] = Some(v);
        }
        Ok(())
    }

    fn check_present(&self, pn: PageNum) {
        // We are increasing the capacity of what pages we cache in memory, not changing the on-disk database file.
        if pn > self.pages.len() {
            panic!("Pager capacity does not extend to requested page.");
        }

        if self.pages[pn - 1].is_none() {
            panic!("Page not loaded!");
        }
    }

    // I think this says that the self object, has lifetime 'b which must be longer than the lifetime of the returned reference
    // to the vector it contains.
    // That is currently true, since we don't get rid of or modify pages.
    // Once we implement writing or paging-out, we will need to provide a shorter lifetime for the
    // Page and/or use runtime locking to ensure we don't page out or write to something
    // that is in use.  So, the returned object (say, struct PageRef?) will need to participate in reference
    // counting.
    pub fn get_page_ro<'a, 'b: 'a>(&'b self, pn: PageNum) -> Result<&'a Vec<u8>, Error> {
        if pn > MAX_PAGE_NUM {
            return Err(Error::PageNumberBeyondLimits);
        }
        self.check_present(pn);
        match &self.pages[pn - 1] {
            Some(v) => Ok(v),
            None => Err(Error::InternalError),
        }
    }

    #[allow(dead_code)]
    pub fn get_page_rw(self, _: PageNum) -> Result<Vec<u8>, Error> {
        //self.check_initialized();
        // TODO: support writing pages. This will need reader/writer locks.
        unimplemented!("Writing not implemented")
    }

    pub fn get_page_size(&self) -> u32 {
        self.page_size
    }
}
