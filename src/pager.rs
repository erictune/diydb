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

use std::cell::RefCell;
use std::io::{Read, Seek, SeekFrom};
use std::boxed::Box;

/// A `Pager` manages the file locking and the memory use for one open database file.
pub struct Pager {
    f: Box<RefCell<std::fs::File>>,
    pages: Vec<Option<Vec<u8>>>,
    initialized: RefCell<bool>,
    page_size: u32,
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

// TODO: support databases with more on-disk pages, limiting memory usage by paging out unused pages.
const MAX_PAGE_NUM: PageNum = 10_000; // 10_000 * 4k page ~= 40MB

impl Pager {
    pub fn open(path: &str) -> Self {
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
                        .expect("Should have opened file."
                    )
                );
        let h = crate::dbheader::get_header_clone(&mut file.borrow_mut())
            .expect("Should have parsed db header");
       file
            .borrow_mut()
            .seek(SeekFrom::Start(0))
            .expect("Should have returned file cursor to start");
        if h.numpages > MAX_PAGE_NUM as u32 {
            panic!("Too many pages");
        }
        Pager {
            f: Box::new(file),
            pages: vec![],
            initialized: RefCell::new(false),
            page_size: h.pagesize as u32,
        }
    }

    // Separate from open because I could not figure out how to return a file from the constructor
    // and use the file in one function.
    /// Must be called before using other methods.  Checks the database header and reads in its contents.
    pub fn initialize(&mut self) {
        if *self.initialized.borrow() {
            return;
        }
        let h = crate::dbheader::get_header_clone(&mut self.f.borrow_mut())
            .expect("Should have parsed db header");
        self.f
            .borrow_mut()
            .seek(SeekFrom::Start(0))
            .expect("Should have returned file cursor to start");
        if h.numpages > MAX_PAGE_NUM as u32 {
            panic!("Too many pages");
        }
        *self.initialized.borrow_mut() = true;

        for pn in 1..h.numpages + 1 {
            self.make_page_present(pn as usize);
        }
    }
    // Reads the header of a file after the file has been opened to ensure it is a valid file.
    fn check_initialized(&self) {
        if !*self.initialized.borrow() {
            panic!("Use of uninitialized Pager.");
        }
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
            .seek(SeekFrom::Start(
                (pn - 1) as u64 * self.page_size as u64,
            ))
            .unwrap();
        match self
            .f
            .borrow_mut()
            .read_exact(&mut v[..])
            .map_err(|_| Error::ReadFailed)
        {
            Ok(()) => Ok(v),
            Err(e) => Err(e),
        }
    }

    // TODO: implement transparent paging in of pages.
    pub fn make_page_present(&mut self, pn: PageNum) {
        if pn > self.pages.len() {
            // println!("Extending pager capacity to {}", pn);
            self.pages.resize(pn, None)
        }
        if self.pages[pn - 1].is_none() {
            // println!("Reading page {} on demand.", pn);
            let v = self
                .read_page_from_file(pn)
                .map_err(|_| Error::ReadFailed)
                .unwrap();
            self.pages[pn - 1] = Some(v);
        }
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
        self.check_initialized();
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
