//! manages pages from a sqlite3 file as defined at https://www.sqlite.org/fileformat.html
//!
//! `Pager` manages the pages from a single attached database.
//! `PagerSet` holds the `Pager`s for zero, one, or many databases.
//!
//! When there are zero databases open, SELECTs without FROM are still possible.
//! Routines that only deal with a single database, such as `crate::btree::*`, are provided with a `Pager`.
//! The few parts of the code that deal with potentially several databases (e.g. `main`, `do_query`) use a `PagerSet`, and
//! lend the pagers out to table-specific subroutines that need them.
//!
//! Currently, a Pager only supports single-threaded read-only access a database file. It reads all the pages into memory at once.
//!
//! A full implementation of a Pager would support concurrent read and write accesses, with demand paging and multiple files,
//! with the necessary reference counting and locking.
//!
//! A Pager is responsible for opening and locking a database file at the OS level.  A Pager owns the data in each page,
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
//! Freelist pages will be managed by the Pager once supported.
//! 
//! # Examples
//! 
//! You can open one or more pages readonly at once.
//! 
//! ```
//! # let path = (std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set") + "/resources/test/" + "minimal.db");
//! # use diydb::pager::Pager;;
//! let pager = Pager::open(path.as_str()).unwrap();
//! let p1 = pager.get_page_ro(1).unwrap();
//! let p2 = pager.get_page_ro(2).unwrap();
//! ```
//! 
// The following doc is here as a test, to ensure that borrow checking enforces the expected invariants.
//! At present, you cannot hold one page for read and one page for write at the same time.  This doesn't work:
//! ```compile_fail
//! # let path = (std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set") + "/resources/test/" + "minimal.db").as_str();
//! # use diydb::pager::Pager;;
//! let pager = Pager::open(path.as_str()).unwrap();
//! let p1 = pager.get_page_ro(1).unwrap();
//! let p2 = pager.get_page_rw(2).unwrap();
//!```
//! 
//! You also cannot hold two pages for write. This doesn't work:
//! ```compile_fail
//! # let path = (std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set") + "/resources/test/" + "minimal.db").as_str();
//! # use diydb::pager::Pager;;
//! let pager = Pager::open(path.as_str()).unwrap();
//! let p1 = pager.get_page_rw(1).unwrap();
//! let p2 = pager.get_page_rw(2).unwrap();
//!```
//! These limits will be fixed in the future.
//!
//! # Future work
//! -   Clarify how temporary tables are handled:
//!     - Short-lived temporary tables (For a single Tx or Connection) - don't need to be paged, can use internal
//!       vector implementation, but have limited memory, so don't need a Pager.
//!     - Long-lived in-memory non-persisted tables - May use page-based structure, but no backing file.
//!       May be arbitraryily large.  Should use pagerset.
//! SELECTS from temporary in-memory table, which do not use a Pager.
//! -   Use OS locking to lock the opened database file.
//! -   Support accessing pages for modification by locking the entire Pager.
//! -   Support concurrent access for read and write via table or page-level locking.
//! -   Support adding pages to the database.
//! -   Support reading pages on demand.
//! -   Support dropping unused pages when memory is low.
//! -   When there are multiple pagers (multiple open files), coordinating to stay under a total memory limit.
//! 

use std::boxed::Box;
use std::cell::RefCell;
use std::io::{Read, Seek, SeekFrom};

// Split the "list of Tables" and "memory manager" responsibilities of PagerSet/Pager.
use crate::temp_table::TempTable;
use crate::sql_type::SqlType;
use crate::table_traits::TableMeta;


#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Pager: Page number greater than maximum supported page number.")]
    PageNumberBeyondLimits,
    #[error("Pager: Internal error.")]
    Internal,
    #[error("Pager: Error accessing database file: {0}")]
    Io(#[from] std::io::Error),
    #[error("Pager: Error in database header: {0}")]
    DbHdr(#[from] crate::dbheader::Error),
    #[error("Default database pager requested when no databases loaded.")]
    NoDefaultDB,
    #[error("Default database pager requested when multiple databases loaded.")]
    AmbiguousDefaultDB,
    #[error("Too many pages open for write at once.")]
    TooManyPagesOpenForWrite,
    #[error("Table name not found.")]
    TableNameNotFound

}

// A `PagerSet` manages zero or more Pagers, one per open database.
pub struct PagerSet {
    pagers: Vec<Pager>,
    temp_tables: Vec<crate::temp_table::TempTable>, 
}

// 'a: lifetime of self
// 'b: lifetime of a returned Pager
impl<'a, 'b> PagerSet
where
    'a: 'b,
{
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        PagerSet { 
            pagers: vec![],      // TODO: provide table lookup by name, wrapping the persisted schema table.
            temp_tables: vec![], // TODO: key by name.
        }
    }
    pub fn new_temp_table(&'a mut self, table_name: String, column_names: Vec<String>, column_types: Vec<SqlType>, strict: bool) -> Result<(), Error> {
        self.temp_tables.push(
            TempTable {
                rows: vec![],
                table_name,
                column_names,
                column_types,
                strict,
            }
        );
        Ok(())
    }

    pub fn default_pager(&'a self) -> Result<&'b Pager, Error> {
        match self.pagers.len() {
            0 => Err(Error::NoDefaultDB),
            1 => Ok(&self.pagers[0]),
            _ => Err(Error::AmbiguousDefaultDB),
        }
    }
    pub fn default_pager_mut(&'a mut self) -> Result<&'b mut Pager, Error> {
        match self.pagers.len() {
            0 => Err(Error::NoDefaultDB),
            1 => Ok(&mut self.pagers[0]),
            _ => Err(Error::AmbiguousDefaultDB),
        }
    }
    pub fn opendb(&'a mut self, path: &str) -> Result<(), Error> {
        self.pagers.push(Pager::open(path)?);
        Ok(())
    }
    pub fn get_temp_table(&'a self, tablename: &String) -> Result<&'b crate::temp_table::TempTable, Error> {
        for i in 0..self.temp_tables.len() {
            if self.temp_tables[i].table_name() == *tablename {
                return Ok(&self.temp_tables[i]);
            }
        }
        Err(Error::TableNameNotFound)
    }

    pub fn get_temp_table_mut(&'a mut self, tablename: &String) -> Result<&'b  mut crate::temp_table::TempTable, Error> {
        for i in 0..self.temp_tables.len() {
            if self.temp_tables[i].table_name() == *tablename {
                return Ok(&mut self.temp_tables[i]);
            }
        }
        Err(Error::TableNameNotFound)
    }
}

/// A `Pager` manages the file locking and the memory use for one open database file.
pub struct Pager {
    f: Box<RefCell<std::fs::File>>,
    // TODO: pages could return a RefCell so that pages can be paged in on demand.
    // Then have `check_present()` call `make_present()` where needed.
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
    open_rw_page: Option<PageNum>,
    num_open_rw_pages: usize,
}

// Page numbers are 1-based, to match how Sqlite numbers pages.  PageNum ensures people pass something that is meant to be a page number
// to a function that expects a page number.
pub type PageNum = usize;

// TODO: support databases with more on-disk pages, limiting memory usage by paging out unused pages.
const MAX_PAGE_NUM: PageNum = 10_000; // 10_000 * 4k page ~= 40MB

impl Pager {
    /// opens a database file and verfies it is a SQLite db file, and reads in an unspecified number of pages of the database.
    ///
    /// Additional pages may be read in as needed later.
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
                        .map_err(Error::Io)?
                );
        let h = crate::dbheader::get_header_clone(&mut file.borrow_mut()).map_err(Error::DbHdr)?;
        file.borrow_mut()
            .seek(SeekFrom::Start(0))
            .map_err(Error::Io)?;
        if h.numpages > MAX_PAGE_NUM as u32 {
            return Err(Error::PageNumberBeyondLimits);
        }
        //TODO: read these in on demand.
        let mut pages = vec![None; h.numpages as usize];
        for pn in 1_usize..(h.numpages as usize) + 1 {
            let mut v = vec![0_u8; h.pagesize as usize];
            file.borrow_mut()
                .seek(SeekFrom::Start((pn - 1) as u64 * h.pagesize as u64))
                .map_err(Error::Io)?;
            file.borrow_mut()
                .read_exact(&mut v[..])
                .map_err(Error::Io)?;
            pages[pn - 1] = Some(v);
        }
        Ok(Pager {
            f: Box::new(file),
            pages,
            page_size: h.pagesize,
            open_rw_page: None,
            num_open_rw_pages: 0,
        })
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
            .map_err(Error::Io)?;
        self.f
            .borrow_mut()
            .read_exact(&mut v[..])
            .map_err(Error::Io)?;
        Ok(v)
    }

    // TODO: implement transparent paging in of pages.
    pub fn make_page_present(&mut self, pn: PageNum) -> Result<(), Error> {
        if pn > MAX_PAGE_NUM {
            return Err(Error::PageNumberBeyondLimits);
        }
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

    // TODO: need way to decrement count when page use is done.  Therefore caller needs to hold some object to count that.

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
            None => Err(Error::Internal),
        }
    }

    // TODO: need way to decrement count when page use is done.  Therefore caller needs to hold some object to count that.
    pub fn get_page_rw<'a, 'b: 'a>(&'b mut self, pn: PageNum) -> Result<&'a mut Vec<u8>, Error>  {
        if self.num_open_rw_pages > 0 {
            // At this time, we cannot atomically write multiple pages (we don't have rollbacks or a writeahead log).
            // Therefore, it is not supported to open multiple pages in rw mode.
            // Opening one page still allows for limited INSERT and UPDATE operations.
            return Err(Error::TooManyPagesOpenForWrite);
        }
        self.open_rw_page = Some(pn);
        self.num_open_rw_pages = 1;
        if pn > MAX_PAGE_NUM {
            return Err(Error::PageNumberBeyondLimits);
        }
        self.check_present(pn);
        match &mut self.pages[pn - 1] {
            Some(v) => Ok(v),
            None => Err(Error::Internal),
        }
    }

    pub fn get_page_size(&self) -> u32 {
        self.page_size
    }
}

#[cfg(test)]
fn path_to_testdata(filename: &str) -> String {
    std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set")
        + "/resources/test/"
        + filename
}

#[test]
fn test_open_db() {
    let path = path_to_testdata("minimal.db");
    let _pager = Pager::open(path.as_str()).expect("Should have opened db with pager.");
}

#[test]
fn test_get_page_rw() {
    let path = path_to_testdata("minimal.db");
    let mut pager = Pager::open(path.as_str()).expect("Should have opened db with pager.");
    let p1 = pager.get_page_rw(1);
    assert!(p1
            .expect("Should have gotten a page")
            .len()
            > 0
    );
}

#[test]
fn test_get_two_page_ro() {
    let path = path_to_testdata("minimal.db");
    let pager = Pager::open(path.as_str()).expect("Should have opened db with pager.");
    let p1 = pager.get_page_ro(1);
    let p2 = pager.get_page_ro(2);
    assert!(
        p1
            .expect("Should have gotten a page")
            .len()
            > 0
    );
    assert!(
        p2
        .expect("Should have gotten a page")
        .len()
        > 0
    );
}

// Testing: Borrow check fails for multiple writers or read and write as expected.  This is tested in doc comments at the top of the file.