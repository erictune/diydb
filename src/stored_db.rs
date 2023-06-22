//! Defines `StoredDb` type, which represents one disk-backed database file.
//! 
//! Manages the file access to one sqlite3 file.
//! The sqlite3 file format is defined at https://www.sqlite.org/fileformat.html
//! 

// TODO:
//  - Use OS locking to lock the opened database file.
//  - Support accessing pages for modification by locking the entire Pager.
//  - Support concurrent access for read and write via table or page-level locking.
//  - Support adding pages to the database.
//  - Support reading pages on demand.
//  - Support dropping unused pages when memory is low.
//  - When there are multiple pagers (multiple open files), coordinating to stay under a total memory limit.

use std::boxed::Box;
use std::collections::HashMap;
use std::cell::RefCell;
use std::io::{Read, Seek, SeekFrom};
use std::str::FromStr;

use streaming_iterator::StreamingIterator;

use crate::sql_type::SqlType;
use crate::sql_value::SqlValue;

use crate::stored_table::StoredTable;

// Page 1 (the first page) is always a btree page, and it is the root page of the schema table.
// It has references to the root pages of other btrees.
const SCHEMA_TABLE_NAME: &str = "sqlite_schema";
const SCHEMA_BTREE_ROOT_PAGENUM: PageNum = 1;
const SCHEMA_SCHEMA: &str =
    "CREATE TABLE sqlite_schema (type text, name text, tbl_name text, rootpage integer, sql text)";
const SCHEMA_TABLE_COL_NAMES: [&str; 5] = ["type", "name", "tbl_name", "rootpage", "sql"];
const SCHEMA_TABLE_COL_TYPES: [SqlType; 5] = [SqlType::Text, SqlType::Text, SqlType::Text, SqlType::Int, SqlType::Text];
const SCHEMA_TABLE_TBL_NAME_COLIDX: usize = 2;
const SCHEMA_TABLE_ROOTPAGE_COLIDX: usize = 3;
const SCHEMA_TABLE_SQL_COLIDX: usize = 4;

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
    #[error("Too many pages open for write at once.")]
    TooManyPagesOpenForWrite,
    #[error("Table {0} not found in database.")]
    TableNameNotFound(String),
    #[error("Error opening stored table.")]
    OpeningStoredTable,
}

/// A `StoredDb` manages the file locking and the memory use for one open database file.
/// 
/// Currently, a StoredDb only supports single-threaded read-only access a database file. It reads all the pages into memory at once.
///
/// A full implementation of a StoredDb would support concurrent read and write accesses, with demand paging and multiple files,
/// with the necessary reference counting and locking.
///
/// A StoredDb is responsible for opening and locking a database file at the OS level.  A StoredDb owns the data in each page,
/// and allows callers to access it for reading without copying.
///
/// There are a number of page types in a SQLite database: Summarizing the SQLite documentation:
/// > The complete state of an SQLite database is usually contained in a single file on disk called the "main database file".
/// >    The main database file consists of one or more pages.*
/// > -   Every page in the main database has a single use which is one of the following:
/// >    -   The lock-byte page
/// >    -   A freelist page
/// >    -   A freelist trunk page
/// >    -   A freelist leaf page
/// >    -   A b-tree page
/// >        -   A table b-tree interior page
/// >        -   A table b-tree leaf page
/// >        -   An index b-tree interior page
/// >        -   An index b-tree leaf page
/// >    -   A payload overflow page
/// >    -   A pointer map page
/// 
/// However, simple database files only contain table btree pages.
/// Freelist pages will be managed by the Pager once supported.
// A `PagerSet` manages zero or more Pagers, one per open database.
/// # Examples
/// 
/// You can open one or more pages readonly at once.
/// 
/// ```
/// # let path = (std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set") + "/resources/test/" + "minimal.db");
/// # use diydb::stored_db::StoredDb;
/// let sdb = StoredDb::open(path.as_str()).unwrap();
/// let p1 = sdb.get_page_ro(1).unwrap();
/// let p2 = sdb.get_page_ro(2).unwrap();
/// ```
/// 
// The following doc is here as a test, to ensure that borrow checking enforces the expected invariants.
/// At present, you cannot hold one page for read and one page for write at the same time.  This doesn't work:
/// ```compile_fail
/// # let path = (std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set") + "/resources/test/" + "minimal.db");
/// # use diydb::stored_db::StoredDb;
/// let sdb = StoredDb::open(path.as_str()).unwrap();
/// let p1 = sdb.get_page_ro(1).unwrap();
/// let p2 = sdb.get_page_rw(2).unwrap();
/// ```
///  
///  You also cannot hold two pages for write. This doesn't work:
///  ```compile_fail
/// # let path = (std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set") + "/resources/test/" + "minimal.db");
/// # use diydb::stored_db::StoredDb;
/// let sdb = StoredDb::open(path.as_str()).unwrap();
/// let p1 = sdb.get_page_rw(1).unwrap();
/// let p2 = sdb.get_page_rw(2).unwrap();
/// ```
///  These limits will be fixed in the future.
pub struct StoredDb {
    // This would be per DB.
    f: Box<RefCell<std::fs::File>>,

    // TODO: pages could return a RefCell so that pages can be paged in on demand.
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

    // TODO: This can be per-table - a table has its btree pages, and any overflow pages.  When there is freelist support, that would be at the Db level.
    /// Map from page number to the page data, or key not found if page not in memory.
    pages: HashMap<PageNum, Vec<u8>>,
    // This goes into the StoredDB.
    page_size: u32,
    // This could be per table, though there might need to be special consideration for the first page when the header changes.
    open_rw_page: Option<PageNum>,
    // This could be per table, though there might need to be special consideration for the first page when the headers changes.
    num_open_rw_pages: usize,
}

// Page numbers are 1-based, to match how Sqlite numbers pages.  PageNum ensures people pass something that is meant to be a page number
// to a function that expects a page number.
pub type PageNum = usize;

// TODO: support databases with more on-disk pages, limiting memory usage by paging out unused pages.
const MAX_PAGE_NUM: PageNum = 10_000; // 10_000 * 4k page ~= 40MB

impl StoredDb {
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
        let mut pages: HashMap<PageNum, Vec<u8>> = HashMap::new();
        for pn in 1_usize..(h.numpages as usize) + 1 {
            let mut v = vec![0_u8; h.pagesize as usize];
            file.borrow_mut()
                .seek(SeekFrom::Start((pn - 1) as u64 * h.pagesize as u64))
                .map_err(Error::Io)?;
            file.borrow_mut()
                .read_exact(&mut v[..])
                .map_err(Error::Io)?;
            pages.insert(pn, v.into());
        }
        Ok(StoredDb {
            f: Box::new(file),
            pages,
            page_size: h.pagesize,
            open_rw_page: None,
            num_open_rw_pages: 0,
        })
    }

    /// Get the root page number for `table_name`.
    pub fn get_root_pagenum(&self, table_name: &str) -> Option<PageNum> {
        if table_name == SCHEMA_TABLE_NAME {
            return Some(SCHEMA_BTREE_ROOT_PAGENUM);
        } else {
            let schema_table = StoredTable::new(
                self,
                String::from(SCHEMA_TABLE_NAME),
                SCHEMA_BTREE_ROOT_PAGENUM,
                SCHEMA_TABLE_COL_NAMES.iter().map(|x| x.to_string()).collect(),
                Vec::from(SCHEMA_TABLE_COL_TYPES),
                true,
            );   
            let mut it = schema_table.streaming_iterator();
            while let Some(row) = it.next() {
                let this_table_name = match &row.items[SCHEMA_TABLE_TBL_NAME_COLIDX] {
                    SqlValue::Text(s) => s.clone(),
                    _ => panic!("Type mismatch in schema table column {}, expected Text", SCHEMA_TABLE_TBL_NAME_COLIDX),
                };
                if this_table_name != table_name {
                    continue;
                }
                // TODO: refactor code below to "get row element as type x or return nicely formatted Error", which can be used elsewhere too.
                let root_pagenum = match &row.items[SCHEMA_TABLE_ROOTPAGE_COLIDX] {
                    SqlValue::Int(i) => *i as PageNum,
                    // TODO: return Result rather than panicing.
                    _ => panic!("Type mismatch in schema table column {}, expected Int", SCHEMA_TABLE_ROOTPAGE_COLIDX),
                };
                return Some(root_pagenum);
            }
        }
        None
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
        if !self.pages.contains_key(&pn) {
            // println!("Reading page {} on demand.", pn);
            let v = self.read_page_from_file(pn)?;
            self.pages.insert(pn, v.into()).expect("Should have inserted a page.");
        }
        assert!(self.pages.contains_key(&pn));

        Ok(())
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
        let maybe_page_ref = self.pages.get(&pn);
        println!("Found: {} PageNum: {}", maybe_page_ref.is_some(), pn);
        maybe_page_ref.ok_or(Error::Internal)
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
        self.pages.get_mut(&pn).ok_or(Error::Internal)
    }

    pub fn get_page_size(&self) -> u32 {
        self.page_size
    }

    // opens a table for reading.
    pub fn open_table_for_read(&self, table_name: &str) -> Result<StoredTable<'_>, Error> {
        let root_pagenum =
            self.get_root_pagenum(table_name).ok_or(Error::TableNameNotFound(table_name.to_owned()))?;
        let create_statement =
            self.get_creation_sql(table_name).ok_or(Error::TableNameNotFound(table_name.to_owned()))?;
        let cs = crate::pt_to_ast::pt_create_statement_to_ast(&create_statement);
        Ok(StoredTable::new(
            self,
            cs.tablename,
            root_pagenum,
            cs.coldefs.iter().map(|x| x.colname.name.clone()).collect(),
            cs.coldefs.iter().map(|x| SqlType::from_str(x.coltype.as_str()).unwrap()).collect(),
            cs.strict,
        ))    
    }

    pub fn main_schema(&self) -> Result<String, Error> {
        let mut result= String::new();
        let tt = self.open_table_for_read(SCHEMA_TABLE_NAME)
            .map_err(|_| Error::OpeningStoredTable)?
            .to_temp_table()
            .map_err(|_| Error::OpeningStoredTable)?;
        for row in tt.rows {
            result.push_str(&format!("{};", row.items[SCHEMA_TABLE_SQL_COLIDX]));
        }
        Ok(result)
    }

    /// Get the SQL CREATE statement used to create `table_name`, or None.
    pub fn get_creation_sql(&self, table_name: &str) -> Option<String> {
        if table_name == SCHEMA_TABLE_NAME {
            return Some(String::from(SCHEMA_SCHEMA));
        } else {
            let schema_table = StoredTable::new(
                self,
                String::from(SCHEMA_TABLE_NAME),
                SCHEMA_BTREE_ROOT_PAGENUM,
                SCHEMA_TABLE_COL_NAMES.iter().map(|x| x.to_string()).collect(),
                Vec::from(SCHEMA_TABLE_COL_TYPES),
                true,
            );   
            let mut it = schema_table.streaming_iterator();
            while let Some(row) = it.next() {
                let this_table_name = match &row.items[SCHEMA_TABLE_TBL_NAME_COLIDX] {
                    SqlValue::Text(s) => s.clone(),
                    _ => panic!("Type mismatch in schema table column {}, expected Text", SCHEMA_TABLE_TBL_NAME_COLIDX),
                };
                if this_table_name != table_name {
                    continue;
                }
                // TODO: refactor code below to "get row element as type x or return nicely formatted Error", which can be used elsewhere too.
                let creation_sql = match &row.items[SCHEMA_TABLE_SQL_COLIDX] {
                    SqlValue::Text(s) => s.clone(),
                    _ => panic!("Type mismatch in schema table column {}, expected Text", SCHEMA_TABLE_SQL_COLIDX),
                };
                return Some(creation_sql);
            }
        }
        None
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
    let _db = StoredDb::open(path.as_str()).expect("Should have opened db.");
}

#[test]
fn test_get_creation_sql() {
    let path = path_to_testdata("minimal.db");
    let db = StoredDb::open(path.as_str()).expect("Should have opened db.");
    let create = db.get_creation_sql("a").expect("Should have looked up table.");
    assert_eq!(create.to_lowercase().replace("\n", " "), "create table a ( b int )")
}

#[test]
fn test_root_pagenum() {
    let path = path_to_testdata("minimal.db");
    let db = StoredDb::open(path.as_str()).expect("Should have opened db.");
    let pn = db.get_root_pagenum("a").expect("Should have looked up table.");
    assert_eq!(pn, 2);
    let pn = db.get_root_pagenum("sqlite_schema").expect("Should have looked up table.");
    assert_eq!(pn, 1);
}

#[test]
fn test_get_page_rw() {
    let path = path_to_testdata("minimal.db");
    let mut pager = StoredDb::open(path.as_str()).expect("Should have opened db.");
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
    let pager = StoredDb::open(path.as_str()).expect("Should have opened db.");
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

// test of reading schema with multiple tables.
#[test]
fn test_get_creation_sql_and_root_pagenum_using_schematable_db() {
    let path = path_to_testdata("schema_table.db");
    let db =
        crate::stored_db::StoredDb::open(path.as_str()).expect("Should have opened db with pager.");
    let cases = vec![
        ("t1", 2, "create table t1 (a int)"),
        ("t2", 3, "create table t2 (a int, b int)"),
        (
            "t3",
            4,
            "create table t3 (a text, b int, c text, d int, e real)",
        ),
    ];
    for (tablename, actual_pgnum, actual_csql) in cases {
        let csql = db.get_creation_sql(tablename).expect("Should have found table's creation sql.");
        let pgnum = db.get_root_pagenum(tablename).expect("Should have found table's root page.");
        assert_eq!(pgnum, actual_pgnum);
        assert_eq!(csql.to_lowercase().replace('\n', " "), actual_csql);
    }
}

// Testing: Borrow check fails for multiple writers or read and write as expected.  This is tested in doc comments at the top of the file.