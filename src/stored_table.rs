//! represents access to a file-backed SQLite database table.
//! 
//! Currently, only reading is supported.
//! A subset of the SQLite file format is supported.

use crate::table_traits::TableMeta;
use crate::typed_row::Row;
use crate::stored_db;
use crate::sql_type::SqlType;
use streaming_iterator::StreamingIterator;

pub struct StoredTable<'a> {
    pager: &'a stored_db::StoredDb,
    table_name: String,
    root_pagenum: stored_db::PageNum,
    column_names: Vec<String>,
    column_types: Vec<SqlType>,
    strict: bool,
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum Error {
    #[error("While converting persistent table to a temporary table, type casting failure.")]
    CastingError,
}

/// iterates over the rows of a TempTable .
/// The lifetime 'p is the lifetime of the pager used in the table::Iterator.
///
pub struct TableStreamingIterator<'p> {
    // Implementation note: Tried by could not get streaming_iterator::Convert
    // to work: because inscrutible compiler error when used with a non-default lifetime.
    // Also, we want to convert from raw data to typed data in the process.
    it: crate::btree::table::Iterator<'p>,
    column_types: Vec<SqlType>,
    raw_item: Option<<crate::btree::table::Iterator<'p> as IntoIterator>::Item>,
    item: Option<Row>,
}
impl<'p> TableStreamingIterator<'p> {
    fn new(
        it: crate::btree::table::Iterator<'p>,
        column_types: Vec<SqlType>,
    ) -> TableStreamingIterator<'p> {
        TableStreamingIterator {
            it,
            column_types,
            raw_item: None,
            item: None,
        }
    }
}

impl<'p> StreamingIterator for TableStreamingIterator<'p> {
    type Item = Row;

    #[inline]
    fn advance(&mut self) {
        self.raw_item = self.it.next();
        self.item = match self.raw_item {
            None => None,
            Some(raw) => Some(
                crate::typed_row::from_serialized(&self.column_types, raw.1)
                    .expect("Should have cast the row."),
            ), // TODO: pass through errors?
        }
    }

    #[inline]
    fn get(&self) -> Option<&Self::Item> {
        self.item.as_ref()
    }
}

impl<'a> TableMeta for StoredTable<'a> {
    fn column_names(&self) -> Vec<String> {
        self.column_names.clone()
    }
    fn column_types(&self) -> Vec<SqlType> {
        self.column_types.clone()
    }
    fn table_name(&self) -> String {
        self.table_name.clone()
    }
    fn strict(&self) -> bool {
        self.strict
    }
}

impl<'a> StoredTable<'a> {

    /// creates a Table for unspecified (read vs write).
    /// Note: Most use cases should use open_read(), not new()
    pub fn new(
        pager: &'a stored_db::StoredDb,
        table_name: String,
        root_pagenum: stored_db::PageNum,
        column_names: Vec<String>,
        column_types: Vec<SqlType>,
        strict: bool
    ) -> StoredTable<'a> {
        StoredTable {
            pager,
            table_name,
            root_pagenum,
            column_names,
            column_types,
            strict
        }
    }
    
    pub fn streaming_iterator(&'a self) -> TableStreamingIterator<'a> {
        TableStreamingIterator::new(self.iter(), self.column_types())
    }

    // TODO: hide this internal type using an impl Iterator or a simple wrapper?
    fn iter(&self) -> crate::btree::table::Iterator {
        crate::btree::table::Iterator::new(self.root_pagenum, self.pager)
    }

    pub fn to_temp_table(&self) -> core::result::Result<crate::TempTable, Error> {
        let mut rows: Vec<Row> = vec![];
        let mut it = self.iter();
        while let Some((_rowid, serialized_row)) = it.next() {
            if let Ok(row) = crate::typed_row::from_serialized(&self.column_types, serialized_row) {
                rows.push(row.clone());
            } else {
                return Err(Error::CastingError)
            }
        }
        Ok(crate::TempTable {
            // TODO: take() a limited number of rows when collect()ing them, and return error if they don't fit?
            rows,
            table_name: self.table_name.clone(),
            column_names: self.column_names.clone(),
            column_types: self.column_types.clone(),
            strict: self.strict(),
        })
    }
}

#[cfg(test)]
fn path_to_testdata(filename: &str) -> String {
    std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set")
        + "/resources/test/"
        + filename
}

#[test]
fn test_table() {
    use crate::sql_type::SqlType;
    use crate::sql_value::SqlValue;
    let path = path_to_testdata("minimal.db");
    let db =
        crate::stored_db::StoredDb::open(path.as_str()).expect("Should have opened db.");
    let tbl = db.open_table_for_read("a").expect("Should have opened db.");
    assert_eq!(tbl.column_names(), vec![String::from("b")]);
    assert_eq!(tbl.column_types(), vec![SqlType::Int]);
    let mut it = tbl.streaming_iterator();
    it.advance();
    assert_eq!(
        it.get(),
        Some(&Row {
            items: vec![SqlValue::Int(1)]
        })
    );
    it.advance();
    assert_eq!(it.get(), None);
}