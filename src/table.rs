//! represents access to a file-backed SQLite database table.

use std::str::FromStr;
use streaming_iterator::StreamingIterator;

use crate::typed_row::{RawRowCaster, Row, RowCastingError};
use crate::{pager, sql_type::SqlType};



pub struct Table<'a> {
    pager: &'a pager::Pager,
    _table_name: String,
    root_pagenum: pager::PageNum,
    column_names: Vec<String>,
    column_types: Vec<SqlType>,
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum Error {
    #[error("The table {0} was not found in the database.")]
    TableNameNotFoundInDb(String),
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
    fn new(it: crate::btree::table::Iterator<'p>, column_types: Vec<SqlType>) -> TableStreamingIterator<'p> {
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
            Some(raw) =>  Some(crate::typed_row::build_row(&self.column_types, raw.1).expect("Should have cast the row.")), // TODO: pass through errors?
        }
    }

    #[inline]
    fn get(&self) -> Option<&Self::Item> {
        self.item.as_ref()
    }
}

impl<'a> Table<'a> {
    pub fn column_names(&self) -> Vec<String> {
        self.column_names.clone()
    }

    pub fn column_types(&self) -> Vec<SqlType> {
        self.column_types.clone()
    }
    
    pub fn open_read(pager: &'a pager::Pager, table_name: &str) -> Result<Table<'a>, Error> {
        let (root_pagenum, create_statement) =
            match crate::get_creation_sql_and_root_pagenum(pager, table_name) {
                Some(x) => x,
                None => return Err(Error::TableNameNotFoundInDb(String::from(table_name))),
            };
        let (_, column_names, column_types) =
            crate::pt_to_ast::parse_create_statement(&create_statement);
        Ok(Table {
            pager,
            _table_name: String::from(table_name),
            root_pagenum,
            column_names,
            column_types: column_types
                .iter()
                .map(|s| SqlType::from_str(s.as_str()).unwrap())
                .collect(),
        })
    }

    pub fn streaming_iterator(&'a self) -> TableStreamingIterator<'a>
    {
        TableStreamingIterator::new(self.iter(), self.column_types())
    }

    // TODO: hide this internal type using an impl Iterator or a simple wrapper?
    fn iter(&self) -> crate::btree::table::Iterator {
        crate::btree::table::Iterator::new(self.root_pagenum, self.pager)
    }

    pub fn to_temp_table(&self) -> core::result::Result<crate::TempTable, Error> {
        let r: Result<Vec<Row>, RowCastingError> =
            RawRowCaster::new(self.column_types.clone(), &mut self.iter()).collect();
        let r = match r {
            Err(_) => return Err(Error::CastingError),
            Ok(r) => r,
        };
        Ok(crate::TempTable {
            // TODO: take() a limited number of rows when collect()ing them, and return error if they don't fit?
            rows: r,
            column_names: self.column_names.clone(),
            column_types: self.column_types.clone(),
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
    use crate::sql_value::SqlValue;
    use crate::sql_type::SqlType;
    let path = path_to_testdata("minimal.db");
    let pager =
        crate::pager::Pager::open(path.as_str()).expect("Should have opened db with pager.");
    let tbl = Table::open_read(&pager, "a").expect("Should have opened db.");
    assert_eq!(tbl.column_names(), vec![String::from("b")]);
    assert_eq!(tbl.column_types(), vec![SqlType::Int]);
    let mut it = tbl.streaming_iterator();
    it.advance();
    assert_eq!(it.get(), Some(&Row{ items: vec![SqlValue::Int(1)]}));
    it.advance();
    assert_eq!(it.get(), None);

}
