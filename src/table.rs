//! represents access to a file-backed SQLite database table.

use crate::typed_row::{RawRowCaster, RowCastingError, TypedRow};
use crate::{pager, sql_type::SqlType};
use std::str::FromStr;

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

impl<'a> Table<'a> {
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

    // TODO: hide this internal type using an impl Iterator or a simple wrapper?
    pub fn iter(&self) -> crate::btree::table::Iterator {
        crate::btree::table::Iterator::new(self.root_pagenum, self.pager)
    }

    pub fn to_temp_table(&self) -> core::result::Result<crate::TempTable, Error> {
        let r: Result<Vec<TypedRow>, RowCastingError> =
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
