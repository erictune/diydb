//! Defines `TempDB` type, used to hold the tables of a temporary database.
//! 

// TODO:
//  - Use OS locking to lock the opened database file.
//  - Support accessing pages for modification by locking the entire Pager.
//  - Support concurrent access for read and write via table or page-level locking.
//  - Support adding pages to the database.
//  - Support reading pages on demand.
//  - Support dropping unused pages when memory is low.
//  - When there are multiple pagers (multiple open files), coordinating to stay under a total memory limit.

use crate::temp_table::TempTable;
use crate::sql_type::SqlType;
use crate::table_traits::TableMeta;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Table name not found.")]
    TableNameNotFound,
}

/// A `TempDb` holds one temporary database.
///
/// The temporary database is a collection of tables of type `TempTable`.  These have a lifetime limited to the duration of the execution
/// of the program.
/// 
/// # TODOs
///   - After introducing a connection concept, consider whether TempTables are global to the server, or local to a Connection.
///
pub struct TempDb {
    temp_tables: Vec<crate::temp_table::TempTable>, 
}

impl TempDb {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        TempDb { 
            temp_tables: vec![], // TODO: key by name.
        }
    }
    pub fn new_temp_table(&mut self, table_name: String, column_names: Vec<String>, column_types: Vec<SqlType>, strict: bool) -> Result<(), Error> {
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

    pub fn get_temp_table(&self, tablename: &String) -> Result<&crate::temp_table::TempTable, Error> {
        for i in 0..self.temp_tables.len() {
            if self.temp_tables[i].table_name() == *tablename {
                return Ok(&self.temp_tables[i]);
            }
        }
        Err(Error::TableNameNotFound)
    }

    pub fn get_temp_table_mut(&mut self, tablename: &String) -> Result<&mut crate::temp_table::TempTable, Error> {
        for i in 0..self.temp_tables.len() {
            if self.temp_tables[i].table_name() == *tablename {
                return Ok(&mut self.temp_tables[i]);
            }
        }
        Err(Error::TableNameNotFound)
    }

    pub fn temp_schema(&self) -> Result<String, Error> {
        let mut result= String::new();
        for tt in self.temp_tables.iter() {
            result.push_str(&format!("{}", tt.creation_sql()));
        }
        Ok(result)
    }

    // TODO: move to "db traits"?
    pub fn get_creation_sql(&self, table_name: &str) -> Option<String> {
        for tt in self.temp_tables.iter() {
            if tt.table_name() == table_name {
                return Some(tt.creation_sql());
            }
        }
        None
    }
} 