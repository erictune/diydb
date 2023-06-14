//! Traits common to Table and TempTable.

use crate::sql_type::SqlType;

pub trait TableMeta {
    /// Names of each column, excluding the table name. 
    fn column_names(&self) -> Vec<String>;
    /// Type of value to be stored in each column, per the schema for this table.
    fn column_types(&self) -> Vec<SqlType>;
    /// Name of the table.
    fn table_name(&self) -> String;
}