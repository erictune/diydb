//! Traits common to Table and TempTable.

use crate::sql_type::SqlType;

pub trait TableMeta {
    fn column_names(&self) -> Vec<String>;
    fn column_types(&self) -> Vec<SqlType>;
    //pub fn tablename(&self) -> Vec<String> {
    //    self.column_names.clone()
    //}
}