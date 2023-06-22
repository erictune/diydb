//! Traits common to Table and TempTable.

use crate::sql_type::SqlType;

pub trait TableMeta {
    /// Names of each column, excluding the table name. 
    fn column_names(&self) -> Vec<String>;
    /// Type of value to be stored in each column, per the schema for this table.
    fn column_types(&self) -> Vec<SqlType>;
    /// Name of the table.
    fn table_name(&self) -> String;
    /// True if the table is 'STRICT' about rows matching the schema.
        /// True if SQLite strict mode is enforced on this table.
    ///
    /// When "strict mode is not set, SQLite does not check if column types match.  For example:
    /// ```shell
    /// sqlite> create temp table t (a int);
    /// sqlite> insert into t values ("foo");
    /// sqlite> select * from t;
    /// foo
    /// ```shell
    /// When it is set, SQLite rejects mismatched columns. For example:
    /// ```
    /// sqlite> create temp table t2 (a int) strict;
    /// sqlite> insert into t2 values ("foo");
    /// Runtime error: cannot store TEXT value in INT column t2.a (19)
    /// ```
    fn strict(&self) -> bool;

    // TODO: add "creation_sql()" as a default method.
}