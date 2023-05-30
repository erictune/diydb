//! Defines an enum of all the possible values that a SQL value can have.

#[derive(Debug, Clone, PartialEq)]
/// can hold any value that can be stored in table.
/// Values are any of the 4 types that can be stored in a `sql_type::SqlType``, or `NULL`.
/// These types are sufficient to hold any of the  storage classes that SQLite files use.
pub enum SqlValue {
    Int(i64),
    Text(String),
    Blob(Vec<u8>),
    Real(f64),
    Bool(bool),
    Null(),
}

impl std::fmt::Display for SqlValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlValue::Int(x) => x.fmt(f),
            SqlValue::Text(x) => x.fmt(f),
            SqlValue::Blob(_) => "<BLOB>".fmt(f),
            SqlValue::Real(x) => x.fmt(f),
            SqlValue::Bool(x) => x.fmt(f),
            SqlValue::Null() => "NULL".fmt(f),
        }
    }
}
