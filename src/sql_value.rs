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
            SqlValue::Int(x) => write!(f, "{}", x),
            SqlValue::Text(x) => write!(f, "{}", x),
            SqlValue::Blob(_) => write!(f, "<BLOB>"),
            SqlValue::Real(x) => write!(f, "{}", x),
            SqlValue::Bool(x) => write!(f, "{}", x),
            SqlValue::Null() => write!(f, "NULL"),
        }
    }
}
