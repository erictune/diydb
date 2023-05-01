//! Defines an enum of the 4 basic SQL supported column types and routines for conversion to and from string.
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq)]
/// These are the SQL Column types that we support.
/// These are believed to match the basic types that SQLite supports for `CREATE TABLE ... STRICT;` format.
/// In particular, `BOOL` is not a distinct type.
/// SQLite supports type name aliases like `INTEGER` for `INT`.  Those are not supported here.
pub enum SqlType {
    Int,
    Text,
    Blob,
    Real,
}

impl std::fmt::Display for SqlType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlType::Int => write!(f, "INT"),
            SqlType::Text => write!(f, "TEXT"),
            SqlType::Blob => write!(f, "BLOB"),
            SqlType::Real => write!(f, "REAL"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParseSqlTypeError;

impl FromStr for SqlType {
    type Err = ParseSqlTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "INT" => Ok(SqlType::Int),
            "TEXT" => Ok(SqlType::Text),
            "BLOB" => Ok(SqlType::Blob),
            "REAL" => Ok(SqlType::Real),
            _ => Err(ParseSqlTypeError),
        }
    }
}
