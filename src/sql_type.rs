//! Defines an enum of the 4 basic SQL supported column types and routines for conversion to and from string.
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
            SqlType::Int => "INT".fmt(f),
            SqlType::Text => "TEXT".fmt(f),
            SqlType::Blob => "BLOB".fmt(f),
            SqlType::Real => "REAL".fmt(f),
        }
    }
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
    #[error("Unable to parse SqlType from creation SQL: {0}.")]
    ParseSqlTypeError(String),
}

impl FromStr for SqlType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "INT" | "INTEGER" => Ok(SqlType::Int),
            "TEXT" | "STRING" => Ok(SqlType::Text),
            "BLOB" => Ok(SqlType::Blob),
            "REAL" => Ok(SqlType::Real),
            x => Err(Error::ParseSqlTypeError(String::from(x))),
        }
    }
}
