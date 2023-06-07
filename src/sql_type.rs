//! Defines an enum of the 4 basic SQL supported column types and routines for conversion to and from string.
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// These are the basic types that a SQL value can have.
/// They correspond to the possible return values of `typeof()` in sqlite3.
/// Notes:
///   - In sqlite, `typeof(true)` is `integer`.
///   - SQLite supports type name aliases like `varchar` for `text` in create statements, but does not
///     values have the canonical type.
pub enum SqlType {
    Int,
    Text,
    Blob,
    Real,
    Null,
}

impl std::fmt::Display for SqlType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlType::Int => "integer".fmt(f),
            SqlType::Text => "text".fmt(f),
            SqlType::Blob => "blob".fmt(f),
            SqlType::Real => "real".fmt(f),
            SqlType::Null => "null".fmt(f),
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
        match s.to_lowercase().as_str() {
            "int" | "integer" => Ok(SqlType::Int),
            "text" | "string" => Ok(SqlType::Text),
            "blob" => Ok(SqlType::Blob),
            "real" => Ok(SqlType::Real),
            "null" => Ok(SqlType::Null),
            x => Err(Error::ParseSqlTypeError(String::from(x))),
        }
    }
}

use crate::ast;
pub fn from_ast_constant(c: &ast::Constant) -> SqlType {
    match c {
        ast::Constant::Int(_) => SqlType::Int,
        ast::Constant::String(_) => SqlType::Text,
        ast::Constant::Real(_) => SqlType::Real,
        ast::Constant::Bool(_) => SqlType::Int,
        ast::Constant::Null() => SqlType::Null,
    }
}
