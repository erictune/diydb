//! `ir` defines type for an intermediate representation (IR) for SQL statements.
//!
//! The AST and IR are separate. [article about separating them](https://www.querifylabs.com/blog/relational-operators-in-apache-calcite)
//! The IR more closely represents the querying procedure (e.g. scan vs key lookup), and dependes on the availability of indicies,
//! the sizes of tables being joined, etc.
//! As an example, begin with these definitions:
//!
//! ```sql
//! create table t (a int, b int);
//! insert into t values (1,10);
//! insert into t values (2,20);
//! insert into t values (3,30);
//! ```
//! This SQL statement: `select * from t where a = 1` might have this IR:
//! ```text
//! Filter(                     // remove non-matching return matching rows.
//!     ColExpr(                // matching expression to execute on each row.
//!         Eq(
//!             Col("a"),
//!             IntConst(1)
//!         ),
//!     Scan(t)                 // Consider every row of the table.
//! )
//! ```
//! But if a relevant index is available:
//! ```sql
//! create index t_a on t (a);
//! ```
//! Then the IR can be optimized to this form:
//! ```text
//! `IndexSeekEq(               // Return only those rows with a particular key
//!     "t",                    // from this table
//!     "t_a",                  // using this index to lookup the key
//!     1                       // Looking up this key.
//! )`
//!
//! ### Design questions
//! * Are locks going to be acquired when building the IR, or only when evaluating it?
//!     * If during build, will that hold them longer than necessary?
//!     * If during eval, then what if the schema changes after build, then build cannot rely on schema?
//!     * May need to just lock the schema table, or hold a version number of the schema table to
//!     * We will solve this later on.

use crate::ast;
use std::boxed::Box;

/// `Block` represents any of the IR blocks that can be chained together.
/// A Block takes rows in from 0, one or more sources, and emits rows to a parent block.
#[derive(Debug, Clone, PartialEq)]
pub enum Block {
    Scan(Scan),
    Project(Project),
    ConstantRow(ConstantRow),
}

/// `ConstantRow` represents a table that has one row.
#[derive(Debug, Clone, PartialEq)]
pub struct ConstantRow {
    pub row: Vec<ast::Constant>,
}

/// `Scan` represents a one-pass scan over all the rows of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scan {
    pub tablename: String,
}

/// `Project` represents the projection operation: taking a subset of columns, and computing new columns.
#[derive(Debug, Clone, PartialEq)]
pub struct Project {
    pub outcols: Vec<ast::SelItem>,
    pub input: Box<Block>,
}
