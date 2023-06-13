mod ast;
mod ast_to_ir;
mod btree;
mod dbheader;
mod ir;
mod ir_interpreter;
mod optimize_ast;
pub mod pager;
pub mod parser;
mod project;
mod pt_to_ast;
mod record;
mod serial_type;
pub mod sql_type;
pub mod sql_value;
mod table_traits;
mod table;
mod temp_table;
pub mod typed_row;
extern crate pest;
#[macro_use]
extern crate pest_derive;

use anyhow::bail;
use sql_type::SqlType;
use sql_value::SqlValue;
use table::Table;
use temp_table::TempTable;
use typed_row::Row;

use streaming_iterator::StreamingIterator;

// Page 1 (the first page) is always a btree page, and it is the root page of the schema table.
// It has references to the root pages of other btrees.
const SCHEMA_TABLE_NAME: &str = "sqlite_schema";
const SCHEMA_BTREE_ROOT_PAGENUM: pager::PageNum = 1;
const SCHEMA_SCHEMA: &str =
    "CREATE TABLE sqlite_schema (type text, name text, tbl_name text, rootpage integer, sql text)";
const SCHEMA_TABLE_COL_NAMES: [&str; 5] = ["type", "name", "tbl_name", "rootpage", "sql"];
const SCHEMA_TABLE_COL_TYPES: [SqlType; 5] = [SqlType::Text, SqlType::Text, SqlType::Text, SqlType::Int, SqlType::Text];
const SCHEMA_TABLE_TBL_NAME_COLIDX: usize = 2;
const SCHEMA_TABLE_ROOTPAGE_COLIDX: usize = 3;
const SCHEMA_TABLE_SQL_COLIDX: usize = 4;

/// Get the root page number for, and the SQL CREATE statement used to create `table_name`.
pub fn get_creation_sql_and_root_pagenum(
    pgr: &pager::Pager,
    table_name: &str,
) -> Option<(pager::PageNum, String)> {
    if table_name == SCHEMA_TABLE_NAME {
        return Some((SCHEMA_BTREE_ROOT_PAGENUM, String::from(SCHEMA_SCHEMA)));
    } else {
        let schema_table = Table::new(
            pgr,
            String::from(SCHEMA_TABLE_NAME),
            SCHEMA_BTREE_ROOT_PAGENUM,
            SCHEMA_TABLE_COL_NAMES.iter().map(|x| x.to_string()).collect(),
            Vec::from(SCHEMA_TABLE_COL_TYPES),
        );   
        let mut it = schema_table.streaming_iterator();
        while let Some(row) = it.next() {
            let this_table_name = match &row.items[SCHEMA_TABLE_TBL_NAME_COLIDX] {
                SqlValue::Text(s) => s.clone(),
                _ => panic!("Type mismatch in schema table column {}, expected Text", SCHEMA_TABLE_TBL_NAME_COLIDX),
            };
            if this_table_name != table_name {
                continue;
            }
            // TODO: refactor code below to "get row element as type x or return nicely formatted Error", which can be used elsewhere too.
            let root_pagenum = match &row.items[SCHEMA_TABLE_ROOTPAGE_COLIDX] {
                SqlValue::Int(i) => *i as pager::PageNum,
                // TODO: return Result rather than panicing.
                _ => panic!("Type mismatch in schema table column {}, expected Int", SCHEMA_TABLE_ROOTPAGE_COLIDX),
            };
            let creation_sql = match &row.items[SCHEMA_TABLE_SQL_COLIDX] {
                SqlValue::Text(s) => s.clone(),
                _ => panic!("Type mismatch in schema table column {}, expected Text", SCHEMA_TABLE_SQL_COLIDX),
            };
            return Some((root_pagenum, creation_sql));
        }
    }
    None
}

pub fn new_table_iterator(pgr: &pager::Pager, pgnum: usize) -> btree::table::Iterator {
    crate::btree::table::Iterator::new(pgnum, pgr)
}

/// Print the Schema table to standard output.
pub fn print_schema(pager: &pager::Pager) -> anyhow::Result<()> {
    let tbl = Table::open_read(pager, SCHEMA_TABLE_NAME)?;
    let tt: TempTable = tbl.to_temp_table()?;
    tt.print(false)?;
    Ok(())
}

pub fn run_query(ps: &pager::PagerSet, query: &str) -> anyhow::Result<()> {
    let tt = run_query_no_print(ps, query)?;
    tt.print(false)?;
    Ok(())
}

pub fn run_insert(_ps: &pager::PagerSet, stmt: &str) -> anyhow::Result<()> {
    let _is: ast::InsertStatement = pt_to_ast::pt_insert_statement_to_ast(stmt)?;
    bail!("Not implemented yet.")
}

pub fn run_query_no_print(ps: &pager::PagerSet, query: &str) -> anyhow::Result<TempTable> {
    // Convert parse tree to AST.
    let mut ss: ast::SelectStatement = pt_to_ast::pt_select_statement_to_ast(query)?;
    // Optimize the AST (in place).
    optimize_ast::simplify_ast_select_statement(&mut ss)?;
    // Convert the AST to IR.
    let ir: ir::Block = ast_to_ir::ast_select_statement_to_ir(&ss)?;
    // Execute the IR.
    let tt: TempTable = ir_interpreter::run_ir(ps, &ir)?;
    Ok(tt)
}
