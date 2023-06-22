mod ast;
mod ast_to_ir;
mod btree;
mod dbheader;
mod ir;
mod ir_interpreter;
mod optimize_ast;
pub mod stored_db;
pub mod parser;
mod project;
mod pt_to_ast;
mod record;
mod serial_type;
pub mod sql_type;
pub mod sql_value;
mod table_traits;
mod temp_db;
mod stored_table;
mod temp_table;
pub mod typed_row;
extern crate pest;
#[macro_use]
extern crate pest_derive;

use anyhow::bail;
use std::str::FromStr;

use sql_value::SqlValue;
use temp_table::TempTable;
use typed_row::Row;

// DbServerState holds the context of running database engine: hold the open persistent and temporary databases.
pub struct DbServerState {
    pub stored_db: Option<crate::stored_db::StoredDb>,  // Try to make this private.
    pub temp_db: crate::temp_db::TempDb,
}

impl DbServerState {
    pub fn new() -> DbServerState {
        DbServerState { 
            stored_db: None,
            temp_db: crate::temp_db::TempDb::new(),
        }
    }
}
// Open a database file, and hold it in the DbServerState.
pub fn open_db(server_state: &mut DbServerState, path: &str) -> anyhow::Result<()> {
    if server_state.stored_db.is_some() { bail!("Database file already open.  Close the old one first.  Close might be supported in the future.")}
    server_state.stored_db = Some(crate::stored_db::StoredDb::open(path)?);
    Ok(())
}

pub fn new_table_iterator(pgr: &stored_db::StoredDb, pgnum: usize) -> btree::table::Iterator {
    crate::btree::table::Iterator::new(pgnum, pgr)
}

/// Print the Schema table to standard output.
pub fn print_schema(server_state: &DbServerState) -> anyhow::Result<()> {
    // Print temp database and main database if open; we only support these two kinds of dbs.
    println!("{}", server_state.temp_db.temp_schema()?);
    if server_state.stored_db.is_some() {
        println!("{}", server_state.stored_db.as_ref().unwrap().main_schema()?);
    }
    Ok(())
}

pub fn run_query(server_state: &DbServerState, query: &str) -> anyhow::Result<()> {
    let tt = run_query_no_print(server_state, query)?;
    tt.print(false)?;
    Ok(())
}

pub fn run_insert(server_state: &mut DbServerState, stmt: &str) -> anyhow::Result<()> {
    let is: ast::InsertStatement = pt_to_ast::pt_insert_statement_to_ast(stmt)?;
    // TODO: use helper functions or "impl Trait" argument types to reduce how much code is duplicated
    // across these two match arms.
    match is.databasename == "temp" {
        true /* temporary table */ => {
            let tbl = server_state.temp_db.get_temp_table_mut(&is.tablename)?;
            for row in is.values {
                // Convert row from AST constants to SQL values.
                let row: Vec<SqlValue> = row.iter().map(sql_value::from_ast_constant).collect();
                tbl.append_row(&row)?;
            }
            // Writing to disk not needed for temp tables.
        }
        false /* Persistent, SQLite table */ => {
            bail!("Inserting into persistent (SQLite-format) tables is not supported yet.  Try a temporary table.");
        }
    }
    Ok(())
}

pub fn run_create(server_state: &mut DbServerState, stmt: &str) -> anyhow::Result<()> {
    let cs: ast::CreateStatement = pt_to_ast::pt_create_statement_to_ast(stmt);
    // TODO: use helper functions or "impl Trait" argument types to reduce how much code is duplicated
    // across these two match arms.
    match cs.databasename == "temp" {
        true /* temporary table */ => {            
            server_state.temp_db.new_temp_table(
                cs.tablename,
                cs.coldefs.iter().map(|x| x.colname.name.clone()).collect(),
                cs.coldefs.iter().map(|x| sql_type::SqlType::from_str(x.coltype.as_str()).unwrap()).collect(),
                cs.strict,
            )?;
        }
        false /* Persistent, SQLite table */ => {
            bail!("Creation of persistent (SQLite-format) tables is not supported yet.  Try 'CREATE TEMP TABLE ...;' instead.");
        }
    }
    Ok(())
}


pub fn run_query_no_print(server_state: &DbServerState, query: &str) -> anyhow::Result<TempTable> {
    // Convert parse tree to AST.
    let mut ss: ast::SelectStatement = pt_to_ast::pt_select_statement_to_ast(query)?;
    // Optimize the AST (in place).
    optimize_ast::simplify_ast_select_statement(&mut ss)?;
    // Convert the AST to IR.
    let ir: ir::Block = ast_to_ir::ast_select_statement_to_ir(&ss)?;
    // Execute the IR.
    let tt: TempTable = ir_interpreter::run_ir(server_state, &ir)?;
    Ok(tt)
}
