//! executes SQL intermediate representation (IR).

use anyhow::{Context, Result};
use streaming_iterator::StreamingIterator;

use crate::ast;
use crate::ir;
use crate::pager;
use crate::project;
use crate::sql_type;
use crate::sql_value;
use crate::table_traits::TableMeta;
use crate::stored_table::StoredTable;
use crate::typed_row::Row;
use crate::TempTable;

fn project_any_table_into_temp_table<T, I>(in_tbl: &T, in_it: I, out_cols: &[ast::SelItem]) -> Result<crate::TempTable>
where
    T: TableMeta,
    I: StreamingIterator<Item = Row>,
{
    let (actions, column_names, column_types) =
    project::build_project(&in_tbl.column_names(), &in_tbl.column_types(), out_cols)?;
    let mut it = in_it.map(|row| project::project_row(&actions, row));
    let mut rows: Vec<Row> = vec![];
    loop {
        it.advance();
        if it.get().is_none() {
            break;
        }
        let res = it.get().unwrap().as_ref();
        match res {
            Err(e) => {
                return Err(anyhow::anyhow!(format!("Not able to convert value: {}", e)))
            }
            Ok(r) => rows.push(r.clone()),
        }
    }
    Ok(TempTable {
        rows,
        table_name: String::from("?unnamed?"),
        column_names,
        column_types,
        strict: false,  // SQLite defaults to non-strict, so result tables (without an explicit CREATE) shall be non-strict.
    })
}

/// Run an IR representation of a query, returning a TempTable with the results of the query.
pub fn run_ir(server_state: &crate::DbServerState, ir: &ir::Block) -> Result<crate::TempTable> {
    let ps = &server_state.pager_set;
    match ir {
        ir::Block::Project(p) => {
            let child = p
                .input
                .as_scan()
                .context("Project should only have Scan as child")?;
            match child.databasename == "temp" {
                true => {
                    let tbl = ps.get_temp_table(&child.tablename)?;
                    let base_it = tbl.streaming_iterator();
                    project_any_table_into_temp_table(tbl, base_it, &p.outcols)
                }
                false => {
                    // TODO: this should be a reference to a Table held by the DB, not a Table created here on the stack.
                    let tbl: StoredTable<'_> = StoredTable::open_read(ps.default_pager()?, child.tablename.as_str())?;
                    let base_it = tbl.streaming_iterator();
                    project_any_table_into_temp_table(&tbl, base_it, &p.outcols)
                }
            }
        }
        ir::Block::ConstantRow(cr) => {
            return Ok(TempTable {
                rows: vec![Row {
                    items: cr.row.iter().map(sql_value::from_ast_constant).collect(),
                }],
                table_name: String::from("?unnamed?"),
                column_names: (0..cr.row.len()).map(|i| format!("_f{i}")).collect(),
                column_types: cr.row.iter().map(sql_type::from_ast_constant).collect(),
                strict: false,  // SQLite defaults to non-strict, so result tables (without an explicit CREATE) shall be non-strict.
            });
        }
        ir::Block::Scan(s) => {
            match s.databasename == "temp" {
                true => Ok(ps.get_temp_table(&s.tablename)?.clone()),
                false => {
                // TODO: lock the table in the pager when opening the table for read.
                // TODO: if we previously loaded the schema speculatively during IR optimization, verify unchanged now, e.g. with hash.
                StoredTable::open_read(ps.default_pager()?, s.tablename.as_str())?
                    .to_temp_table()
                    .map_err(|e| anyhow::anyhow!(e))
                }
            }
        }
    }
}