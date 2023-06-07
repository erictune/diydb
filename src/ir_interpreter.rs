//! executes SQL intermediate representation (IR).

use anyhow::{Context, Result};

use crate::ir;
use crate::pager;
use crate::project;
use crate::sql_type;
use crate::sql_value;
use crate::table::Table;
use crate::typed_row::Row;
use crate::TempTable;

/// Run an IR representation of a query, returning a TempTable with the results of the query.
pub fn run_ir(ps: &pager::PagerSet, ir: &ir::Block) -> Result<crate::TempTable> {
    match ir {
        ir::Block::Project(p) => {
            let child = p
                .input
                .as_scan()
                .context("Project should only have Scan as child")?;
            let tbl = Table::open_read(ps.default_pager()?, child.tablename.as_str())?;
            let (actions, column_names, column_types) =
                project::build_project(&tbl.column_names(), &tbl.column_types(), &p.outcols)?;
            use streaming_iterator::StreamingIterator;
            let mut it = tbl
                .streaming_iterator()
                .map(|row| project::project_row(&actions, row));
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
                column_names,
                column_types,
            })
        }
        ir::Block::ConstantRow(cr) => {
            return Ok(TempTable {
                rows: vec![Row {
                    items: cr.row.iter().map(sql_value::from_ast_constant).collect(),
                }],
                column_names: (0..cr.row.len()).map(|i| format!("_f{i}")).collect(),
                column_types: cr.row.iter().map(sql_type::from_ast_constant).collect(),
            });
        }
        ir::Block::Scan(s) => {
            // TODO: lock the table in the pager when opening the table for read.
            // TODO: if we previously loaded the schema speculatively during IR optimization, verify unchanged now, e.g. with hash.
            Table::open_read(ps.default_pager()?, s.tablename.as_str())?
                .to_temp_table()
                .map_err(|e| anyhow::anyhow!(e))
        }
    }
}
