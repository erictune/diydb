//! executes SQL intermediate representation (IR).

use anyhow::{Context, Result};

use crate::ir;
use crate::pager;
use crate::project;
use crate::sql_type;
use crate::sql_value;
use crate::table_traits::TableMeta;
use crate::table::Table;
use crate::typed_row::Row;
use crate::TempTable;

fn is_temporary_table(tablename: &str) -> bool {
    // TODO: do this properly using parsing the db name separate from the table name, and 
    // check checking if the DB is called "temp".
    println!("{} {}", tablename, "temp");
    tablename == "temp"
}

/// Run an IR representation of a query, returning a TempTable with the results of the query.
pub fn run_ir(ps: &pager::PagerSet, ir: &ir::Block) -> Result<crate::TempTable> {
    use streaming_iterator::StreamingIterator;

    match ir {
        ir::Block::Project(p) => {
            let child = p
                .input
                .as_scan()
                .context("Project should only have Scan as child")?;
            let (rows, column_names, column_types) = match is_temporary_table(child.tablename.as_str()) {
                true => {
                    let tbl = ps.get_temp_table()?;
                    // TODO: reduce redundancy between this arm and the next  by making a generic function like this:
                    // `f<T>() where T: TableMeta + RowStream`, which does the steps in the remainder of this arm.
                    let (actions, column_names, column_types) =
                    project::build_project(&tbl.column_names(), &tbl.column_types(), &p.outcols)?;
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
                    (rows, column_names, column_types)
                }
                false => {
                    let tbl: Table<'_> = Table::open_read(ps.default_pager()?, child.tablename.as_str())?;
                    let (actions, column_names, column_types) =
                    project::build_project(&tbl.column_names(), &tbl.column_types(), &p.outcols)?;
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
                    (rows, column_names, column_types)
                }
            };
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
            match is_temporary_table(s.tablename.as_str()) {
                true => Ok(ps.get_temp_table()?.clone()),
                false => {
                // TODO: lock the table in the pager when opening the table for read.
                // TODO: if we previously loaded the schema speculatively during IR optimization, verify unchanged now, e.g. with hash.
                Table::open_read(ps.default_pager()?, s.tablename.as_str())?
                    .to_temp_table()
                    .map_err(|e| anyhow::anyhow!(e))
                }
            }
        }
    }
}