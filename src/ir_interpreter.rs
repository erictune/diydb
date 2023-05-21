//! executes SQL intermediate representation (IR).

use anyhow::Result;

use crate::ast;
use crate::ir;
use crate::pager;
use crate::sql_type::SqlType;
use crate::sql_value::SqlValue;
use crate::TempTable;
use crate::table::Table;
use crate::typed_row::Row;

fn ast_constant_to_sql_value(c: &ast::Constant) -> SqlValue {
    match c {
        ast::Constant::Int(i) => SqlValue::Int(*i),
        ast::Constant::String(s) => SqlValue::Text(s.clone()),
        ast::Constant::Real(f) => SqlValue::Real(*f),
        ast::Constant::Bool(b) => SqlValue::Int(match b {
            true => 1,
            false => 0,
        }),
        ast::Constant::Null() => SqlValue::Null(),
    }
}

fn ast_constant_to_sql_type(c: &ast::Constant) -> SqlType {
    match c {
        ast::Constant::Int(_) => SqlType::Int,
        ast::Constant::String(_) => SqlType::Text,
        ast::Constant::Real(_) => SqlType::Real,
        ast::Constant::Bool(_) => SqlType::Int,
        ast::Constant::Null() => SqlType::Int, // Not clear what to do in this case.  Need Unknown type?
    }
}

/// Run an IR representation of a query, returning a TempTable with the results of the query.
pub fn run_ir(ps: &pager::PagerSet, ir: &ir::Block) -> Result<crate::TempTable> {
    match ir {
        ir::Block::Project(_) => {
            // TODO: project should only contain Scan, not ConstantRow, for now?  So can use
            // let input_columns = p.input.as_scan().map_err(...).column_names();
            // and so on.
            return Err(anyhow::anyhow!("IR that uses Project not supported yet."))
        }
        ir::Block::ConstantRow(cr) => {
            return Ok(TempTable {
            rows: vec![Row {
                items: cr.row.iter().map(ast_constant_to_sql_value).collect(),
            }],
            column_names: (0..cr.row.len()).map(|i| format!("_f{i}")).collect(),
            column_types: cr.row.iter().map(ast_constant_to_sql_type).collect(),
            });
        }
        ir::Block::Scan(s) => {
            // TODO: lock table at this point so schema does not change.
            // TODO: if we previously loaded the schema speculatively during IR optimization, verify unchanged now, e.g. using a message hash.
            Table::open_read(ps.default_pager()?, s.tablename.as_str())?
                .to_temp_table()
                .map_err(|e| anyhow::anyhow!(e))
        }
    }
}
