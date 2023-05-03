//! executes SQL intermediate representation (IR).

use crate::ir;
use crate::pager;
use crate::TempTable;
use anyhow::Result;

use crate::sql_type::SqlType;
use crate::sql_value::SqlValue;

use crate::ast;
use crate::table::Table;
use crate::typed_row::TypedRow;

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

pub fn run_ir(ps: &pager::PagerSet, ir: &ir::Block) -> Result<crate::TempTable> {
    match ir {
        // TODO support root Project blocks.  This requires printing rows that
        // have constant exprs, dropping rows, etc.
        // The right way to do is to have formatting::print_table accept different kinds of iterators?
        // Project can project without converting, so we should allow it to Project a Scan without converting?
        ir::Block::Project(_) => {
            // TODO: Project needs a pointer to a Scan.  For now, we will only support Project of Scan.
            panic!("IR that uses Project not supported yet.");
        }
        ir::Block::ConstantRow(cr) => Ok(TempTable {
            rows: vec![TypedRow {
                row_id: 1,
                items: cr.row.iter().map(ast_constant_to_sql_value).collect(),
            }],
            column_names: (0..cr.row.len()).map(|i| format!("_f{i}")).collect(),
            column_types: cr.row.iter().map(ast_constant_to_sql_type).collect(),
        }),
        ir::Block::Scan(s) => {
            // Question: what happens if the IR was built based on assumptions about the schema (e.g. number and types of columns),
            // and then the schema changed?  How about storing the message digest of the creation_sql in the Scan block and verify
            // it here.
            let table_name = s.tablename.as_str();
            let pager = ps.default_pager()?;
            let tbl = Table::open_read(pager, table_name)?;
            tbl.to_temp_table().map_err(anyhow::Error::msg)
        }
    }
}
