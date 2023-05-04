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
    // TODO: add an "expanded_outcols" which has * expanded.
    // TODO: walk down the IR and then initalize the blocks going upwards.  This may require having extra optional fields set here
    // at runtime as opposed to at ast_to_ir time.  These could be cleared to allow resetting the IR to run again?
    // TODO: We need to acquire tables as we initialize the blocks that use them.
    match ir {
        // TODO: support root Project blocks.  This requires printing rows that
        // have constant exprs, dropping rows, etc.
        ir::Block::Project(_) => {
            // TODO: return an error, e.g. with anyhow!(), if there is a star or column name but input.is_none().
            // TODO: expand stars to the list of input.column_names.  Here or in previous pass on the IR?
            // TODO: add a method to get the list of output column types to Project, Scan and ConstantRow which can fail if they are not runtime
            // initialized.
            // TODO: rename constantrow to constanttable since it could have several rows, like in `select * from (select 1 union select 2);`
            // TODO:  check each projected column to see if it is in the Scan's Tables
            //         panic!("Cannot select * without a FROM clause"),
            // TODO: we need a Projec Iterator that returns rows that are a composite
            //       of the Scan's rows and any constants or computed expressions it introduces.
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
