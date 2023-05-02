//! executes SQL intermediate representation (IR).

use crate::TempTable;
use crate::ir;
use crate::pager;
use anyhow::Result;

use crate::sql_value::SqlValue;
use crate::sql_type::SqlType;

use crate::ast;
use crate::typed_row::TypedRow;

fn ast_constant_to_sql_value(c: &ast::Constant) -> SqlValue {
    match c {
        ast::Constant::Int(i) => SqlValue::Int(*i),
        ast::Constant::String(s) => SqlValue::Text(s.clone()),
        ast::Constant::Real(f) => SqlValue::Real(*f),
        ast::Constant::Bool(b) => SqlValue::Int(match b { true => 1, false => 0 }),
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

        ir::Block::Project(_) => panic!("IR that uses Project not supported yet."),
        ir::Block::ConstantRow(cr) => {
            Ok(TempTable {
                rows: vec![
                    TypedRow {
                        row_id: 1,
                        items: cr.row.iter().map(|e| ast_constant_to_sql_value(e)).collect(),
                    }
                ],
                column_names: (0..cr.row.len()).map(|i| format!("_f{i}")).collect(),
                column_types: cr.row.iter().map(|e| ast_constant_to_sql_type(e)).collect(),
            })
        }
        ir::Block::Scan(s) => {
            // Question: what happens if the IR was built based on assumptions about the schema (e.g. number and types of columns),
            // and then the schema changed?  How about storing the message digest of the creation_sql in the Scan block and verify
            // it here.
            let table_name = s.tablename.as_str();
            let pager = ps.default_pager()?;
            let (root_pagenum, create_statement) =
                crate::get_creation_sql_and_root_pagenum(pager, table_name).unwrap_or_else(|| {
                    panic!("Should have looked up the schema for {}.", table_name)
                });
            let (_, column_names, column_types) =
                crate::pt_to_ast::parse_create_statement(&create_statement);
            let mut tci = crate::btree::table::Iterator::new(root_pagenum, pager);
            // TODO: make this a convenience function int typed_row.rs.
            crate::clone_and_cast_table_iterator(&mut tci, &column_names, &column_types)
        }
    }
}
