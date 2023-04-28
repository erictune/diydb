use crate::ir;
use crate::pager;
use anyhow::Result;

// TODO: instead printing directly from here:
// - return output headers and a table iterator, and then
// have the caller call formatting::print_table.
// TODO: return Result<> to allow for errors to propagate up to main without panicing.
pub fn run_ir(pager: &pager::Pager, ir: &ir::Block) -> Result<crate::QueryOutputTable> {
    match ir {
        // TODO support root Project blocks.  This requires printing rows that
        // have constant exprs, dropping rows, etc.
        // The right way to do is to have formatting::print_table accept different kinds of iterators.
        ir::Block::Project(_) => panic!("IR that uses Project not supported yet."),
        ir::Block::ConstantRow(_) => panic!("IR that uses ConstantRow not supported yet."),
        ir::Block::Scan(s) => {
            // Question: what happens if the IR was built based on assumptions about the schema (e.g. number and types of columns),
            // and then the schema changed?  How about storing the message digest of the creation_sql in the Scan block and verify
            // it here.
            let table_name = s.tablename.as_str();
            let (root_pagenum, create_statement) =
                crate::get_creation_sql_and_root_pagenum(pager, table_name).unwrap_or_else(|| {
                    panic!("Should have looked up the schema for {}.", table_name)
                });
            let (_, column_names, column_types) =
                crate::pt_to_ast::parse_create_statement(&create_statement);
            // Here we make a copy of the results so that the callers does not have to deal with a limited lifetime.
            // During the query, which may process many rows, we tried to avoid copies at all costs.
            // But once it is done. we want the caller to be able to peruse the results at their leisure, while releasing
            // the page locks as soon as possible.  Also, the caller should not have to think about the lifetimes of the internal
            // iterator.
            // The assumption here is that the caller is an interactive user who wants a limited number of rows (thousands).
            // For non-interactive bulk use, perhaps this needs to be revisted.
            //crate::print_table(pager, root_pagenum, table_name, column_names, column_types, false)?;
            let tci = crate::btree::table::Iterator::new(root_pagenum, pager);
            Ok(crate::QueryOutputTable {
                // TODO: take() a limited number of rows when collect()ing them, and return error if they don't fit?
                rows: tci.map(|t| (t.0, Vec::from(t.1))).collect(),
                column_names: column_names.clone(),
                column_types: column_types.clone(),
            })
        }
    }
}
