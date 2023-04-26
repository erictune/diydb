use crate::ir;
use crate::pager;

// TODO: instead printing directly from here:
// - return output headers and a table iterator, and then
// have the caller call formatting::print_table.
// TODO: return Result<> to allow for errors to propagate up to main without panicing.
pub fn run_ir(pager: &pager::Pager, ir: &ir::Block)  {
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
            let (root_pagenum, create_statement) = crate::get_creation_sql_and_root_pagenum(pager, table_name)
                .unwrap_or_else(|| panic!("Should have looked up the schema for {}.", table_name));
            let (_, column_names, column_types) = crate::pt_to_ast::parse_create_statement(&create_statement);
            // TODO: these results should come over a "connection" and then be formatted and emitted to a file or stdout outside of the execution
            // of the code.  That means splitting print_table into the execution part (goes in IR interpreter) and the formatter.
            // Queries should end as soon as possible to allow writers to have a chance.
            // For interactive queries, this means eagerly reading all the results into a limited sized buffer,
            // and failing if they don't fit.
            // For a single-process interaction, the caller should be able to provide a buffer?
            // For an administrative command (e.g. dump table to backup file), then blocking writes is okay, I guess?
            // Therefore, we can for now copy to a buffer at the last step of evaluating the IR.
            crate::print_table(pager, root_pagenum, table_name, column_names, column_types, false)
        },
    }
}