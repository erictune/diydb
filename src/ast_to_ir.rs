//! `ast_to_ir` converts an AST into an intermediate representation (IR).
//! At present, IR is only used for Select statements, not Create statements.
//! Insert, delete and updates,  (data manipulation language) would all use IR.
//! Drop and alter (data definition language) do not need IR optimizations.

use crate::ast;
use crate::ir;
use std::boxed::Box;

// TODO: Consider that there could be a trait ToIR with method to_ir().
// However, we have a bounded set of things that need to be
// converted, so enums are working fine so far.

// TODO: use this in the main query processing path.
fn ast_select_statement_to_ir(ss: &ast::SelectStatement) -> ir::Block {
    // If the select only has a select clause, then we just need to return a constant
    // expression.
    if ss.from.is_none() {
        let mut row: Vec<ast::Constant> = vec![];
        for item in &ss.select.items {
            match item {
                ast::SelItem::Const(c) => row.push(c.clone()),
                ast::SelItem::ColName(c) => panic!("Cannot select {} without a FROM clause", c),
                ast::SelItem::Star => panic!("Cannot select * without a FROM clause"),
            }
        }
        return ir::Block::ConstantRow(ir::ConstantRow {
            row: row, // TODO: get this from the select expression, and make sure it does not include column refs.
        });
    }
    // If the select has a "from" clause, then we need a Scan wrapped in a Project.
    let scan = ir::Scan {
        tablename: ss.from.as_ref().unwrap().tablename.clone(),
    };
    let mut outcols: Vec<ast::SelItem> = vec![];
    for item in &ss.select.items[..] {
        match item {
            ast::SelItem::Const(_) => outcols.push(item.clone()), // TODO: temporary name for constant valued columns?
            ast::SelItem::ColName(c) => outcols.push(item.clone()), // TODO: Is this a good time to check if row in table's schema?  Or at execution time?
            ast::SelItem::Star => outcols.push(item.clone()),       // TODO: expand star here?
        }
    }
    ir::Block::Project(ir::Project {
        outcols: outcols, // For star, we need to lookup the table to expand star, or do that on the fly?
        input: Box::new(ir::Block::Scan(scan)),
    })
}

#[test]
fn test_ast_select_statement_to_ir() {
    // These should not panic.
    // TODO: have then return a Result.
    let cases = vec![
        // "Select 1;"
        ast::SelectStatement {
            select: ast::SelectClause {
                items: vec![ast::SelItem::Const(ast::Constant::Int(1))],
            },
            from: None,
        },
        // "Select a from t;"
        ast::SelectStatement {
            select: ast::SelectClause {
                items: vec![ast::SelItem::ColName(ast::ColName {
                    name: String::from("a"),
                })],
            },
            from: Some(ast::FromClause {
                tablename: String::from("t"),
            }),
        },
    ];
    for case in cases {
        let _ = ast_select_statement_to_ir(&case);
    }
}
