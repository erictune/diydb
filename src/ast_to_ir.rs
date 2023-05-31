//! `ast_to_ir` converts an AST into an intermediate representation (IR).
//! At present, IR is only used for Select statements, not Create statements.
//! Insert, delete and updates,  (data manipulation language) would all use IR.
//! Drop and alter (data definition language) do not need IR optimizations.

use crate::ast;
use crate::ir;
use anyhow::{bail, Result};
use std::boxed::Box;

pub fn ast_select_statement_to_ir(ss: &ast::SelectStatement) -> Result<ir::Block, anyhow::Error> {
    // If the select only has a select clause,then we just need to return a constant
    // single row one time (or maybe multiple rows if we support UNION in the future and simplify it).
    if ss.from.is_none() {
        let mut row: Vec<ast::Constant> = vec![];
        for item in &ss.select.items {
            match item {
                ast::SelItem::Expr(e) => {
                    match e {
                        ast::Expr::Constant(c) => {
                            row.push(c.clone())
                        }
                        ast::Expr::BinOp{..} => {
                            // We have done a constant propagation pass over the AST.
                            // So, if there is a BinOp expression, it must contain a ColName. 
                            // You can't use a ColName when there is no FROM clause.
                            bail!("Unexpected BinOp in a query without a FROM clause");
                        }
                    }
                }
                ast::SelItem::ColName(c) => bail!("Cannot select {c} without a FROM clause"),
                ast::SelItem::Star => bail!("Cannot select * without a FROM clause"),
            }
        }
        return Ok(ir::Block::ConstantRow(ir::ConstantRow { row }));
    }
    // At this point, the select has a "from" clause.  In a degenerate case, it might not
    // be referenced by the select or where or other clauses, but we still have to "scan" to return
    // one result row for every input row.
    let scan = ir::Scan {
        tablename: ss.from.as_ref().unwrap().tablename.clone(),
    };
    let mut outcols: Vec<ast::SelItem> = vec![];
    for item in &ss.select.items[..] {
        match item {
            ast::SelItem::Expr(_) => outcols.push(item.clone()),
            ast::SelItem::ColName(_) => outcols.push(item.clone()),
            ast::SelItem::Star => outcols.push(item.clone()),
        }
    }
    if outcols.len() == 1 && outcols[0].is_star()
    {
        // No project block needed if all columns selected.
        return Ok(ir::Block::Scan(scan));
        // Ponder: This could be moved to an opimization pass?
        // Call it Project Elimination (?): remove unneeded Project() from Project(Scan), if
        // the Project is not adding or eliminating any rows (minor efficiency boost maybe?)
    }
    Ok(ir::Block::Project(ir::Project {
        // TODO: Consider whether to lookup the table's column names and types at this point.
        // Table information like sizes would be needed prior to execution to do cost-based optimization.
        // This lookup can be done as a pass after building the initial IR but before interpreting it.
        // Presumably there are many optimizations and checks that can be done once we know the types
        // of columns.  
        //
        // When we do look up the schema, we will need to verify it again at execution time (abort
        // if any Scans have different column names or types than previously fetched, while locking the schema
        // row for that table.)
        //
        // In the future when we handle nested selects, we will need to find the inner select and then work
        // outwards so that we can propagate up output names to input names.  That is currently handled during interpretation.
        // Would need to be handled earlier for code generation, and maybe for other optimizations.
        outcols,
        input: Box::new(ir::Block::Scan(scan)),
    }))
}

#[test]
fn test_ast_select_statement_to_ir() {
    struct Case {
        desc: String,
        input: ast::SelectStatement,
        expected: Result<ir::Block, ()>,
    }
    let cases: Vec<Case> = vec![
        Case {
            desc: "Select 1;".to_string(),
            input: ast::SelectStatement {
                select: ast::SelectClause {
                    items: vec![ast::SelItem::Expr(ast::Expr::Constant(ast::Constant::Int(1)))],
                },
                from: None,
            },
            expected: Ok(ir::Block::ConstantRow(ir::ConstantRow {
                row: vec![ast::Constant::Int(1)],
            })),
        },
        Case {
            desc: "Select a from t;".to_string(),
            input: ast::SelectStatement {
                select: ast::SelectClause {
                    items: vec![ast::SelItem::ColName(ast::ColName {
                        name: String::from("a"),
                    })],
                },
                from: Some(ast::FromClause {
                    tablename: String::from("t"),
                }),
            },
            expected: Ok(ir::Block::Project(ir::Project {
                outcols: vec![ast::SelItem::ColName(ast::ColName {
                    name: String::from("a"),
                })],
                input: std::boxed::Box::new(ir::Block::Scan(ir::Scan {
                    tablename: String::from("t"),
                })),
            })),
        },
        Case {
            desc: "Select * from t;".to_string(),
            input: ast::SelectStatement {
                select: ast::SelectClause {
                    items: vec![ast::SelItem::Star],
                },
                from: Some(ast::FromClause {
                    tablename: String::from("t"),
                }),
            },
            expected: Ok(ir::Block::Scan(ir::Scan {
                tablename: String::from("t"),
            })),
        },
        Case {
            desc: "Select 1 from t;".to_string(),
            input: ast::SelectStatement {
                select: ast::SelectClause {
                    items: vec![ast::SelItem::Expr(ast::Expr::Constant(ast::Constant::Int(1)))],
                },
                from: Some(ast::FromClause {
                    tablename: String::from("t"),
                }),
            },
            expected: Ok(ir::Block::Project(ir::Project {
                outcols: vec![ast::SelItem::Expr(ast::Expr::Constant(ast::Constant::Int(1)))],
                input: std::boxed::Box::new(ir::Block::Scan(ir::Scan {
                    tablename: String::from("t"),
                })),
            })),
        },
        Case {
            desc: "Select 1, a, 3 from t;".to_string(),
            input: ast::SelectStatement {
                select: ast::SelectClause {
                    items: vec![
                        ast::SelItem::Expr(ast::Expr::Constant(ast::Constant::Int(1))),
                        ast::SelItem::ColName(ast::ColName {
                            name: String::from("a"),
                        }),
                        ast::SelItem::Expr(ast::Expr::Constant(ast::Constant::Int(3))),
                    ],
                },
                from: Some(ast::FromClause {
                    tablename: String::from("t"),
                }),
            },
            expected: Ok(ir::Block::Project(ir::Project {
                outcols: vec![
                    ast::SelItem::Expr(ast::Expr::Constant(ast::Constant::Int(1))),
                    ast::SelItem::ColName(ast::ColName {
                        name: String::from("a"),
                    }),
                    ast::SelItem::Expr(ast::Expr::Constant(ast::Constant::Int(3))),
                ],
                input: std::boxed::Box::new(ir::Block::Scan(ir::Scan {
                    tablename: String::from("t"),
                })),
            })),
        },
        Case {
            desc: "Select a;".to_string(), // Error detected at IR phase.
            input: ast::SelectStatement {
                select: ast::SelectClause {
                    items: vec![ast::SelItem::ColName(ast::ColName {
                        name: String::from("a"),
                    })],
                },
                from: None,
            },
            expected: Err(()),
        },
        Case {
            desc: "Select *;".to_string(), // Error detected at IR phase.
            input: ast::SelectStatement {
                select: ast::SelectClause {
                    items: vec![ast::SelItem::Star],
                },
                from: None,
            },
            expected: Err(()),
        },
    ];
    for case in cases {
        println!("Running case: {}", case.desc);
        let actual = ast_select_statement_to_ir(&case.input);
        let actual_ok = actual.is_ok();
        let expected_ok = case.expected.is_ok();
        if actual.is_ok() {
            assert!(case.expected.is_ok());
            assert_eq!(actual.unwrap(), case.expected.unwrap());
        } else {
            println!("Actual's error: {}", actual.unwrap_err())
        }
        assert_eq!(actual_ok, expected_ok);
    }
}
