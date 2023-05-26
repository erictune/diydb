//! `ast_to_ir` converts an AST into an intermediate representation (IR).
//! At present, IR is only used for Select statements, not Create statements.
//! Insert, delete and updates,  (data manipulation language) would all use IR.
//! Drop and alter (data definition language) do not need IR optimizations.

use crate::ast;
use crate::ir;
use anyhow::bail;
use std::boxed::Box;

pub fn ast_select_statement_to_ir(ss: &ast::SelectStatement) -> Result<ir::Block, anyhow::Error> {
    // If the select only has a select clause,then we just need to return a constant
    // expression.
    // Rationale: Doing this now means we don't need to have `Project(Some(Scan))`.
    if ss.from.is_none() {
        let mut row: Vec<ast::Constant> = vec![];
        for item in &ss.select.items {
            match item {
                ast::SelItem::Const(c) => row.push(c.clone()),
                ast::SelItem::ColName(c) => bail!("Cannot select {c} without a FROM clause"),
                ast::SelItem::Star => bail!("Cannot select * without a FROM clause"),
            }
        }
        return Ok(ir::Block::ConstantRow(ir::ConstantRow { row }));
    }
    // At this point, the select has a "from" clause, (though in a degenerate case, it might not
    // be referenced by the select or where or other clauses.

    // TODO: add an IR optimization pass between ast_to_ir and ir_interpreter, starting with this simple optimization:
    // Working from bottom to top, do "Constant Table Propagation":
    // if a Project's returned row is all constants, then replace Project(Scan)
    // with a ConstantRow.
    // e.g.:
    // if Project.outputrows.all(|col| match col {ast::SelItem::Const(_) => true, _ => false,}))
    // { /* collapse Project(Scan) to ConstantRow */}
    // This gets more complex when you add in intervening Filter expressions like `Project(Filter("a=1", ConstantRow(["a"], ["1"])))`
    // Which will come from `select 1 as a where a = 0;`, and perhaps with other select clauses.

    let scan = ir::Scan {
        tablename: ss.from.as_ref().unwrap().tablename.clone(),
    };
    let mut outcols: Vec<ast::SelItem> = vec![];
    for item in &ss.select.items[..] {
        match item {
            ast::SelItem::Const(_) => outcols.push(item.clone()),
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
        // TODO: For star, lookup the table to expand outcols to all column names of the input table.
        // This could be done as a pass after building the initial IR but before interpreting it.
        // Presumably there are some optimizations or checks that can be done once we know the types
        // of columns.  Is there a benefit to doing that before we start execution?
        // (e.g. don't discover issues midway through long query)
        //
        // When we do look up the schema, we will need to verify it again at execution time (abort
        // if any Scans have different column names or types than previously fetched, while locking the schema
        // row for that table.)
        //
        // For the simple queries we deal with today, we have the option to do that here with out local view.
        // In the future when we handle nested selects, we will need to find the inner select and then work
        // outwards so that we can propagate up output names to input names. Not sure if that is better handled
        // in the ast_to_ir pass or later.  Clearly we want to start at the leaves of the IR an work up building
        // the IR.  But is that the time to handle types and column names?  Or in a later pass on the IR?
        //
        // TODO: It might be good to add types info alongside outcols, too, so that we can pre-validate expression types(?).
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
                    items: vec![ast::SelItem::Const(ast::Constant::Int(1))],
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
                    items: vec![ast::SelItem::Const(ast::Constant::Int(1))],
                },
                from: Some(ast::FromClause {
                    tablename: String::from("t"),
                }),
            },
            expected: Ok(ir::Block::Project(ir::Project {
                outcols: vec![ast::SelItem::Const(ast::Constant::Int(1))],
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
                        ast::SelItem::Const(ast::Constant::Int(1)),
                        ast::SelItem::ColName(ast::ColName {
                            name: String::from("a"),
                        }),
                        ast::SelItem::Const(ast::Constant::Int(3)),
                    ],
                },
                from: Some(ast::FromClause {
                    tablename: String::from("t"),
                }),
            },
            expected: Ok(ir::Block::Project(ir::Project {
                outcols: vec![
                    ast::SelItem::Const(ast::Constant::Int(1)),
                    ast::SelItem::ColName(ast::ColName {
                        name: String::from("a"),
                    }),
                    ast::SelItem::Const(ast::Constant::Int(3)),
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
        assert_eq!(actual.is_ok(), case.expected.is_ok());
        if actual.is_ok() {
            assert!(case.expected.is_ok());
            assert_eq!(actual.unwrap(), case.expected.unwrap());
        }
    }
}
