//! simplifies ast trees.
//! - evaluates constant expressions in select items.

use anyhow::{bail, Result};

use crate::ast;

fn do_int_binop(i: i64, op: &ast::Op, j: i64) -> i64 {
    use ast::Op::*;
    match op {
        Add => i + j,
        Subtract => i - j,
        Multiply => i * j,
        Divide => i / j,
    }
}

fn do_real_binop(i: f64, op: &ast::Op, j: f64) -> f64 {
    use ast::Op::*;
    match op {
        Add => i + j,
        Subtract => i - j,
        Multiply => i * j,
        Divide => i / j,
    }
}

fn do_binop(i: ast::Constant, op: &ast::Op, j: ast::Constant) -> Result<ast::Constant> {
    use ast::Constant::*;
    let icopy = i.clone();
    let jcopy = j.clone();
    match (i, j) {
        (Int(i), Int(j)) => Ok(ast::Constant::Int(do_int_binop(i.clone(), op, j))),
        (Real(i), Real(j)) => Ok(ast::Constant::Real(do_real_binop(i, op, j))),
        (Int(i), Real(j)) => Ok(ast::Constant::Real(do_real_binop(i as f64, op, j))),
        (Real(i), Int(j)) => Ok(ast::Constant::Real(do_real_binop(i, op, j as f64))),
        (Null(), _) => Ok(ast::Constant::Null()),
        (_, Null()) => Ok(ast::Constant::Null()),
        _ => bail!("Invalid types in binary expression: {} {} {}", icopy, op, jcopy),
    }

}

#[test]
fn test_do_binop_ok() {
    use ast::Constant::*;
    use ast::Op::*;
    let cases = vec![
        (Int(1), Add, Int(1), Int(2)),
    ];
    for case in cases {
        let res = do_binop(case.0, &case.1, case.2);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), case.3);
    }
}
#[test]
fn test_do_binop_err() {
    use ast::Constant::*;
    use ast::Op::*;
    let cases = vec![
        (String("foo".to_string()), Subtract, Real(1.1)),
    ];
    for case in cases {
        assert!(do_binop(case.0, &case.1, case.2).is_err());
    }
}

// TODO: handle expressions which contain column references too.
// TODO: just call this simplify_expr.  There isn't a clear case where we need to get the Constant.
fn try_simplify_expr_to_constant(expr: &ast::Expr) -> Result<ast::Constant>{
    match expr {
        ast::Expr::Constant(c) => return Ok(c.clone()),
        ast::Expr::BinOp { lhs, op, rhs } => {
            if let Ok(l) = try_simplify_expr_to_constant(lhs) {
                if let Ok(r) = try_simplify_expr_to_constant(rhs) {
                    return do_binop(l, op, r)
                }
            }
        }
        // ast::Expr::ColumnName => Ok(None) // meaning no errors, but not able to simplify to a constant.
    }
    unreachable!();
}

pub fn simplify_ast_select_statement(ss: &mut ast::SelectStatement) -> Result<()> {
    let len = ss.select.items.len();
    let mut newitems = vec![];
    for i in 0..len {
        newitems.push(
            match &mut ss.select.items[i] {
                ast::SelItem::Star => ast::SelItem::Star,
                x @ ast::SelItem::ColName(_) => x.clone(),
                ast::SelItem::Expr(e) => {
                    let c = try_simplify_expr_to_constant(e)?;
                    ast::SelItem::Expr(ast::Expr::Constant(c.clone()))
                }
            }
        );
    }
    ss.select.items = newitems;
    Ok(())
}

#[test]
fn test_simplify_ast_select_statement() {
    struct Case {
        desc: String,
        input: ast::SelectStatement,
        expected:  ast::SelectStatement,
    }
    let cases: Vec<Case> = vec![
        Case {
            desc: "Select 1+1;".to_string(),
            input: ast::SelectStatement {
                select: ast::SelectClause {
                    items: vec![
                        ast::SelItem::Expr(
                            ast::Expr::BinOp{
                                lhs: Box::new(ast::Expr::Constant(ast::Constant::Int(1))),
                                op: crate::ast::Op::Add,
                                rhs: Box::new(ast::Expr::Constant(ast::Constant::Int(1))),
                            }
                        )
                    ],
                },
                from: None,
            },
            expected: ast::SelectStatement {
                select: ast::SelectClause {
                    items: vec![ast::SelItem::Expr(ast::Expr::Constant(ast::Constant::Int(2)))],
                },
                from: None,
            },
        },
        Case {
            desc: "Select 2 from t;".to_string(),
            input: ast::SelectStatement {
                select: ast::SelectClause {
                    items: vec![ast::SelItem::Expr(ast::Expr::Constant(ast::Constant::Int(2)))],
                },
                from: Some(ast::FromClause {
                    tablename: String::from("t"),
                }),
            },
            expected: ast::SelectStatement {
                select: ast::SelectClause {
                    items: vec![ast::SelItem::Expr(ast::Expr::Constant(ast::Constant::Int(2)))],
                },
                from: Some(ast::FromClause {
                    tablename: String::from("t"),
                }),
            },
        },
    ];
    for case in cases {
        println!("Running case: {}", case.desc);
        let mut actual = case.input.clone();
        let res = simplify_ast_select_statement(&mut actual);
        if res.is_ok() {
            assert_eq!(actual, case.expected);
        } else {
            println!("Actual's error: {}", res.unwrap_err());
            assert!(false, "Actual was not ok");
        }
    }
}