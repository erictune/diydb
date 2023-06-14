//! `parser` contains generated parsing routines for SQL and tests on them.

use pest::iterators::Pairs;
use pest::pratt_parser::PrattParser;

use crate::ast;

#[allow(unused_imports)]
use pest::Parser; // This needs to be in scope for the next statements to work.
#[derive(Parser)]
#[grammar = "sql.pest"]
pub struct SQLParser;

// From: https://pest.rs/book/examples/calculator.html, MIT,Apache2.0 licenses.
lazy_static::lazy_static! {
    pub static ref PRATT_PARSER: PrattParser<Rule> = {
        use pest::pratt_parser::{Assoc::*, Op};
        use Rule::*;

        // Precedence is defined lowest to highest
        PrattParser::new()
            // Addition and subtract have equal precedence
            .op(Op::infix(add, Left) | Op::infix(subtract, Left))
            .op(Op::infix(multiply, Left) | Op::infix(divide, Left))
    };
}

// From: https://pest.rs/book/examples/calculator.html, MIT,Apache2.0 licenses.
pub fn parse_expr(pairs: Pairs<Rule>) -> ast::Expr {
    PRATT_PARSER
        .map_primary(|primary| match primary.as_rule() {
            Rule::null_literal
            | Rule::true_literal
            | Rule::false_literal
            | Rule::integer_literal
            | Rule::decimal_literal
            | Rule::single_quoted_string => ast::Expr::Constant(crate::pt_to_ast::parse_literal_from_rule(primary)),
            rule => unreachable!("parse_expr expected literal, found {:?}", rule),
        })
        .map_infix(|lhs, op, rhs| {
            let op = match op.as_rule() {
                Rule::add => ast::Op::Add,
                Rule::subtract => ast::Op::Subtract,
                Rule::multiply => ast::Op::Multiply,
                Rule::divide => ast::Op::Divide,
                rule => unreachable!("Expr::parse expected infix operation, found {:?}", rule),
            };
            ast::Expr::BinOp {
                lhs: Box::new(lhs),
                op,
                rhs: Box::new(rhs),
            }
        })
        .parse(pairs)

}

#[test]
fn test_parse_literals() {
    let cases = vec![
        ("1"),
        ("1000000000000"),
        ("-1000000000000"),
        ("1.01"),
        ("123456789.987654321"),
        ("'hi'"),
        ("true"),
        ("tRuE"),
        ("TRUE"),
        ("false"),
        ("fAlSe"),
        ("FALSE"),
        ("null"),
        ("nUlL"),
        ("NULL"),
    ];
    for case in cases {
        assert!(SQLParser::parse(Rule::literal, case).is_ok());
    }
}

#[test]
fn test_not_parse_invalid_literals() {
    let cases = vec![
        ("A"),
        ("\"hi\""),
        ("the quick brown fox"),
        ("NIL"),
        ("DELETE"),
    ];
    for case in cases {
        assert!(SQLParser::parse(Rule::literal, case).is_err());
    }
}

#[test]
fn test_parse_expr() {
    let cases = vec![
        ("1 + 2"),
        ("3 * 4"),
        ("5 * 6 + 7"),
        ("8 + 9 * 10"), 
    ];

    for case in cases {
        assert!(SQLParser::parse(Rule::expr, case).is_ok());
    }
}

#[test]
fn test_parse_create_statements() {
    let cases = vec![
        "CREATE TABLE FOO (A INT, B INT)",
        "create table foo (a int, b int)",
        "create table foo (a int)",
        "create table foo (a int,  b int, c int, dee real)",
        "CREATE TABLE t (a int, b integer, c text, d string, e real)",
        "creaTe TaBle superlongname (superduperlongname integer)",
        "CREATE TEMPORARY TABLE FOO (A INT, B INT)",
        "CREATE TEMP TABLE FOO (A INT, B INT)",
    ];
    for case in cases {
        println!("Case: {}", case);
        assert!(SQLParser::parse(Rule::create_stmt, case).is_ok());
    }
}

#[test]
fn test_not_parse_invalid_create_statements() {
    let cases = vec![
        "CREATE TABLE FOO (nonsense that does not have commas)",
        "create table foo a int, b int",
        "create table foo ()",
        "create table foo (,,,,,)",
        "SELECT * from T",
        "CREATE T TABLE FOO (A INT, B INT)",
    ];
    for case in cases {
        assert!(SQLParser::parse(Rule::create_stmt, case).is_err());
    }
}

#[test]
fn test_parse_expr_list() {
    let cases = vec![
        "(1, 'two', 3.3)",
    ];
    for case in cases {
        println!("Case: {}", case);
        match SQLParser::parse(Rule::expr_list, case) {
            Ok(_) => continue,
            Err(e) => panic!("Error parsing [{}] : {}",  case, e),
        }    
    }
}

#[test]
fn test_parse_expr_list_list() {
    let cases = vec![
        "(1, 'two', 3.3)",
        "(1, 'two', 3.3), (4, 'five', 6.6)",
    ];
    for case in cases {
        println!("Case: {}", case);
        match SQLParser::parse(Rule::expr_list_list, case) {
            Ok(_) => continue,
            Err(e) => panic!("Error parsing [{}] : {}",  case, e),
        }    
    }
}

#[test]
fn test_parse_insert_statements() {
    let cases = vec![
        "INSERT INTO FOO VALUES (1, 'two', 3.3)",
        "insert into foo values (1, 'two', 3.3)",
        "insert into foo values (1, 'two', 3.3), (4, 'five', 6.6)",
    ];
    for case in cases {
        println!("Case: {}", case);
        match SQLParser::parse(Rule::insert_stmt, case) {
            Ok(_) => continue,
            Err(e) => panic!("Error parsing [{}] : {}",  case, e),
        }    
    }
}

#[test]
fn test_not_parse_invalid_insert_statements() {
    let cases = vec![
        "INSERT INTO FOO VALUES",
    ];
    for case in cases {
        assert!(SQLParser::parse(Rule::insert_stmt, case).is_err());
    }
}

#[test] 
fn test_parse_select_with_expr() {
    let e = SQLParser::parse(Rule::select_stmt, "select 1 + 1");
    if e.is_err() {
        println!("{:?}", e.err())
    } 

}
#[test]
fn test_parse_select_statement() {
    let cases = vec![
        ("SELECT * FROM tbl"),
        ("select a,b,c fRoM tbl"),
        ("select x, 1 from tbl"),
        ("select x, 1"), // This is invalid SQL, but this check happens after parsing.
        ("select 1.01"),
        ("select 'hi'"),
        ("select 1 + 1"),
    ];

    for case in cases {
        assert!(SQLParser::parse(Rule::select_stmt, case).is_ok());
    }
}

#[test]
fn test_not_parse_invalid_select_statement() {
    let cases = vec![
        ("CREATE * FROM tbl"),
        ("FROM blahblah"),
        ("select \"hi\""), // Double quotes are invalid as literals in std SQL.
    ];

    for case in cases {
        assert!(SQLParser::parse(Rule::select_stmt, case).is_err());
    }
}
