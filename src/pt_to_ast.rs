//! `pt_to_ast` has routines for converting parse trees to ASTs for SQL.
//! A Pest parse tree has one enum for all possible terminals and non-terminals.
//! Our AST has enums for groups of terminals that are used in the same production.
//! The AST also discards some lexical detail like case and position in the input.

use crate::ast;
use crate::parser::Rule;
use crate::parser::SQLParser;
use crate::pest::Parser;

pub fn pt_create_statement_to_ast(c: &str) -> ast::CreateStatement {
    use itertools::Itertools;
    let create_stmt = SQLParser::parse(Rule::create_stmt, c)
        .expect("unsuccessful parse") // unwrap the parse result
        .next()
        .unwrap();

    let mut coldefs: Vec<ast::ColDef> = vec![];

    let mut tablename = String::from("");
    // Confirm it is a create statement.
    for c in create_stmt.into_inner() {
        match c.as_rule() {
            Rule::table_identifier => {
                tablename = String::from(c.as_str());
            }
            Rule::column_defs => {
                for column_def in c.into_inner() {
                    match column_def.as_rule() {
                        Rule::column_def => {
                            let (col_name, col_type) = column_def
                                .into_inner()
                                .take(2)
                                .map(|e| String::from(e.as_str()))
                                .collect_tuple()
                                .unwrap();
                            coldefs.push(ast::ColDef {
                                colname: ast::ColName { name: col_name },
                                coltype: col_type,
                            });
                        }
                        _ => unreachable!(),
                    }
                }
            }
            Rule::EOI => (),
            _ => unreachable!(),
        }
    }
    ast::CreateStatement { tablename, coldefs }
}

#[test]
fn test_pt_create_statement_to_ast() {
    let input = "CREATE TABLE t (a int)";
    let actual = pt_create_statement_to_ast(input);
    let expected = ast::CreateStatement {
        tablename: "t".to_string(),
        coldefs: vec![ast::ColDef {
            colname: ast::ColName {
                name: "a".to_string(),
            },
            coltype: "int".to_string(),
        }],
    };
    assert_eq!(actual, expected);
}
// Select(SelectItems(Constant(1), ColName(x)), From(TableName("t")))
pub fn ast_create_statement_to_tuple(
    c: ast::CreateStatement,
) -> (String, Vec<String>, Vec<String>) {
    (
        c.tablename,
        c.coldefs.iter().map(|x| x.colname.name.clone()).collect(),
        c.coldefs.iter().map(|x| x.coltype.clone()).collect(),
    )
}

pub fn parse_create_statement(c: &str) -> (String, Vec<String>, Vec<String>) {
    let ast: ast::CreateStatement = pt_create_statement_to_ast(c);
    // TODO: would there ever be any optimizations or type checks to do on a create statement?
    ast_create_statement_to_tuple(ast)
}

#[test]
fn test_parse_create_statement() {
    let cases = vec![
        (
            "CREATE TABLE t (a int, b integer, c text, d string, e real)",
            (
                "t",
                vec!["a", "b", "c", "d", "e"],
                vec!["int", "integer", "text", "string", "real"],
            ),
        ),
        (
            "CREATE TABLE Tbl_Two(a int,b int)",
            ("Tbl_Two", vec!["a", "b"], vec!["int", "int"]),
        ),
    ];
    for case in cases {
        let input = case.0;
        println!("Input: {}", input);
        let ast: ast::CreateStatement = pt_create_statement_to_ast(input);
        let actual = ast_create_statement_to_tuple(ast);
        let expected = (
            String::from(case.1 .0),
            case.1 .1.iter().map(|x| String::from(*x)).collect(),
            case.1 .2.iter().map(|x| String::from(*x)).collect(),
        );
        assert_eq!(actual, expected);
    }
}

fn parse_literal_from_rule<'i>(pair: pest::iterators::Pair<'i, Rule>) -> ast::Constant {
    match pair.as_rule() {
        Rule::null_literal => ast::Constant::Null(),
        Rule::true_literal => ast::Constant::Bool(true),
        Rule::false_literal => ast::Constant::Bool(false),
        Rule::integer_literal => ast::Constant::Int(str::parse::<i64>(pair.as_str()).unwrap()),
        Rule::decimal_literal => {
            // Danger: floating point conversion.
            ast::Constant::Real(str::parse::<f64>(pair.as_str()).unwrap())
        }
        Rule::single_quoted_string => ast::Constant::String(String::from(pair.as_str())),
        Rule::double_quoted_string => {
            panic!("Double quoted strings are not valid string literals in SQL.")
        }
        _ => {
            panic!(
                "parse_literal_from_rule does not handle {:?}",
                pair.as_rule()
            )
        }
    }
}

#[test]
fn test_parsing_literals() {
    let cases = vec![
        ("1", "1"),
        ("1.01", "1.01"),
        ("'hi'", "'hi'"),
        ("true", "TRUE"),
        ("tRuE", "TRUE"),
        ("TRUE", "TRUE"),
        ("false", "FALSE"),
        ("fAlSe", "FALSE"),
        ("FALSE", "FALSE"),
        ("null", "NULL"),
        ("nUlL", "NULL"),
        ("NULL", "NULL"),
    ];
    for case in cases {
        let input = case.0;
        let literal = SQLParser::parse(Rule::literal, input)
            .expect("unsuccessful parse") // unwrap the parse result
            .next()
            .unwrap();
        let ast = parse_literal_from_rule(literal);
        let actual = format!("{}", ast);
        let expected = case.1;
        assert_eq!(actual, expected);
    }
}

// TODO: expand star into list of all column names of all tables in the input table list.
pub fn pt_select_statement_to_ast(query: &str) -> ast::SelectStatement {
    let select_stmt = SQLParser::parse(Rule::select_stmt, query)
        .expect("unsuccessful parse") // unwrap the parse result
        .next()
        .unwrap();

    let mut ast = ast::SelectStatement {
        select: ast::SelectClause { items: vec![] },
        from: None,
    };

    // Confirm it is a select statement.
    for s in select_stmt.into_inner() {
        match s.as_rule() {
            Rule::table_identifier => {
                if ast.from.is_none() {
                    ast.from = Some(ast::FromClause {
                        tablename: String::from(s.as_str()),
                    });
                } else {
                    panic!("Too many tables in from.")
                }
            }
            Rule::select_items => {
                // println!("s: {}", s);
                // println!("s.as_span(): {:?}", s.as_span());
                // println!("s.as_rule(): {:?}", s.as_rule());
                // println!("s.as_str(): {}", s.as_str());

                // For each select item.
                for t in s.into_inner() {
                    use ast::{ColName, SelItem};
                    let u = t.into_inner().next().unwrap();
                    ast.select.items.push(match u.as_rule() {
                        Rule::column_name => SelItem::ColName(ColName {
                            name: String::from(u.as_str()),
                        }),
                        Rule::star => SelItem::Star,
                        Rule::null_literal
                        | Rule::true_literal
                        | Rule::false_literal
                        | Rule::integer_literal
                        | Rule::decimal_literal
                        | Rule::single_quoted_string => SelItem::Const(parse_literal_from_rule(u)),
                        _ => panic!("Parse error in select item"),
                    });
                }
            }
            Rule::EOI => (),
            _ => panic!("Unable to parse expr:  {} ", s.as_str()),
        }
    }
    ast
}

// TODO: remove this and the following function and directly test that the correct AST is produced.
#[cfg(test)]
fn ast_select_statement_to_tuple(ss: &ast::SelectStatement) -> (Vec<String>, Vec<String>) {
    (
        match &ss.from {
            Some(fromclause) => {
                vec![fromclause.tablename.clone()]
            }
            None => vec![],
        },
        ss.select.items.iter().map(|i| format!("{}", i)).collect(),
    )
}

#[cfg(test)]
pub fn parse_select_statement(query: &str) -> (Vec<String>, Vec<String>) {
    let ss: ast::SelectStatement = pt_select_statement_to_ast(query);
    ast_select_statement_to_tuple(&ss)
}

#[test]
fn test_parse_select_statement() {
    let cases = vec![
        ("SELECT * FROM tbl", (vec!["tbl"], vec!["*"])),
        ("select a,b,c fRoM tbl", (vec!["tbl"], vec!["a", "b", "c"])),
        ("select x, 1 from tbl", (vec!["tbl"], vec!["x", "1"])),
        (
            "select x, 1", // This is invalid SQL, but this check happens after parsing.
            (vec![], vec!["x", "1"]),
        ),
        ("select 1", (vec![], vec!["1"])),
        ("select 1.01", (vec![], vec!["1.01"])),
        (
            "select 'hi'",
            (vec![], vec!["'hi'"]), // TODO: this needs to return an expression in the select_items.
        ),
        ("select tRuE", (vec![], vec!["TRUE"])),
        ("select FALSe", (vec![], vec!["FALSE"])),
        (
            "select 123.456, 'seven', 8, 9, NULL",
            (vec![], vec!["123.456", "'seven'", "8", "9", "NULL"]),
        ),
    ];

    for case in cases {
        let input = case.0;
        println!("Input: {}", input);
        let actual: (Vec<String>, Vec<String>) = parse_select_statement(input);
        let expected: (Vec<String>, Vec<String>) = (
            case.1 .0.iter().map(|x| String::from(*x)).collect(),
            case.1 .1.iter().map(|x| String::from(*x)).collect(),
        );
        assert_eq!(actual, expected);
    }
}
