//! `pt_to_ast` has routines for converting parse trees to ASTs for SQL.
//! A Pest parse tree has one enum for all possible terminals and non-terminals.
//! Our AST has enums for groups of terminals that are used in the same production.
//! The AST also discards some lexical detail like case and position in the input.

use anyhow::{Result, bail};

use crate::ast;
use crate::parser::Rule;
use crate::parser::SQLParser;
use crate::parser::parse_expr;
use crate::pest::Parser;

pub fn pt_create_statement_to_ast(c: &str) -> ast::CreateStatement {
    use itertools::Itertools;
    let create_stmt = SQLParser::parse(Rule::create_stmt, c)
        .expect("unsuccessful parse") // unwrap the parse result
        .next()
        .unwrap();

    let mut coldefs: Vec<ast::ColDef> = vec![];
    let mut databasename: String = String::from("main");
    let mut tablename = String::from("");
    // Confirm it is a create statement.
    for c in create_stmt.into_inner() {
        match c.as_rule() {
            Rule::temp => {
                databasename = String::from("temp");
            }
            Rule::table_identifier_with_optional_db => {
                let t = c.into_inner().collect_vec();
                match t.len() {
                    1 => {
                        tablename = String::from(t[0].as_str());
                    }
                    2 => {
                        databasename = String::from(t[0].as_str());
                        tablename = String::from(t[1].as_str());
                    }
                    _ => unreachable!(),
                }
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
    ast::CreateStatement { databasename, tablename, coldefs }
}

#[test]
fn test_pt_create_statement_to_ast() {
    let input = "CREATE TABLE t (a int)";
    let actual = pt_create_statement_to_ast(input);
    let expected = ast::CreateStatement {
        databasename: String::from("main"),
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
#[test]
fn test_pt_create_statement_to_ast_with_temp() {
    let input = "CREATE TEMP TABLE t (a int)";
    let actual = pt_create_statement_to_ast(input);
    let expected = ast::CreateStatement {
        databasename: String::from("temp"),
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

fn remove_single_quoting(s: String) -> String {
    let s2 = s.replace("''", "");
    if s2.len() > 2 {
        s2[1..s2.len()-1].to_string()
    } else {
        s2
    }
}

#[test]
fn test_remove_single_quoting() {
    let cases = [
        ("''", ""),
        ("'hi'", "hi"),
        ("'h''i'", "hi"),
        ("'h''''i'", "hi"),
        ("'''", "'"),
    ];
    for case in cases {
        assert_eq!(remove_single_quoting(case.0.to_string()), case.1.to_string());
    }
}

pub fn parse_literal_from_rule(pair: pest::iterators::Pair<'_, Rule>) -> ast::Constant {
    match pair.as_rule() {
        Rule::null_literal => ast::Constant::Null(),
        Rule::true_literal => ast::Constant::Bool(true),
        Rule::false_literal => ast::Constant::Bool(false),
        Rule::integer_literal => ast::Constant::Int(str::parse::<i64>(pair.as_str()).unwrap()),
        Rule::decimal_literal => {
            // Danger: floating point conversion.
            ast::Constant::Real(str::parse::<f64>(pair.as_str()).unwrap())
        }
        Rule::single_quoted_string => ast::Constant::String(remove_single_quoting(String::from(pair.as_str()))),
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
        ("1", ast::Constant::Int(1)),
        ("1000000000000", ast::Constant::Int(1000000000000)),
        ("-1000000000000", ast::Constant::Int(-1000000000000)),
        ("1.01", ast::Constant::Real(1.01)),
        ("123456789.987654321", ast::Constant::Real(123456789.987654321)),
        ("'hi'", ast::Constant::String("hi".to_string())),
        ("'h''i'", ast::Constant::String("hi".to_string())),
        ("true", ast::Constant::Bool(true)),
        ("tRuE", ast::Constant::Bool(true)),
        ("TRUE", ast::Constant::Bool(true)),
        ("false", ast::Constant::Bool(false)),
        ("fAlSe", ast::Constant::Bool(false)),
        ("FALSE", ast::Constant::Bool(false)),
        ("null", ast::Constant::Null()),
        ("nUlL", ast::Constant::Null()),
        ("NULL", ast::Constant::Null()),
    ];
    for case in cases {
        let input = case.0;
        let literal = SQLParser::parse(Rule::literal, input)
            .expect("unsuccessful parse") // unwrap the parse result
            .next()
            .unwrap();
        let ast = parse_literal_from_rule(literal);
        assert_eq!(ast, case.1);
    }
}

pub fn parse_constant_expr_list(pair: pest::iterators::Pair<'_, Rule>) -> Result<Vec<ast::Constant>> {
    let mut row: Vec<ast::Constant> = vec![];
    for i in pair.into_inner() {
        match i.as_rule() {
            Rule::expr => {
                let expr = parse_expr(i.into_inner());
                match expr {
                    ast::Expr::Constant(c) => row.push(c),
                    // TODO: simplify constant expressions, e.g. "INSERT INTO t VALUES (1+1)"
                    ast::Expr::BinOp{..} => bail!("Operators not supported in constant expression lists."),
                }
            }
            _ => bail!("Unexpected syntax in expression list"),
        }
    }
    Ok(row.clone())
}

#[test]
fn test_parse_constant_expr_list() {
    let cases = vec![
        (
            "(1, 'two', 3.3)", 
            vec![ast::Constant::Int(1), ast::Constant::String("two".to_string()), ast::Constant::Real(3.3)]
        ),
    ];
    for case in cases {
        println!("Case: {}", case.0);
        let mut pairs = SQLParser::parse(Rule::expr_list, case.0).unwrap();
        let res = parse_constant_expr_list(pairs.next().unwrap());
        match res {
            Ok(row) => {
                assert_eq!(row, case.1);
            },
            Err(e) => panic!("Error parsing [{}] : {}",  case.0, e),
        }
    }
}

pub fn parse_constant_expr_list_list(pair: pest::iterators::Pair<'_, Rule>) -> Result<Vec<Vec<ast::Constant>>> {
    let mut rows: Vec<Vec<ast::Constant>> = vec![];
    for i in pair.into_inner() {
        match i.as_rule() {
            Rule::expr_list => rows.push(parse_constant_expr_list(i)?),
            _ => bail!("Unexpected syntax in expression list list.")
        }
    }
    Ok(rows.clone())
}

#[test]
fn test_parse_constant_expr_list_list() {
    let cases = vec![
        (
            "(1, 'two', 3.3)",
            vec![ 
                vec![ast::Constant::Int(1), ast::Constant::String("two".to_string()), ast::Constant::Real(3.3)],
            ],
        ),
        (
            "(1, 'two', 3.3), (4, 'five', 6.6)",
            vec![
                vec![ast::Constant::Int(1), ast::Constant::String("two".to_string()), ast::Constant::Real(3.3)],
                vec![ast::Constant::Int(4), ast::Constant::String("five".to_string()), ast::Constant::Real(6.6)],
            ],
        ),
    ];
    for case in cases {
        println!("Case: {}", case.0);
        let mut pairs = SQLParser::parse(Rule::expr_list_list, case.0).unwrap();
        let res = parse_constant_expr_list_list(pairs.next().unwrap());
        match res {
            Ok(row) => {
                assert_eq!(row, case.1);
            },
            Err(e) => panic!("Error parsing [{}] : {}",  case.0, e),
        }
    }
}

pub fn pt_insert_statement_to_ast(stmt: &str) -> Result<ast::InsertStatement> {
    let insert_stmt = SQLParser::parse(Rule::insert_stmt, stmt)?
        .next()
        .unwrap();

    // Confirm it is an insert statement.
    let tablename; 
    let mut databasename = "main".to_owned();
    let mut pairs = insert_stmt.into_inner();
    if let Some(pair) = pairs.next() {
        if let Rule::table_identifier_with_optional_db = pair.as_rule() {
            let t: Vec<_> = pair.into_inner().collect();
            match t.len() {
                1 => {
                    tablename = String::from(t[0].as_str());
                }
                2 => {
                    databasename = String::from(t[0].as_str());
                    tablename = String::from(t[1].as_str());
                }
                _ => unreachable!(),
            }
        } else { bail!("Missing table identifier in INSERT statement.") }
    } else { bail!("Unexpected syntax in INSERT statement.") }

    if let Some(pair) = pairs.next() {
        if let Rule::expr_list_list = pair.as_rule() {
            let values = parse_constant_expr_list_list(pair)?;
            return Ok(ast::InsertStatement{ databasename, tablename, values });
        }
    }
    bail!("Error parsing VALUES in INSERT statement.");
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

pub fn pt_select_statement_to_ast(query: &str) -> Result<ast::SelectStatement> {
    let select_stmt = SQLParser::parse(Rule::select_stmt, query)?
        .next()
        .unwrap();

    let mut ast = ast::SelectStatement {
        select: ast::SelectClause { items: vec![] },
        from: None,
    };

    // Confirm it is a select statement.
    for s in select_stmt.into_inner() {
        match s.as_rule() {
            Rule::table_identifier_with_optional_db => {    
                if ast.from.is_none() {    
                    let t: Vec<_> = s.into_inner().collect();
                    ast.from = Some(
                        match t.len() {
                            1 => {
                                ast::FromClause {
                                    databasename: "main".to_owned(),
                                    tablename: String::from(t[0].as_str()),
                                }
                            }
                            2 => {
                                ast::FromClause {
                                    databasename: String::from(t[0].as_str()),
                                    tablename: String::from(t[1].as_str()),
                                }
                            }
                            _ => unreachable!(),
                        });    
                } else {
                    bail!("Too many tables in from.")
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
                        Rule::expr => SelItem::Expr(crate::parser::parse_expr(u.into_inner())),
                        _ => bail!("Parse error in select item"),
                    });
                }
            }
            Rule::EOI => (),
            _ => bail!("Unable to parse expr:  {} ", s.as_str()),
        }
    }
    Ok(ast)
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
    let ss = pt_select_statement_to_ast(query).unwrap();
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
            (vec![], vec!["hi"]), // TODO: this needs to return an expression in the select_items.
        ),
        ("select tRuE", (vec![], vec!["TRUE"])),
        ("select FALSe", (vec![], vec!["FALSE"])),
        (
            "select 123.456, 'seven', 8, 9, NULL",
            (vec![], vec!["123.456", "seven", "8", "9", "NULL"]),
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
