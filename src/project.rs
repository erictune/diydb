//! provides helper functions for the projection block of a query.

use crate::sql_type::SqlType;
use crate::sql_value::SqlValue;
use crate::Row;
use anyhow::Result;

// TODO: SelItem can be defined again in IR.
use crate::ast;

#[derive(Clone, Debug, PartialEq)]
/// holds possible actions to take in a ProjectStreamingIterator.
pub enum ProjectAction {
    Take(usize), // let Take(x) ; 0 <= x < input_row.len(); take index x from input row.
    Constant(SqlValue), // put constant value into output row.
                 // Expr(),
}

/// builds the information needed to do a project of a table at runtime.
pub fn build_project(
    in_colnames: &[String],
    in_coltypes: &[SqlType],
    out_cols: &[ast::SelItem],
) -> Result<(Vec<ProjectAction>, Vec<String>, Vec<SqlType>)> {
    let mut actions = vec![];
    let mut out_colnames = vec![];
    let mut out_coltypes = vec![];
    use std::collections::HashMap;
    let mut input_indexes: HashMap<&str, usize> = HashMap::new();
    for (i, c) in in_colnames.iter().enumerate() {
        input_indexes.insert(c, i);
    }
    for out_item in out_cols.iter() {
        match out_item {
            ast::SelItem::Expr(ast::Expr::Constant(c)) => {
                actions.push(ProjectAction::Constant(match c {
                    ast::Constant::Bool(_) => {
                        return Err(anyhow::anyhow!(
                            "Boolean constant in projection is not supported."
                        ));
                    }
                    ast::Constant::Int(i) => SqlValue::Int(*i),
                    ast::Constant::Real(f) => SqlValue::Real(*f),
                    ast::Constant::Null() => {
                        return Err(anyhow::anyhow!(
                            "Null constant in projection is not supported."
                        ));
                    }
                    ast::Constant::String(s) => SqlValue::Text(s.clone()),
                }));
                // TODO: handle AS statements.
                // Sqlite3 names columns after the literal expression used, like "sum(1)"; postgres calls it "?column?"
                out_colnames.push("?column?".to_string());
                // TODO: check if columns can reference other columns by number.
                out_coltypes.push(match c {
                    ast::Constant::Bool(_) => {
                        return Err(anyhow::anyhow!(
                            "Boolean constant in projection is not supported."
                        ));
                    }
                    ast::Constant::Int(_) => SqlType::Int,
                    ast::Constant::Real(_) => SqlType::Real,
                    ast::Constant::Null() => {
                        return Err(anyhow::anyhow!(
                            "Null constant in projection is not supported."
                        ));
                    }
                    ast::Constant::String(_) => SqlType::Text,
                });
            }
            ast::SelItem::Expr(_) => {
                unimplemented!("Only constant items supported in expressions at this time");
            }
            ast::SelItem::ColName(n) => {
                let idx: usize = match input_indexes.get(n.name.as_str()) {
                    Some(idx) => *idx,
                    None => panic!(
                        "Column name not found: {} not in {:?}",
                        n,
                        input_indexes.keys()
                    ),
                };
                actions.push(ProjectAction::Take(idx));
                out_colnames.push(in_colnames[idx].clone()); // TODO: handle AS statements.
                out_coltypes.push(in_coltypes[idx]);
            }
            ast::SelItem::Star => {
                for i in 0..in_colnames.len() {
                    actions.push(ProjectAction::Take(i));
                    out_colnames.push(in_colnames[i].clone()); // TODO: handle AS statements.
                    out_coltypes.push(in_coltypes[i]);
                }
            }
        }
    }
    Ok((actions, out_colnames, out_coltypes))
}

#[cfg(test)]
fn make_ast_colname(s: &str) -> ast::SelItem {
    ast::SelItem::ColName(ast::ColName {
        name: String::from(s),
    })
}

#[cfg(test)]
fn make_ast_constant(i: i64) -> ast::SelItem {
    ast::SelItem::Expr(ast::Expr::Constant(ast::Constant::Int(i)))
}

#[test]
fn test_build_project_colnames_only() {
    use crate::SqlType::*;
    use ProjectAction::*;
    let colnames: Vec<String> = vec!["a", "b", "c", "d", "e"]
        .iter()
        .map(|i| String::from(*i))
        .collect();
    let coltypes: Vec<SqlType> = vec![Int, Int, Real, Real, Text];
    let out_cols = vec![
        make_ast_colname("a"),
        make_ast_colname("c"),
        make_ast_colname("b"),
        make_ast_colname("a"),
    ];
    let expected_actions = vec![Take(0), Take(2), Take(1), Take(0)];
    let expected_colnames: Vec<String> = vec!["a", "c", "b", "a"]
        .iter()
        .map(|i| String::from(*i))
        .collect();
    let expected_coltypes = vec![Int, Real, Int, Int];
    let (actual_actions, actual_colnames, actual_coltypes) =
        build_project(&colnames, &coltypes, &out_cols).unwrap();
    assert_eq!(actual_actions, expected_actions);
    assert_eq!(actual_colnames, expected_colnames);
    assert_eq!(actual_coltypes, expected_coltypes);
}

#[test]
fn test_build_project_constant_expression() {
    use crate::SqlType::*;
    use ProjectAction::*;
    let colnames: Vec<String> = vec!["a", "b", "c", "d", "e"]
        .iter()
        .map(|i| String::from(*i))
        .collect();
    let coltypes: Vec<SqlType> = vec![Int, Int, Real, Real, Text];
    let out_cols = vec![make_ast_constant(1)];
    let expected_actions = vec![Constant(SqlValue::Int(1))];
    let expected_colnames: Vec<String> =
        vec!["?column?"].iter().map(|i| String::from(*i)).collect();
    let expected_coltypes = vec![Int];
    let (actual_actions, actual_colnames, actual_coltypes) =
        build_project(&colnames, &coltypes, &out_cols).unwrap();
    assert_eq!(actual_actions, expected_actions);
    assert_eq!(actual_colnames, expected_colnames);
    assert_eq!(actual_coltypes, expected_coltypes);
}

#[test]
fn test_build_project_multiple_star() {
    use crate::SqlType::*;
    use ProjectAction::*;
    let colnames: Vec<String> = vec!["a", "b", "c", "d", "e"]
        .iter()
        .map(|i| String::from(*i))
        .collect();
    let coltypes: Vec<SqlType> = vec![Int, Int, Real, Real, Text];
    let out_cols = vec![
        ast::SelItem::Star,
        make_ast_colname("a"),
        ast::SelItem::Star,
    ];
    let expected_actions = vec![
        Take(0),
        Take(1),
        Take(2),
        Take(3),
        Take(4),
        Take(0),
        Take(0),
        Take(1),
        Take(2),
        Take(3),
        Take(4),
    ];
    let expected_colnames: Vec<String> =
        vec!["a", "b", "c", "d", "e", "a", "a", "b", "c", "d", "e"]
            .iter()
            .map(|i| String::from(*i))
            .collect();
    let expected_coltypes = vec![Int, Int, Real, Real, Text, Int, Int, Int, Real, Real, Text];
    let (actual_actions, actual_colnames, actual_coltypes) =
        build_project(&colnames, &coltypes, &out_cols).unwrap();
    assert_eq!(actual_actions, expected_actions);
    assert_eq!(actual_colnames, expected_colnames);
    assert_eq!(actual_coltypes, expected_coltypes);
}

/// does the "Project" action of the relational algebra, using a pre-built set of actions.
pub fn project_row(actions: &Vec<ProjectAction>, input: &Row) -> Result<Row> {
    let mut ret: Vec<SqlValue> = vec![];
    for action in actions {
        ret.push(match action {
            ProjectAction::Take(idx) => input.items[*idx].clone(),
            ProjectAction::Constant(v) => v.clone(),
        })
    }
    Ok(Row {
        items: ret.to_vec(),
    })
}

#[test]
fn test_project_row_take() {
    use ProjectAction::*;
    use SqlValue::*;
    let input = Row {
        items: vec![Int(0), Int(10), Int(20), Int(30)],
    };
    let actions = vec![Take(2), Take(0), Take(2)];
    let output = project_row(&actions, &input).unwrap();
    assert_eq!(output.items.len(), 3);
    assert_eq!(output.items[0], Int(20));
    assert_eq!(output.items[1], Int(0));
    assert_eq!(output.items[2], Int(20));
}

#[test]
fn test_project_row_constants() {
    use ProjectAction::*;
    use SqlValue::*;
    let input = Row {
        items: vec![Int(0), Int(10), Int(20), Int(30)],
    };
    let actions = vec![
        Take(2),
        Constant(Real(123.456)),
        Constant(Int(7)),
        Constant(Text("eight".to_string())),
    ];
    let output = project_row(&actions, &input).unwrap();
    assert_eq!(output.items.len(), 4);
    assert_eq!(output.items[0], Int(20));
    assert_eq!(output.items[1], Real(123.456));
    assert_eq!(output.items[2], Int(7));
    assert_eq!(output.items[3], Text("eight".to_string()));
}
