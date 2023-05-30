//! This module defines abstract syntax tree (AST) types for SQL.

use enum_as_inner::EnumAsInner;

#[derive(Debug, Clone, PartialEq)]
pub struct SelectClause {
    pub items: Vec<SelItem>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColName {
    pub name: String,
}
impl std::fmt::Display for ColName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[derive(Debug, Clone, PartialEq, EnumAsInner)]
pub enum SelItem {
    Expr(Expr),
    ColName(ColName),
    Star,
}

impl std::fmt::Display for SelItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SelItem::Expr(x) => write!(f, "{}", x),
            SelItem::ColName(x) => write!(f, "{}", x),
            SelItem::Star => write!(f, "*"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FromClause {
    pub tablename: String,
}

// #[derive(Debug, Copy, Clone, PartialEq, Eq)]
// pub struct WhereClause {}

// #[derive(Debug, Copy, Clone, PartialEq, Eq)]
// pub struct GroupByClause {}

// #[derive(Debug, Copy, Clone, PartialEq, Eq)]
// pub struct OrderByClause {}

// #[derive(Debug, Copy, Clone, PartialEq, Eq)]
// pub struct HavingClause {}

// #[derive(Debug, Copy, Clone, PartialEq, Eq)]
// pub struct LimitClause {}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub select: SelectClause,
    pub from: Option<FromClause>,
    // pub r#where: Option<WhereClause>,
    // pub group_by: Option<GroupByClause>,
    // pub order_by: Option<OrderByClause>,
    // pub having: Option<HavingClause>,
    // pub limit: Option<LimitClause>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColDef {
    pub colname: ColName,
    pub coltype: String, // Todo: enumerate possible values.
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateStatement {
    pub tablename: String,    // Create clause - be more specific.
    pub coldefs: Vec<ColDef>, // Be more specific.
}

#[derive(Debug, Clone, PartialEq)]
pub enum Constant {
    Int(i64),
    String(String),
    Real(f64),
    Bool(bool),
    Null(),
}

impl std::fmt::Display for Constant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Constant::Int(x) => write!(f, "{}", x),
            Constant::String(x) => write!(f, "{}", x),
            Constant::Real(x) => write!(f, "{}", x),
            Constant::Bool(x) => match x {
                true => write!(f, "TRUE"),
                false => write!(f, "FALSE"),
            },
            Constant::Null() => write!(f, "NULL",),
        }

    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Constant(Constant),
    BinOp {
        lhs: Box<Expr>,
        op: Op,
        rhs: Box<Expr>,
    },
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Constant(x) => write!(f, "{}", x),
            Expr::BinOp{ lhs: l, op: o, rhs: r} => write!(f, "{} {} {}", l, o, r),
        }
    }
}


#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Add,
    Subtract,
    Multiply,
    Divide,
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Op::*;
        write!(f, "{}", 
            match self {
                Add => "+",
                Subtract => "-",
                Multiply => "*",
                Divide => "/",
            }
        )
    }
}