//! This module defines abstract syntax tree (AST) types for SQL.
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

#[derive(Debug, Clone, PartialEq)]
pub enum SelItem {
    Const(Constant),
    ColName(ColName),
    Star,
}

impl std::fmt::Display for SelItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SelItem::Const(x) => write!(f, "{}", x),
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
    // TODO: can we be more specific than Node here?
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
