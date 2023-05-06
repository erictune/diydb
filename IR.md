Intermediate Representation
====================


# Tables used in examples

Table `t`:
```
create table t (a int, b int)
insert into t values (1,10)
insert into t values (2,20)
```

Index `t_a` as shown below: 
```
create index t_a on t (a)
```

# IR Types
These types hold some data (creation arguments), and can be evaluated.  By using traits, they
can be strung together using different 

**TODO: combine AddColFromExpr into Project for consistency with how relational algebra is defined on e.g. wikipedia.**

|       type         | creation arguments | eval() step input | eval() step output  |
| ------------------ | ------------------ | ---------------- | ------------- |
| `TempTable`        | literal     values | no eval args     | `Row`        |
| `Scan`             | table name         | no eval args     | `Row`        |
| `SeekRowid`        | table name, rowid  | no eval args     | `Row`        |
| `IndexedSeekEq`    | table, index, key  | no eval args     | `Row`        |
| `Filter`           | `&LogicalOp`, `&RowIterable` | `Row` | `Option<Row>` |
| `Project`          | list of `&ColExpr` | `Row`            | `Row`        |

A `ColExpr` is an expression referencing elements of a row by column name (or index for efficiency).
They include binary ops (`Eq`, `Gt`, `Add`, `Mul`) and unary ops (`Negate`)

# Examples of SQL converted to IR

**TODO: combine AddColFromExpr into Project for consistency with how relational algebra is defined on e.g. wikipedia.**


|  Preconditions | SQL Statement           |    IR    |   Notes |
| - | --------------- | ----------- | --------- |
| None | `select 1` | `ScanConstantRows` |  |
| Table `t` | `select * from t` | `Scan("t")` |  A star says we don't need a `Project`. |
| Table `t` | `select * from t where rowid = 1` | `SeekRowid("t", rowid)` |  | 
| Table `t` | `select * from t where a = 1` | `Filter(LogicalColExpr(Eq(Col("a"), Const(1, int)), Scan(t)` | `Filter` only returns rows from _arg2_ which match expression _arg1_. |
| Table `t` | `select a from t` | `Project(["a"], Scan("t"))` | `Project` drops columns not mentioned in the column list (arg1) from table (arg2) |
| Table `t` | `select b from t where rowid = 1` | `Project(["b"], RowidSeek("t", 1))` | |
| Table `t` | `select b from t where a = 1` | `Project(["b"], Filter(Eq(Col("a"), Const(1, int)), Scan(a)))` | `Filter` only returns rows from _arg2_ which match expression _arg1_. | 
| Table `t` and Index `t_a` | `select b from t where a = 1` | `Project(["b"], IndexedSeekEq("t", "t_a", 1))` | `Filter` only returns rows from _arg2_ which match expression _arg1_. | 
| Table `t`  | `select a + a from t` | `Project(["_expr1"], NewColFromExpr("_expr1", Expr(Add(Col("a"), Col("a"))), Scan(t)))` |  NewColFromExpr adds a new column to table named _arg1_ to the table _arg3_ computed with expression _arg2_.   |
| None | `select 1 + 1`    | `ScanConstantRow(2)` | Constant expressions are evaluated before query exectution
| Table `t` | `select *, -a from t` | `AddColFromExpr("_expr1", Expr(UnaryMinus(Col("a"))), Scan(t))` | `AddColFromExpr` adds a new column to table named _arg1_ to the table _arg3_ computed with expression _arg2_. |
| Table `t` | `select a + a from t` | `Project(["_expr1"], AddColFromExpr("_expr1", Expr(UnaryMinus(Col("a"))), Scan(t)))` |  NewColFromExpr .   |
| Table `t` and Index `t_a` | `select * from t` | `Scan("t")` | No change. |
| Table `t` and Index `t_a` | `select * from t where rowid = 1` | `RowidSeek("t", rowid)` | No change | 
| Table `t` and Index `t_a` | `select * from t where a = 1` | `IndexSeekEq("t", "t_a", 1)` | `IndexSeekEq` returns the range of values in _arg1_ equal to _arg3_ using index table _arg2_. | 

# IR Traits

TBD: if there should be stronger typing of Exprs? SQLite does not use strong types.

Trait names to be determined.  Whether to use common rust traits or new ones is TBD also.
TBD if traits are needed.

| tenative trait name |      trait description              | example IR types meeting it   |
| ------------------- | ----------------------------------- | ------------------------------ | 
| `LogicalOp`         | evaluation returns boolean          | `Eq`, `Gt`, `Ne`               |
| `RowIterable`       | evaluation returns sequence of rows | `ScanConstantRows`, `Scan`, `SeekRowid` |
| `NumericOp`         | evaluation returns numeric scalar   | `Negate`, `Add`                |
| `NullaryOp`         | evaluation takes a no arguments     | `ScanConstantRows`, `Scan`, `SeekRowid` |
| `UnaryOp`           | evaluation takes one scalar args    | `Negate`, `Sqrt`               |
| `BinaryOp`          | evaluation takes two scalar args    | `Add`, `Mul`                   |

# Questions and Observations
-   To stepwise evaluate a `Filter`, you have to check if it has a row ready right now, which is different from it being done.
-   An immediate table is needed to handle some sql expressions.  Also, an immediate table would help with testing SQL.  This requires
    a ScannableTable trait that can iterate over a constant or btree-based table. 
-   Is index selection part of AST to IR conversion, or  a subsequent steps (optimization, interpretation, codegen).
-   A complete implementation of `SeekRowid` would allow for expressions rather than a constant rowid, right?

